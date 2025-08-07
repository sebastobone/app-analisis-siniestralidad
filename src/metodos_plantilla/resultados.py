import os
from typing import Literal

import polars as pl
import xlwings as xw

from src import constantes as ct
from src import utils
from src.logger_config import logger


def concatenar_archivos_resultados() -> pl.DataFrame:
    columnas_distintivas = [
        "apertura_reservas",
        "mes_corte",
        "atipico",
        "tipo_analisis",
    ]

    files = [file for file in os.listdir("output/resultados") if ".parquet" in file]
    sorted_files = sorted(
        files, key=lambda f: os.path.getmtime(os.path.join("output/resultados", f))
    )

    dfs = []
    for file in sorted_files:
        df = pl.read_parquet(f"output/resultados/{file}").filter(
            pl.col("plata_ultimate_bruto").sum().over(columnas_distintivas) != 0
        )
        dfs.append(utils.generalizar_tipos_columnas_resultados(df))

    if dfs:
        df_resultados = (
            pl.DataFrame(pl.concat(dfs, how="diagonal_relaxed"))
            .unique(subset=columnas_distintivas + ["periodo_ocurrencia"], keep="last")
            .sort(columnas_distintivas + ["periodo_ocurrencia"])
            .pipe(agregar_periodo_granular, columnas_distintivas)
            .with_columns(
                pl.col("periodo_ocurrencia", "periodo_granular").cast(pl.Int32)
            )
        )
    else:
        logger.warning("No se encontraron resultados anteriores.")
        df_resultados = pl.DataFrame()

    return df_resultados


def agregar_periodo_granular(
    dfs: pl.DataFrame, columnas_distintivas: list[str]
) -> pl.DataFrame:
    granularidades = (
        dfs.select(columnas_distintivas + ["periodicidad_ocurrencia"])
        .with_columns(
            pl.col("periodicidad_ocurrencia").first().over(columnas_distintivas)
        )
        .rename({"periodicidad_ocurrencia": "granularidad"})
        .unique()
    )

    return dfs.join(
        granularidades, on=columnas_distintivas, how="left", validate="m:1"
    ).with_columns(
        periodo_granular=pl.when(pl.col("granularidad") == "Anual")
        .then((pl.col("periodo_ocurrencia") // 100) * 100 + 12)
        .when(pl.col("granularidad") == "Semestral")
        .then(
            (pl.col("periodo_ocurrencia") // 100) * 100
            + pl.when(pl.col("periodo_ocurrencia") % 100 < 7).then(6).otherwise(12)
        )
        .when(pl.col("granularidad") == "Trimestral")
        .then(
            (pl.col("periodo_ocurrencia") // 100) * 100
            + pl.when(pl.col("periodo_ocurrencia") % 100 < 4)
            .then(3)
            .when(pl.col("periodo_ocurrencia") % 100 < 7)
            .then(6)
            .when(pl.col("periodo_ocurrencia") % 100 < 10)
            .then(9)
            .otherwise(12)
        )
        .otherwise(pl.col("periodo_ocurrencia"))
    )


def actualizar_wb_resultados() -> xw.Book:
    if not os.path.exists("output/resultados.xlsx"):
        wb = xw.Book()
        wb.sheets.add("Resultados")
        wb.save("output/resultados.xlsx")
        logger.info("Nuevo libro de resultados creado en output/resultados.xlsx.")
    else:
        wb = xw.Book("output/resultados.xlsx")

    wb.sheets["Resultados"].clear_contents()
    wb.sheets["Resultados"]["A1"].value = concatenar_archivos_resultados().pipe(
        utils.mantener_formato_columnas
    )

    return wb


def generar_informe_actuario_responsable(
    negocio: str, mes_corte: int, tipo_analisis: Literal["triangulos", "entremes"]
) -> None:
    columnas_base = [
        "codigo_op",
        "codigo_ramo_op",
        "periodicidad_ocurrencia",
        "periodo_ocurrencia",
        "mes_corte",
    ]
    columnas_sumadas_tip_atip = [
        "ibnr_bruto",
        "ibnr_contable_bruto",
        "plata_ultimate_bruto",
        "plata_ultimate_contable_bruto",
        "ibnr_retenido",
        "ibnr_contable_retenido",
        "plata_ultimate_contable_retenido",
        "plata_ultimate_retenido",
    ]

    resultados = (
        concatenar_archivos_resultados()
        .lazy()
        .filter(
            (pl.col("mes_corte") == mes_corte)
            & (pl.col("tipo_analisis") == tipo_analisis)
        )
    )

    siniestros = (
        resultados.select(
            columnas_base
            + [
                "atipico",
                "pago_bruto",
                "aviso_bruto",
                "pago_retenido",
                "aviso_retenido",
            ]
            + columnas_sumadas_tip_atip
        )
        .group_by(columnas_base + ["atipico"])
        .sum()
    )

    tipicos = siniestros.filter(pl.col("atipico") == 0).drop("atipico")
    atipicos = siniestros.filter(pl.col("atipico") == 1).drop("atipico")

    siniestros_ar = (
        tipicos.join(
            atipicos, on=columnas_base, how="full", coalesce=True, suffix="_atipicos"
        )
        .fill_null(0)
        .with_columns(
            [
                (pl.col(col) + pl.col(col + "_atipicos")).alias(col)
                for col in columnas_sumadas_tip_atip
            ]
        )
        .drop([col + "_atipicos" for col in columnas_sumadas_tip_atip])
        .unpivot(index=columnas_base, variable_name="cantidad", value_name="valor")
        .with_columns(
            atributo=pl.when(pl.col("cantidad").str.contains("bruto"))
            .then(pl.lit("bruto"))
            .otherwise(pl.lit("retenido")),
            cantidad=pl.col("cantidad").str.replace_many(["_bruto", "_retenido"], [""]),
        )
        .collect()
        .pivot(on="cantidad", index=columnas_base + ["atributo"])
        .lazy()
        .rename(
            {
                "pago": "pago_tipicos",
                "aviso": "aviso_tipicos",
                "plata_ultimate_contable": "sue_contable",
                "plata_ultimate": "sue_actuarial",
                "ibnr": "ibnr_actuarial",
                "ibnr_contable": "ibnr_contable",
            }
        )
    )

    primas_ar = (
        resultados.select(
            columnas_base + ["prima_bruta_devengada", "prima_retenida_devengada"]
        )
        .unique()
        .group_by(columnas_base)
        .sum()
        .unpivot(index=columnas_base, variable_name="cantidad", value_name="valor")
        .with_columns(
            atributo=pl.when(pl.col("cantidad").str.contains("bruta"))
            .then(pl.lit("bruto"))
            .otherwise(pl.lit("retenido")),
            cantidad=pl.col("cantidad").str.replace_many(["_bruta", "_retenida"], [""]),
        )
        .collect()
        .pivot(on="cantidad", index=columnas_base + ["atributo"])
        .lazy()
    )

    base_ar = (
        siniestros_ar.join(primas_ar, on=columnas_base + ["atributo"], how="left")
        .fill_null(0)
        .with_columns(
            num_meses=pl.col("periodicidad_ocurrencia")
            .replace(ct.PERIODICIDADES)
            .cast(pl.Int32)
        )
        .with_columns(
            ano_ocurrencia=(pl.col("periodo_ocurrencia") // 100),
            semestre_ocurrencia=pl.when(pl.col("num_meses") > 6)
            .then(-1)
            .when((pl.col("periodo_ocurrencia") % 100) < 7)
            .then(1)
            .otherwise(2),
            trimestre_ocurrencia=pl.when(pl.col("num_meses") > 3)
            .then(-1)
            .when((pl.col("periodo_ocurrencia") % 100) < 4)
            .then(1)
            .when((pl.col("periodo_ocurrencia") % 100) < 7)
            .then(2)
            .when((pl.col("periodo_ocurrencia") % 100) < 10)
            .then(3)
            .otherwise(4),
            mes_ocurrencia=pl.when(pl.col("num_meses") > 1)
            .then(-1)
            .otherwise(pl.col("periodo_ocurrencia") % 100),
        )
        .select(
            [
                "codigo_op",
                "codigo_ramo_op",
                "mes_corte",
                "ano_ocurrencia",
                "semestre_ocurrencia",
                "trimestre_ocurrencia",
                "mes_ocurrencia",
                "atributo",
                "pago_tipicos",
                "pago_atipicos",
                "aviso_tipicos",
                "aviso_atipicos",
                "sue_contable",
                "sue_actuarial",
                "ibnr_contable",
                "ibnr_actuarial",
                "prima_devengada",
            ]
        )
        .sort(
            [
                "codigo_op",
                "codigo_ramo_op",
                "ano_ocurrencia",
                "semestre_ocurrencia",
                "trimestre_ocurrencia",
                "mes_ocurrencia",
            ]
        )
        .collect()
    )

    base_ar.write_excel(f"output/informe_ar_{negocio}_{mes_corte}.xlsx", worksheet="AR")
    logger.success(
        f"Informe AR almacenado en output/informe_ar_{negocio}_{mes_corte}.xlsx."
    )

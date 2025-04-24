import os

import polars as pl
import xlwings as xw

from src import utils
from src.logger_config import logger


def concatenar_archivos_resultados() -> pl.DataFrame:
    columnas_distintivas = ["apertura_reservas", "mes_corte", "atipico"]

    files = [file for file in os.listdir("output/resultados") if ".parquet" in file]
    sorted_files = sorted(
        files, key=lambda f: os.path.getmtime(os.path.join("output/resultados", f))
    )

    dfs = []
    for file in sorted_files:
        dfs.append(
            pl.read_parquet(f"output/resultados/{file}").filter(
                pl.col("plata_ultimate_bruto").sum().over(columnas_distintivas) != 0
            )
        )

    try:
        df_resultados = (
            pl.DataFrame(pl.concat(dfs))
            .unique(subset=columnas_distintivas + ["periodo_ocurrencia"], keep="last")
            .sort(columnas_distintivas + ["periodo_ocurrencia"])
            .with_columns(pl.col("periodo_ocurrencia").cast(pl.Int32))
        )
    except ValueError:
        logger.warning("No se encontraron resultados anteriores.")
        df_resultados = pl.DataFrame()

    return df_resultados


def actualizar_wb_resultados() -> xw.Book:
    if not os.path.exists("output/resultados.xlsx"):
        wb = xw.Book()
        wb.sheets.add("Resultados")
        wb.save("output/resultados.xlsx")
        logger.info("Nuevo libro de resultados creado en output/resultados.xlsx.")
    else:
        wb = xw.Book("output/resultados.xlsx")

    wb.sheets["Resultados"].clear_contents()
    wb.sheets["Resultados"]["A1"].options(index=False).value = (
        concatenar_archivos_resultados()
        .pipe(utils.mantener_formato_columnas)
        .to_pandas()
    )

    return wb


def generar_informe_actuario_responsable(negocio: str, mes_corte: int) -> None:
    df = (
        concatenar_archivos_resultados().lazy().filter(pl.col("mes_corte") == mes_corte)
    )

    # filtrado por los tipicos
    df_tip = (
        df.select(
            [
                "codigo_ramo_op",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "pago_bruto",
                "aviso_bruto",
                "ibnr_bruto",
                "ibnr_contable_bruto",
                "plata_ultimate_contable_bruto",
                "plata_ultimate_bruto",
                "pago_retenido",
                "aviso_retenido",
                "ibnr_retenido",
                "ibnr_contable_retenido",
                "plata_ultimate_contable_retenido",
                "plata_ultimate_retenido",
                "atipico",
            ]
        )
        .filter(pl.col("atipico") == 0)
        .group_by(["codigo_ramo_op", "periodicidad_ocurrencia", "periodo_ocurrencia"])
        .sum()
    )
    # filtrado, los atipicos
    df_atip = (
        df.select(
            [
                "codigo_ramo_op",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "pago_bruto",
                "aviso_bruto",
                "ibnr_bruto",
                "ibnr_contable_bruto",
                "plata_ultimate_contable_bruto",
                "plata_ultimate_bruto",
                "pago_retenido",
                "aviso_retenido",
                "ibnr_retenido",
                "ibnr_contable_retenido",
                "plata_ultimate_contable_retenido",
                "plata_ultimate_retenido",
                "atipico",
            ]
        )
        .filter(pl.col("atipico") == 1)
        .with_columns(
            [
                pl.when(pl.col("periodo_ocurrencia").cast(pl.Int32) == mes_corte)
                .then(pl.col("periodo_ocurrencia").cast(pl.Int32))
                .when(pl.col("periodo_ocurrencia").cast(pl.Int32) + 2 > mes_corte)
                .then(pl.col("periodo_ocurrencia").cast(pl.Int32))
                .when((pl.col("periodo_ocurrencia").cast(pl.Int32) % 100) < 4)
                .then(((pl.col("periodo_ocurrencia").cast(pl.Int32) // 100) * 100) + 3)
                .when((pl.col("periodo_ocurrencia").cast(pl.Int32) % 100) < 7)
                .then(((pl.col("periodo_ocurrencia").cast(pl.Int32) // 100) * 100) + 6)
                .when((pl.col("periodo_ocurrencia").cast(pl.Int32) % 100) < 10)
                .then(((pl.col("periodo_ocurrencia").cast(pl.Int32) // 100) * 100) + 9)
                .otherwise(
                    ((pl.col("periodo_ocurrencia").cast(pl.Int32) // 100) * 100) + 12
                )
                .alias("periodo_ocurrencia"),
                pl.when(pl.col("periodo_ocurrencia").cast(pl.Int32) == mes_corte)
                .then(pl.lit("Trimestral"))
                .when(pl.col("periodo_ocurrencia").cast(pl.Int32) + 2 > mes_corte)
                .then(pl.lit("Mensual"))
                .otherwise(pl.lit("Trimestral"))
                .alias("periodicidad_ocurrencia")
                .cast(pl.String),
            ]
        )
        .group_by(["codigo_ramo_op", "periodicidad_ocurrencia", "periodo_ocurrencia"])
        .sum()
    )

    df_atip = df_atip.rename(
        {
            "pago_bruto": "pago_bruto_atipico",
            "aviso_bruto": "aviso_bruto_atipico",
            "pago_retenido": "pago_retenido_atipico",
            "aviso_retenido": "aviso_retenido_atipico",
        }
    )
    df_unid = (
        df_tip.join(
            df_atip,
            on=["codigo_ramo_op", "periodicidad_ocurrencia", "periodo_ocurrencia"],
            how="full",
            suffix="_atip",
        )
        .fill_null(0)
        .group_by(["codigo_ramo_op", "periodicidad_ocurrencia", "periodo_ocurrencia"])
        .agg(
            [
                pl.col("ibnr_bruto")
                + pl.col("ibnr_bruto_atip").alias("ibnr_bruto").cast(pl.Float64),
                pl.col("ibnr_contable_bruto")
                + pl.col("ibnr_contable_bruto_atip")
                .alias("ibnr_contable_bruto")
                .cast(pl.Float64),
                pl.col("plata_ultimate_contable_bruto")
                + pl.col("plata_ultimate_contable_bruto_atip")
                .alias("plata_ultimate_contable_bruto")
                .cast(pl.Float64),
                pl.col("plata_ultimate_bruto")
                + pl.col("plata_ultimate_bruto_atip")
                .alias("plata_ultimate_bruto")
                .cast(pl.Float64),
                pl.col("ibnr_retenido")
                + pl.col("ibnr_retenido_atip").alias("ibnr_retenido").cast(pl.Float64),
                pl.col("ibnr_contable_retenido")
                + pl.col("ibnr_contable_retenido_atip")
                .alias("ibnr_contable_retenido")
                .cast(pl.Float64),
                pl.col("plata_ultimate_contable_retenido")
                + pl.col("plata_ultimate_contable_retenido_atip")
                .alias("plata_ultimate_contable_retenido")
                .cast(pl.Float64),
                pl.col("plata_ultimate_retenido")
                + pl.col("plata_ultimate_retenido_atip")
                .alias("plata_ultimate_retenido")
                .cast(pl.Float64),
                pl.col("pago_bruto_atipico").cast(pl.Float64),
                pl.col("aviso_bruto_atipico").cast(pl.Float64),
                pl.col("pago_bruto").cast(pl.Float64),
                pl.col("aviso_bruto").cast(pl.Float64),
                pl.col("pago_retenido_atipico").cast(pl.Float64),
                pl.col("pago_retenido").cast(pl.Float64),
                pl.col("aviso_retenido_atipico").cast(pl.Float64),
                pl.col("aviso_retenido").cast(pl.Float64),
            ]
        )
    )

    df_bruto = df_unid.select(
        [
            "codigo_ramo_op",
            "periodicidad_ocurrencia",
            "periodo_ocurrencia",
            "ibnr_bruto",
            "ibnr_contable_bruto",
            "plata_ultimate_contable_bruto",
            "plata_ultimate_bruto",
            "pago_bruto_atipico",
            "aviso_bruto_atipico",
            "pago_bruto",
            "aviso_bruto",
        ]
    ).with_columns(pl.lit("bruto").alias("atributo"))
    df_bruto = df_bruto.rename(
        {
            "pago_bruto": "pagos_tipicos",
            "pago_bruto_atipico": "pagos_atipicos",
            "aviso_bruto": "aviso_tipico",
            "aviso_bruto_atipico": "aviso_atipico",
            "plata_ultimate_contable_bruto": "sue_contable",
            "plata_ultimate_bruto": "sue_actuarial",
            "ibnr_bruto": "ibnr_actuarial",
            "ibnr_contable_bruto": "ibnr_contable",
        }
    )

    df_retenido = df_unid.select(
        [
            "codigo_ramo_op",
            "periodicidad_ocurrencia",
            "periodo_ocurrencia",
            "ibnr_retenido",
            "ibnr_contable_retenido",
            "plata_ultimate_contable_retenido",
            "plata_ultimate_retenido",
            "pago_retenido_atipico",
            "aviso_retenido_atipico",
            "pago_retenido",
            "aviso_retenido",
        ]
    ).with_columns(pl.lit("retenido").alias("atributo"))
    df_retenido = df_retenido.rename(
        {
            "pago_retenido": "pagos_tipicos",
            "pago_retenido_atipico": "pagos_atipicos",
            "aviso_retenido": "aviso_tipico",
            "aviso_retenido_atipico": "aviso_atipico",
            "plata_ultimate_contable_retenido": "sue_contable",
            "plata_ultimate_retenido": "sue_actuarial",
            "ibnr_retenido": "ibnr_actuarial",
            "ibnr_contable_retenido": "ibnr_contable",
        }
    )

    df_sinis = df_bruto.collect().vstack(df_retenido.collect())

    df_primas_pre = (
        df.select(
            [
                "codigo_ramo_op",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "prima_bruta_devengada",
                "prima_retenida_devengada",
            ]
        )
        .unique()
        .group_by(["codigo_ramo_op", "periodicidad_ocurrencia", "periodo_ocurrencia"])
        .sum()
    )

    df_primas_brutas = df_primas_pre.select(
        [
            "codigo_ramo_op",
            "periodicidad_ocurrencia",
            "periodo_ocurrencia",
            "prima_bruta_devengada",
        ]
    ).with_columns(pl.lit("bruto").alias("atributo"))

    df_primas_brutas = df_primas_brutas.rename(
        {"prima_bruta_devengada": "prima_devengada"}
    )

    df_primas_retenida = df_primas_pre.select(
        [
            "codigo_ramo_op",
            "periodicidad_ocurrencia",
            "periodo_ocurrencia",
            "prima_retenida_devengada",
        ]
    ).with_columns(pl.lit("retenido").alias("atributo"))

    df_primas_retenida = df_primas_retenida.rename(
        {"prima_retenida_devengada": "prima_devengada"}
    )

    df_primas = df_primas_brutas.collect().vstack(df_primas_retenida.collect())

    df_ar = (
        df_sinis.lazy()
        .join(
            df_primas.lazy(),
            on=[
                "codigo_ramo_op",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "atributo",
            ],
            how="left",
        )
        .fill_null(0)
        .with_columns(
            [
                (pl.col("periodo_ocurrencia").cast(pl.Int32) // 100)
                .cast(pl.Int32)
                .alias("anio_ocurrencia"),
                pl.when((pl.col("periodo_ocurrencia").cast(pl.Int32) % 100) < 7)
                .then(1)
                .otherwise(2)
                .cast(pl.Int32)
                .alias("semestre_ocurrencia"),
                pl.when((pl.col("periodo_ocurrencia").cast(pl.Int32) % 100) < 4)
                .then(1)
                .when((pl.col("periodo_ocurrencia").cast(pl.Int32) % 100) < 7)
                .then(2)
                .when((pl.col("periodo_ocurrencia").cast(pl.Int32) % 100) < 10)
                .then(3)
                .otherwise(4)
                .cast(pl.Int32)
                .alias("trimestre_ocurrencia"),
                pl.when(pl.col("periodicidad_ocurrencia") == "Trimestral")
                .then(-1)
                .otherwise(pl.col("periodo_ocurrencia").cast(pl.Int32) % 100)
                .cast(pl.Int32)
                .alias("mes_ocurrencia"),
                pl.lit(mes_corte).alias("periodo_corte"),
            ]
        )
        .with_columns(
            [
                pl.col(col).list.first().cast(pl.Float64).alias(col)
                for col in [
                    "pagos_tipicos",
                    "pagos_atipicos",
                    "aviso_tipico",
                    "aviso_atipico",
                    "sue_contable",
                    "sue_actuarial",
                    "ibnr_contable",
                    "ibnr_actuarial",
                ]
            ]
        )
        .select(
            [
                "codigo_ramo_op",
                "periodo_corte",
                "anio_ocurrencia",
                "semestre_ocurrencia",
                "trimestre_ocurrencia",
                "mes_ocurrencia",
                "atributo",
                "pagos_tipicos",
                "pagos_atipicos",
                "aviso_tipico",
                "aviso_atipico",
                "sue_contable",
                "sue_actuarial",
                "ibnr_contable",
                "ibnr_actuarial",
                "prima_devengada",
            ]
        )
        .sort(
            [
                "codigo_ramo_op",
                "anio_ocurrencia",
                "semestre_ocurrencia",
                "trimestre_ocurrencia",
                "mes_ocurrencia",
            ]
        )
        .collect()
    )

    df_ar.write_excel(f"output/informe_ar_{negocio}_{mes_corte}.xlsx", worksheet="AR")

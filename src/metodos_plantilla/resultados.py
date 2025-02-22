import os

import polars as pl
import xlwings as xw

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
    wb = xw.Book("output/resultados.xlsx")

    wb.sheets["Resultados"].clear_contents()
    wb.sheets["Resultados"]["A1"].options(
        index=False
    ).value = concatenar_archivos_resultados().to_pandas()

    return wb


def generar_informe_actuario_responsable(negocio: str, mes_corte: int) -> None:
    periodicidades_desc = pl.LazyFrame(
        {
            "periodicidad_ocurrencia": [
                "Mensual",
                "Trimestral",
                "Semestral",
                "Anual",
            ],
            "llave": ["M", "Q", "S", "A"],
            "numero_meses": [1, 3, 6, 12],
        }
    )

    df = (
        concatenar_archivos_resultados().lazy().filter(pl.col("mes_corte") == mes_corte)
    )

    df_sinis = (
        df.select(
            [
                "ramo_desc",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "pago_bruto",
                "aviso_bruto",
                "ibnr_bruto",
                "plata_ultimate_contable_bruto",
                "pago_retenido",
                "aviso_retenido",
                "ibnr_retenido",
                "plata_ultimate_contable_retenido",
            ]
        )
        .group_by(["ramo_desc", "periodicidad_ocurrencia", "periodo_ocurrencia"])
        .sum()
    )

    df_primas = (
        df.select(
            [
                "ramo_desc",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "prima_bruta_devengada",
                "prima_retenida_devengada",
            ]
        )
        .unique()
        .group_by(["ramo_desc", "periodicidad_ocurrencia", "periodo_ocurrencia"])
        .sum()
    )

    df_ar = (
        df_sinis.join(
            df_primas,
            on=["ramo_desc", "periodicidad_ocurrencia", "periodo_ocurrencia"],
            how="left",
        )
        .fill_null(0)
        .join(periodicidades_desc, on="periodicidad_ocurrencia", how="left")
        .with_columns(
            periodo_ocurrencia=(pl.col("periodo_ocurrencia") // pl.lit(100))
            .cast(pl.Int32)
            .cast(pl.String)
            + " "
            + pl.concat_str(
                pl.col("llave"),
                ((pl.col("periodo_ocurrencia") % pl.lit(100)) / pl.col("numero_meses"))
                .ceil()
                .cast(pl.Int8)
                .cast(pl.String),
            )
        )
        .select(
            ["ramo_desc", "periodo_ocurrencia"]
            + [column for column in df_sinis.collect_schema() if "bruto" in column]
            + ["prima_bruta_devengada"]
            + [column for column in df_sinis.collect_schema() if "retenido" in column]
            + ["prima_retenida_devengada"]
        )
        .sort(["ramo_desc", "periodo_ocurrencia"])
        .collect()
    )

    df_ar.write_excel(f"output/informe_ar_{negocio}_{mes_corte}.xlsx", worksheet="AR")

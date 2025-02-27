from typing import Literal

import polars as pl

from src import utils
from src.logger_config import logger


async def realizar_cuadre_contable(
    negocio: str,
    file: Literal["siniestros", "primas", "expuestos"],
    df: pl.DataFrame,
    dif_sap_vs_tera: pl.DataFrame,
) -> pl.DataFrame:
    if negocio == "soat":
        return await realizar_cuadre_contable_soat(file, df, dif_sap_vs_tera)
    elif negocio == "autonomia":
        return await realizar_cuadre_contable_autonomia(file, df, dif_sap_vs_tera)
    else:
        logger.error(
            f"""Definio hacer el cuadre contable, pero el negocio 
            {negocio} no tiene ninguna estrategia de cuadre implementada.
            """
        )
        raise ValueError


async def realizar_cuadre_contable_soat(
    file: Literal["siniestros", "primas", "expuestos"],
    df: pl.DataFrame,
    dif_sap_vs_tera: pl.DataFrame,
) -> pl.DataFrame:
    dif_cols = [
        col for col in dif_sap_vs_tera.collect_schema().names() if "diferencia" in col
    ]
    dif = (
        dif_sap_vs_tera.lazy()
        .filter(pl.col("fecha_registro") == pl.col("fecha_registro").max())
        .select(["codigo_op", "codigo_ramo_op", "fecha_registro"] + dif_cols)
        .rename({col: col.replace("diferencia_", "") for col in dif_cols})
        .with_columns(ramo_desc=pl.lit("AUTOS OBLIGATORIO"))
        .join(
            apertura_para_asignar_diferencia_soat(), on=["codigo_op", "codigo_ramo_op"]
        )
        .collect()
    )

    if file == "siniestros":
        dif = dif.with_columns(
            conteo_pago=0,
            conteo_incurrido=0,
            conteo_desistido=0,
            atipico=0,
            fecha_siniestro=pl.col("fecha_registro"),
        )
    elif file == "primas":
        dif = dif.with_columns(prima_devengada_mod=0)

    df_cuadre = concat_df_dif(df, dif.select(df.collect_schema().names()))

    await guardar_archivos(file, df_cuadre)

    return df_cuadre


async def realizar_cuadre_contable_autonomia(
    file: Literal["siniestros", "primas", "expuestos"],
    df: pl.DataFrame,
    dif_sap_vs_tera: pl.DataFrame,
) -> pl.DataFrame:
    agrups = utils.lowercase_columns(
        pl.read_excel(
            "data/segmentacion_autonomia.xlsx",
            sheet_name=f"Cuadre_Contable_{file.capitalize()}",
        )
    )

    dif = (
        dif_sap_vs_tera.lazy()
        .with_columns(
            [
                pl.col(column).alias(column.replace("diferencia_", ""))
                for column in dif_sap_vs_tera.collect_schema().names()
                if "diferencia" in column
            ]
        )
        .join(pl.LazyFrame(agrups), on=["codigo_op", "codigo_ramo_op"])
        .with_columns(
            conteo_pago=0,
            conteo_incurrido=0,
            conteo_desistido=0,
            atipico=0,
            fecha_siniestro=pl.col("fecha_registro"),
        )
        .select(df.collect_schema().names())
        .collect()
    )

    df_cuadre = concat_df_dif(df, dif)

    await guardar_archivos(file, df_cuadre)

    return df_cuadre


def concat_df_dif(df: pl.DataFrame, dif: pl.DataFrame) -> pl.DataFrame:
    columns = df.collect_schema().names()
    return (
        pl.concat([df, dif], how="vertical_relaxed")
        .group_by(columns[: columns.index("fecha_registro") + 1])
        .sum()
        .sort(columns[: columns.index("fecha_registro") + 1])
    )


async def guardar_archivos(
    file: Literal["siniestros", "primas", "expuestos"], df_cuadre: pl.DataFrame
) -> None:
    df_cuadre.write_csv(f"data/raw/{file}.csv", separator="\t")
    df_cuadre.write_parquet(f"data/raw/{file}.parquet")
    logger.success(f"Cuadre contable para {file} realizado exitosamente.")


def apertura_para_asignar_diferencia_soat() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "codigo_op": ["01"],
            "codigo_ramo_op": ["041"],
            "ramo_desc": ["AUTOS OBLIGATORIO"],
            "apertura_canal_desc": ["DIGITAL"],
            "apertura_amparo_desc": ["Total"],
            "tipo_vehiculo": ["MOTO"],
        }
    ).with_columns(utils.crear_columna_apertura_reservas("soat"))

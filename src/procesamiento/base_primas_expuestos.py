from typing import Literal

import polars as pl

import src.constantes as ct
from src import utils


def fechas_pdn(col: pl.Expr) -> tuple[pl.Expr, pl.Expr, pl.Expr, pl.Expr]:
    return (
        (col.dt.year() * 100 + col.dt.month()).alias("Mensual"),
        (col.dt.year() * 100 + (col.dt.month() / 3).ceil() * 3).alias("Trimestral"),
        (col.dt.year() * 100 + (col.dt.month() / 6).ceil() * 6).alias("Semestral"),
        (col.dt.year() * 100 + 12).alias("Anual"),
    )


def generar_base_primas_expuestos(
    df: pl.LazyFrame, qty: Literal["primas", "expuestos"], negocio: str
) -> pl.DataFrame:
    qty_cols = list(ct.Valores().model_dump()[qty].keys())

    columnas_aperturas = utils.obtener_nombres_aperturas(negocio, qty)

    df_group = (
        df.with_columns(fechas_pdn(pl.col("fecha_registro")))
        .select(columnas_aperturas + qty_cols + list(ct.PERIODICIDADES.keys()))
        .unpivot(
            index=columnas_aperturas + qty_cols,
            variable_name="periodicidad_ocurrencia",
            value_name="periodo_ocurrencia",
        )
        .with_columns(pl.col("periodo_ocurrencia").cast(pl.Int32))
        .group_by(
            columnas_aperturas + ["periodicidad_ocurrencia", "periodo_ocurrencia"]
        )
    )

    if qty == "primas":
        df = df_group.sum()
    elif qty == "expuestos":
        df = df_group.mean()

    return df.sort(
        columnas_aperturas + ["periodicidad_ocurrencia", "periodo_ocurrencia"]
    ).collect()

from typing import Literal

import polars as pl

import src.constantes as ct
from src import utils


def fechas_pdn(col: pl.Expr) -> tuple[pl.Expr, pl.Expr, pl.Expr, pl.Expr]:
    return (
        (col.dt.year() * 100 + col.dt.month()).alias("Mensual"),
        (col.dt.year() * 100 + (col.dt.month() / 3).ceil() * 3).alias("Trimestral"),
        (col.dt.year() * 100 + (col.dt.month() / 6).ceil() * 6).alias("Semestral"),
        col.dt.year().alias("Anual"),
    )


def bases_primas_expuestos(
    df: pl.LazyFrame, qty: Literal["primas", "expuestos"], negocio: str
) -> pl.DataFrame:
    qty_cols = (
        [
            "prima_bruta",
            "prima_bruta_devengada",
            "prima_retenida",
            "prima_retenida_devengada",
        ]
        if qty == "primas"
        else ["expuestos", "vigentes"]
    )

    cols_aperts = ct.columnas_aperturas(negocio)[2:]

    df_group = (
        df.with_columns(
            fechas_pdn(pl.col("fecha_registro")),
            ramo_desc=utils.complementar_col_ramo_desc(),
        )
        .select(["ramo_desc"] + cols_aperts + qty_cols + list(ct.PERIODICIDADES.keys()))
        .unpivot(
            index=["ramo_desc"] + cols_aperts + qty_cols,
            variable_name="periodicidad_ocurrencia",
            value_name="periodo_ocurrencia",
        )
        .with_columns(pl.col("periodo_ocurrencia").cast(pl.Int32))
        .group_by(
            ["ramo_desc"]
            + cols_aperts
            + [
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
        )
    )

    if qty == "primas":
        df = df_group.sum()
    elif qty == "expuestos":
        df = df_group.mean()

    return df.sort(
        ["ramo_desc"]
        + cols_aperts
        + [
            "periodicidad_ocurrencia",
            "periodo_ocurrencia",
        ]
    ).collect()

import polars as pl
from datetime import date
from src import constantes as ct


def lowercase_columns(df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame | pl.DataFrame:
    return df.rename({column: column.lower() for column in df.collect_schema().names()})


def col_ramo_desc() -> pl.Expr:
    return pl.concat_str(
        pl.col("codigo_op"),
        pl.col("codigo_ramo_op"),
        pl.col("ramo_desc"),
        separator=" - ",
    )


def col_apertura_reservas(negocio) -> pl.Expr:
    return pl.concat_str(ct.columnas_aperturas(negocio), separator="_").alias(
        "apertura_reservas"
    )


def yyyymm_to_date(mes_yyyymm: int) -> date:
    return date(mes_yyyymm // 100, mes_yyyymm % 100, 1)

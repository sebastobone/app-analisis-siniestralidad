import polars as pl


def df_triangulos() -> pl.LazyFrame:
    return pl.scan_parquet("data/processed/base_triangulos.parquet")


def df_ult_ocurr() -> pl.LazyFrame:
    return pl.scan_parquet("data/processed/base_ultima_ocurrencia.parquet")


def df_atipicos() -> pl.LazyFrame:
    return pl.scan_parquet("data/processed/base_atipicos.parquet")


def df_expuestos() -> pl.LazyFrame:
    return pl.scan_parquet("data/processed/expuestos.parquet")


def df_primas() -> pl.LazyFrame:
    return pl.scan_parquet("data/processed/primas.parquet")

import polars as pl


def df_triangulos(path_plantilla: str) -> pl.LazyFrame:
    return pl.scan_parquet(
        f"{path_plantilla}/../data/processed/base_triangulos.parquet"
    )


def df_ult_ocurr(path_plantilla: str) -> pl.LazyFrame:
    return pl.scan_parquet(
        f"{path_plantilla}/../data/processed/base_ultima_ocurrencia.parquet"
    )


def df_atipicos(path_plantilla: str) -> pl.LazyFrame:
    return pl.scan_parquet(f"{path_plantilla}/../data/processed/base_atipicos.parquet")


def df_expuestos(path_plantilla: str) -> pl.LazyFrame:
    return pl.scan_parquet(f"{path_plantilla}/../data/processed/expuestos.parquet")


def df_primas(path_plantilla: str) -> pl.LazyFrame:
    return pl.scan_parquet(f"{path_plantilla}/../data/processed/primas.parquet")

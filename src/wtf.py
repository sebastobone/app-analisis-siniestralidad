import polars as pl
from extraccion.tera_connect import col_apertura_reservas

(
    pl.scan_parquet("data/raw/autonomia/expuestos.parquet")
    .select([col_apertura_reservas(), pl.all()])
    .collect()
    .write_parquet("data/raw/autonomia/expuestos.parquet")
)

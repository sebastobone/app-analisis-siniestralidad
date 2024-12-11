import polars as pl

segm = pl.read_excel("data/segmentacion_autonomia.xlsx", )

df_incurrido = (
    pl.scan_parquet("data/raw/autonomia/siniestros_brutos.parquet")
    .join(
        pl.scan_parquet("data/raw/autonomia/siniestros_cedidos.parquet"),
        on=[
            "fecha_registro",
            "poliza_id",
            "asegurado_id",
            "plan_individual_id",
            "siniestro_id",
            "amparo_id",
        ],
        how="full",
        coalesce=True,
        validate="1:1",
    )
    .fill_null(0)
    .join(pl.scan_parquet("data/catalogos/ramos.parquet"), on="ramo_id")
    .with_columns(
        codigo_ramo_op=pl.when(
            (pl.col("ramo_id") == 78)
            & (~pl.col("amparo_id").is_in[930, 641, 64082, 61296, 18647, -1])
        )
        .then(pl.lit("AAV"))
        .otherwise(pl.col("codigo_ramo_op")),
        ramo_desc=pl.when(
            (pl.col("ramo_id") == 78)
            & (~pl.col("amparo_id").is_in[930, 641, 64082, 61296, 18647, -1])
        )
        .then(pl.lit("ANEXOS VI"))
        .otherwise(pl.col("ramo_desc")),
    )
)

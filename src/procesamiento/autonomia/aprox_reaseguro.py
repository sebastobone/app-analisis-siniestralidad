import polars as pl

segm = pl.read_excel(
    "data/segmentacion_autonomia.xlsx",
    sheet_name=[
        "add_pe_Canal-Poliza",
        "add_pe_Canal-Canal",
        "add_pe_Canal-Sucursal",
        "add_pe_Amparos",
        "Atipicos",
        "Inc_Ced_Atipicos",
        "SAP_Sinis_Ced",
    ],
)

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
    .join(pl.scan_parquet("data/catalogos/planes.parquet"), on="plan_individual_id")
    .with_columns(
        codigo_ramo_op=pl.when(
            (pl.col("ramo_id") == 78)
            & (~pl.col("amparo_id").is_in([930, 641, 64082, 61296, 18647, -1]))
        )
        .then(pl.lit("AAV"))
        .otherwise(pl.col("codigo_ramo_op")),
        ramo_desc=pl.when(
            (pl.col("ramo_id") == 78)
            & (~pl.col("amparo_id").is_in([930, 641, 64082, 61296, 18647, -1]))
        )
        .then(pl.lit("ANEXOS VI"))
        .otherwise(pl.col("ramo_desc")),
    )
    .join(pl.scan_parquet("data/catalogos/sucursales.parquet"), on="sucursal_id")
    .join(
        segm["add_pe_Canal-Poliza"].lazy().unique(),
        on=["poliza_id", "codigo_ramo_op", "compania_id"],
        how="left",
    )
    .join(
        segm["add_pe_Canal-Canal"].lazy().unique(),
        on=["canal_comercial_id", "codigo_ramo_op", "compania_id"],
        how="left",
        suffix="_1",
    )
    .join(
        segm["add_pe_Canal-Sucursal"].lazy().unique(),
        on=["sucursal_id_id", "codigo_ramo_op", "compania_id"],
        how="left",
        suffix="_2",
    )
    .with_columns(
        apertura_canal_desc=pl.coalesce(
            pl.col("apertura_canal_desc"),
            pl.col("apertura_canal_desc_1"),
            pl.col("apertura_canal_desc_2"),
            pl.when(pl.col("ramo_id").is_in([78, 274]) & pl.col("compania_id").eq(3))
            .then(pl.lit("Otros Banca"))
            .when(pl.col("ramo_id").eq(274) & pl.col("compania_id").eq(4))
            .then(pl.lit("Otros"))
            .otherwise(pl.lit("Resto")),
        )
    )
    .join(
        segm["add_pe_Amparos"].lazy().unique(),
        on=["codigo_ramo_op", "compania_id", "amparo_id", "apertura_canal_desc"],
        how="left",
    )
    .with_columns(pl.col("apertura_amparo_desc").fill_null(pl.lit("RESTO")))
    .join(
        segm["Atipicos"].lazy().unique(),
        on=["compania_id", "codigo_ramo_op", "siniestro_id", "apertura_amparo_desc"],
        how="left",
    )
    .with_columns(pl.col("atipico").fill_null(0))
    .select(
        [
            "fecha_siniestro",
            "fecha_registro",
            "asegurado_id",
            "poliza_id",
            "nombre_tecnico",
            "codigo_op",
            "codigo_ramo_op",
            "siniestro_id",
            "atipico",
            "tipo_estado_siniestro_cd",
            "apertura_canal_desc",
            "nombre_canal_comercial",
            "nombre_sucursal",
            "apertura_amparo_desc",
            "pago_bruto",
            "pago_cedido",
            "aviso_bruto",
            "aviso_cedido",
        ]
    )
    .group_by(
        [
            "fecha_siniestro",
            "fecha_registro",
            "asegurado_id",
            "poliza_id",
            "nombre_tecnico",
            "codigo_op",
            "codigo_ramo_op",
            "siniestro_id",
            "atipico",
            "tipo_estado_siniestro_cd",
            "apertura_canal_desc",
            "nombre_canal_comercial",
            "nombre_sucursal",
            "apertura_amparo_desc",
        ]
    )
    .sum()
    .with_columns(
        [
            (pl.col(f"{qty}_bruto") - pl.col(f"{qty}_cedido")).alias(f"{qty}_retenido")
            for qty in ["pago", "aviso"]
        ]
    )
    .collect()
)

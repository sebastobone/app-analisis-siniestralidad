import polars as pl

from src import utils

from . import segmentaciones


def base_incurrido() -> pl.LazyFrame:
    segm = segmentaciones.segm()
    return (
        pl.scan_parquet("data/raw/siniestros_brutos.parquet")
        .join(
            pl.scan_parquet("data/raw/siniestros_cedidos.parquet"),
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
        .join(
            pl.scan_parquet("data/catalogos/planes.parquet"),
            on="plan_individual_id",
        )
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
            utils.lowercase_columns(segm["add_pe_Canal-Poliza"])
            .lazy()
            .with_columns(
                pl.col("poliza_id").cast(pl.Int64), pl.col("compania_id").cast(pl.Int32)
            )
            .unique(),
            on=["poliza_id", "codigo_ramo_op", "compania_id"],
            how="left",
        )
        .join(
            utils.lowercase_columns(segm["add_pe_Canal-Canal"])
            .lazy()
            .with_columns(
                pl.col("compania_id").cast(pl.Int32),
                pl.col("canal_comercial_id").cast(pl.Int32),
            )
            .unique(),
            on=["canal_comercial_id", "codigo_ramo_op", "compania_id"],
            how="left",
            suffix="_1",
        )
        .join(
            utils.lowercase_columns(segm["add_pe_Canal-Sucursal"])
            .lazy()
            .with_columns(
                pl.col("compania_id").cast(pl.Int32),
                pl.col("sucursal_id").cast(pl.Int32),
            )
            .unique(),
            on=["sucursal_id", "codigo_ramo_op", "compania_id"],
            how="left",
            suffix="_2",
        )
        .with_columns(
            apertura_canal_desc=pl.coalesce(
                pl.col("apertura_canal_desc"),
                pl.col("apertura_canal_desc_1"),
                pl.col("apertura_canal_desc_2"),
                pl.when(
                    pl.col("codigo_ramo_op").is_in(["081", "AAV", "083"])
                    & pl.col("compania_id").eq(3)
                )
                .then(pl.lit("Otros Banca"))
                .when(
                    pl.col("codigo_ramo_op").is_in(["081", "AAV", "083"])
                    & pl.col("compania_id").eq(4)
                )
                .then(pl.lit("Otros"))
                .otherwise(pl.lit("Resto")),
            )
        )
        .join(
            utils.lowercase_columns(segm["add_e_Amparos"])
            .lazy()
            .with_columns(
                pl.col("compania_id").cast(pl.Int32), pl.col("amparo_id").cast(pl.Int32)
            )
            .unique(),
            on=["codigo_ramo_op", "compania_id", "amparo_id", "apertura_canal_desc"],
            how="left",
        )
        .with_columns(pl.col("apertura_amparo_desc").fill_null(pl.lit("RESTO")))
        .join(
            utils.lowercase_columns(segm["Atipicos"])
            .lazy()
            .with_columns(pl.col("compania_id").cast(pl.Int32))
            .unique(),
            on=[
                "compania_id",
                "codigo_ramo_op",
                "siniestro_id",
                "apertura_amparo_desc",
            ],
            how="left",
        )
        .with_columns(pl.col("atipico").fill_null(0))
        .group_by(
            [
                "fecha_siniestro",
                "fecha_registro",
                "asegurado_id",
                "poliza_id",
                "numero_poliza",
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
        .agg(
            [
                pl.col("pago_bruto").sum(),
                pl.col("aviso_bruto").sum(),
                pl.col("pago_cedido").sum(),
                pl.col("aviso_cedido").sum(),
            ]
        )
        .with_columns(
            [
                (pl.col(f"pago_{attr}") + pl.col(f"aviso_{attr}")).alias(
                    f"incurrido_{attr}"
                )
                for attr in ["bruto", "cedido"]
            ]
        )
        .with_columns(
            [
                (pl.col(f"{qty}_bruto") - pl.col(f"{qty}_cedido")).alias(
                    f"{qty}_retenido"
                )
                for qty in ["pago", "aviso", "incurrido"]
            ]
        )
    )

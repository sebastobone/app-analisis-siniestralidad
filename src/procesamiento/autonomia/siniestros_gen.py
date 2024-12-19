import polars as pl
from . import base_incurrido
from . import aprox_reaseguro
import constantes as ct
import datetime
from calendar import monthrange


def conteo(
    df_incurrido: pl.LazyFrame, cond: pl.Expr, agg_col: str, column_name: str
) -> pl.LazyFrame:
    return (
        df_incurrido.filter(
            (~pl.col("tipo_estado_siniestro_cd").is_in(["N", "O", "D", "C"])) & cond
        )
        .group_by(
            [
                "fecha_siniestro",
                "codigo_op",
                "codigo_ramo_op",
                "apertura_canal_desc",
                "apertura_amparo_desc",
                "siniestro_id",
                "atipico",
            ]
        )
        .agg([pl.col("fecha_registro").min(), pl.col(agg_col).sum()])
        .filter(pl.col(agg_col) > 1000)
        .group_by(
            [
                "fecha_siniestro",
                "codigo_op",
                "codigo_ramo_op",
                "apertura_canal_desc",
                "apertura_amparo_desc",
                "fecha_registro",
                "atipico",
            ]
        )
        .agg(pl.col("siniestro_id").n_unique())
        .rename({"siniestro_id": column_name})
    )


def main() -> None:
    df_incurrido = base_incurrido.base_incurrido()

    if datetime.date.today() < datetime.date(
        ct.END_DATE.year,
        ct.END_DATE.month,
        monthrange(ct.END_DATE.year, ct.END_DATE.month)[1],
    ) + datetime.timedelta(days=ct.DIA_CARGA_REASEGURO):
        df_incurrido = aprox_reaseguro.main(df_incurrido)

    conteo_pago = conteo(
        df_incurrido,
        pl.col("pago_bruto").abs() > 1000,
        "pago_bruto",
        "conteo_pago",
    )
    conteo_incurrido = conteo(
        df_incurrido,
        pl.col("incurrido_bruto").abs() > 1000,
        "incurrido_bruto",
        "conteo_incurrido",
    )

    en_rva = (
        df_incurrido.group_by(
            [
                "siniestro_id",
                "codigo_op",
                "codigo_ramo_op",
                "apertura_amparo_desc",
                "atipico",
            ]
        )
        .agg(pl.col("aviso_bruto").sum())
        .filter(pl.col("aviso_bruto") > 1000)
        .with_columns(en_reserva=1)
    )

    sin_pagos_bruto = (
        df_incurrido.join(
            en_rva,
            on=[
                "siniestro_id",
                "codigo_op",
                "codigo_ramo_op",
                "apertura_amparo_desc",
                "atipico",
            ],
            how="left",
        )
        .filter(pl.col("en_reserva").is_null())
        .group_by(
            [
                "codigo_op",
                "codigo_ramo_op",
                "siniestro_id",
                "apertura_amparo_desc",
                "atipico",
            ]
        )
        .agg(pl.col("pago_bruto").abs().max())
        .filter(pl.col("pago_bruto") < 1000)
    )

    conteo_desistido = conteo(
        df_incurrido.join(
            sin_pagos_bruto,
            on=[
                "codigo_op",
                "codigo_ramo_op",
                "siniestro_id",
                "apertura_amparo_desc",
                "atipico",
            ],
        ),
        ~((pl.col("pago_bruto") == 0) & (pl.col("aviso_bruto") == 0)),
        "incurrido_bruto",
        "conteo_desistido",
    )

    base_pagos_aviso = df_incurrido.group_by(
        [
            "fecha_siniestro",
            "codigo_op",
            "codigo_ramo_op",
            "fecha_registro",
            "apertura_canal_desc",
            "apertura_amparo_desc",
            "atipico",
        ]
    ).agg(
        [
            pl.col("pago_bruto").sum(),
            pl.col("pago_retenido").sum(),
            pl.col("aviso_bruto").sum(),
            pl.col("aviso_retenido").sum(),
        ]
    )

    consolidado = (
        base_pagos_aviso.join(
            conteo_pago,
            on=[
                "fecha_siniestro",
                "codigo_op",
                "codigo_ramo_op",
                "fecha_registro",
                "apertura_canal_desc",
                "apertura_amparo_desc",
                "atipico",
            ],
            how="full",
            coalesce=True,
        )
        .join(
            conteo_incurrido,
            on=[
                "fecha_siniestro",
                "codigo_op",
                "codigo_ramo_op",
                "fecha_registro",
                "apertura_canal_desc",
                "apertura_amparo_desc",
                "atipico",
            ],
            how="full",
            coalesce=True,
        )
        .join(
            conteo_desistido,
            on=[
                "fecha_siniestro",
                "codigo_op",
                "codigo_ramo_op",
                "fecha_registro",
                "apertura_canal_desc",
                "apertura_amparo_desc",
                "atipico",
            ],
            how="full",
            coalesce=True,
        )
        .with_columns(
            pl.col("fecha_siniestro").clip(lower_bound=ct.INI_DATE).dt.month_start(),
            pl.col("fecha_registro").clip(lower_bound=ct.INI_DATE).dt.month_start(),
        )
        .join(
            pl.scan_parquet("data/catalogos/planes.parquet")
            .select(["codigo_ramo_op", "ramo_desc"])
            .unique(),
            on="codigo_ramo_op",
            how="left",
        )
        .with_columns(
            ramo_desc=pl.when(pl.col("codigo_ramo_op") == "AAV")
            .then(pl.lit("ANEXOS VI"))
            .otherwise(pl.col("ramo_desc")),
            apertura_reservas=pl.concat_str(
                [
                    pl.col("codigo_op"),
                    pl.col("codigo_ramo_op"),
                    pl.col("apertura_canal_desc"),
                    pl.col("apertura_amparo_desc"),
                ],
                separator="_",
            ),
        )
        .group_by(
            [
                "apertura_reservas",
                "codigo_op",
                "codigo_ramo_op",
                "ramo_desc",
                "apertura_canal_desc",
                "apertura_amparo_desc",
                "atipico",
                "fecha_siniestro",
                "fecha_registro",
            ]
        )
        .sum()
        .sort(
            [
                "apertura_reservas",
                "codigo_op",
                "codigo_ramo_op",
                "ramo_desc",
                "apertura_canal_desc",
                "apertura_amparo_desc",
                "atipico",
                "fecha_siniestro",
                "fecha_registro",
            ]
        )
        .collect()
    )

    consolidado.write_csv("data/raw/siniestros.csv", separator="\t")
    consolidado.write_parquet("data/raw/siniestros.parquet")

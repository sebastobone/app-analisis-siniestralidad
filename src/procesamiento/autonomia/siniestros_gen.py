import polars as pl

from src import utils
from src.procesamiento.autonomia import adds

from . import aprox_reaseguro, base_incurrido


def conteo(
    df_incurrido: pl.LazyFrame, cond: pl.Expr, agg_col: str, column_name: str
) -> pl.LazyFrame:
    cols_fijas = [
        "fecha_siniestro",
        "codigo_op",
        "codigo_ramo_op",
        "apertura_canal_desc",
        "apertura_amparo_desc",
        "atipico",
    ]
    return (
        df_incurrido.filter(
            (~pl.col("tipo_estado_siniestro_cd").is_in(["N", "O", "D", "C"])) & cond
        )
        .group_by(cols_fijas + ["siniestro_id"])
        .agg([pl.col("fecha_registro").min(), pl.col(agg_col).sum()])
        .filter(pl.col(agg_col) > 1000)
        .group_by(cols_fijas + ["fecha_registro"])
        .agg(pl.col("siniestro_id").n_unique())
        .rename({"siniestro_id": column_name})
    )


async def main(mes_inicio: int, mes_corte: int, aproximar_reaseguro: bool) -> None:
    df_incurrido = base_incurrido.base_incurrido()

    if aproximar_reaseguro:
        await adds.sap_sinis_ced(mes_corte)
        df_incurrido = aprox_reaseguro.main(df_incurrido, mes_corte)

    conteo_pago = conteo(
        df_incurrido, pl.col("pago_bruto").abs() > 1000, "pago_bruto", "conteo_pago"
    )
    conteo_incurrido = conteo(
        df_incurrido,
        pl.col("incurrido_bruto").abs() > 1000,
        "incurrido_bruto",
        "conteo_incurrido",
    )

    cols_base_desist = [
        "siniestro_id",
        "codigo_op",
        "codigo_ramo_op",
        "apertura_amparo_desc",
        "atipico",
    ]
    en_rva = (
        df_incurrido.group_by(cols_base_desist)
        .agg(pl.col("aviso_bruto").sum())
        .filter(pl.col("aviso_bruto") > 1000)
        .with_columns(en_reserva=1)
    )

    sin_pagos_bruto = (
        df_incurrido.join(en_rva, on=cols_base_desist, how="left")
        .filter(pl.col("en_reserva").is_null())
        .group_by(cols_base_desist)
        .agg(pl.col("pago_bruto").abs().max())
        .filter(pl.col("pago_bruto") < 1000)
    )

    conteo_desistido = conteo(
        df_incurrido.join(sin_pagos_bruto, on=cols_base_desist),
        ~((pl.col("pago_bruto") == 0) & (pl.col("aviso_bruto") == 0)),
        "incurrido_bruto",
        "conteo_desistido",
    )

    cols_base = [
        "fecha_siniestro",
        "codigo_op",
        "codigo_ramo_op",
        "fecha_registro",
        "apertura_canal_desc",
        "apertura_amparo_desc",
        "atipico",
    ]

    base_pagos_aviso = df_incurrido.group_by(cols_base).agg(
        [
            pl.col("pago_bruto").sum(),
            pl.col("pago_retenido").sum(),
            pl.col("aviso_bruto").sum(),
            pl.col("aviso_retenido").sum(),
        ]
    )

    cols_finales = [
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

    consolidado = (
        base_pagos_aviso.join(conteo_pago, on=cols_base, how="full", coalesce=True)
        .join(conteo_incurrido, on=cols_base, how="full", coalesce=True)
        .join(conteo_desistido, on=cols_base, how="full", coalesce=True)
        .with_columns(
            pl.col("fecha_siniestro")
            .clip(lower_bound=utils.yyyymm_to_date(mes_inicio))
            .dt.month_start(),
            pl.col("fecha_registro")
            .clip(lower_bound=utils.yyyymm_to_date(mes_inicio))
            .dt.month_start(),
        )
        .join(
            pl.scan_parquet("data/catalogos/planes.parquet")
            .select(["codigo_ramo_op", "ramo_desc"])
            .unique(),
            on="codigo_ramo_op",
            how="left",
        )
        .with_columns(
            utils.crear_columna_apertura_reservas("autonomia"),
            ramo_desc=pl.when(pl.col("codigo_ramo_op") == "AAV")
            .then(pl.lit("ANEXOS VI"))
            .otherwise(pl.col("ramo_desc")),
        )
        .group_by(cols_finales)
        .sum()
        .sort(cols_finales)
        .collect()
    )

    consolidado.write_csv("data/raw/siniestros.csv", separator="\t")
    consolidado.write_parquet("data/raw/siniestros.parquet")

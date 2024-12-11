import polars as pl
import constantes as ct
from datetime import date

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


def lowercase_cols(df: pl.DataFrame | pl.LazyFrame):
    return df.rename({col: col.lower() for col in df.collect_schema().names()})


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
    .join(pl.scan_parquet("data/raw/catalogos/planes.parquet"), on="plan_individual_id")
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
    .join(pl.scan_parquet("data/raw/catalogos/sucursales.parquet"), on="sucursal_id")
    .join(
        lowercase_cols(segm["add_pe_Canal-Poliza"].lazy())
        .with_columns(
            pl.col("poliza_id").cast(pl.Int64), pl.col("compania_id").cast(pl.Int32)
        )
        .unique(),
        on=["poliza_id", "codigo_ramo_op", "compania_id"],
        how="left",
    )
    .join(
        lowercase_cols(segm["add_pe_Canal-Canal"].lazy())
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
        lowercase_cols(segm["add_pe_Canal-Sucursal"].lazy())
        .with_columns(
            pl.col("compania_id").cast(pl.Int32), pl.col("sucursal_id").cast(pl.Int32)
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
            pl.when(pl.col("ramo_id").is_in([78, 274]) & pl.col("compania_id").eq(3))
            .then(pl.lit("Otros Banca"))
            .when(pl.col("ramo_id").eq(274) & pl.col("compania_id").eq(4))
            .then(pl.lit("Otros"))
            .otherwise(pl.lit("Resto")),
        )
    )
    .join(
        lowercase_cols(segm["add_pe_Amparos"].lazy())
        .with_columns(
            pl.col("compania_id").cast(pl.Int32), pl.col("amparo_id").cast(pl.Int32)
        )
        .unique(),
        on=["codigo_ramo_op", "compania_id", "amparo_id", "apertura_canal_desc"],
        how="left",
    )
    .with_columns(pl.col("apertura_amparo_desc").fill_null(pl.lit("RESTO")))
    .join(
        lowercase_cols(segm["Atipicos"].lazy())
        .with_columns(pl.col("compania_id").cast(pl.Int32))
        .unique(),
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
)


pcts_retencion = (
    df_incurrido.filter(
        ~((pl.col("ramo_id") == 274) & (pl.col("compania_id") == 3))
        & (pl.col("atipico") == 0)
        & (
            pl.col("fecha_registro").is_between(
                ct.END_DATE_PL.dt.month_end().dt.offset_by("-13mo"),
                ct.END_DATE_PL.dt.month_end().dt.offset_by("-1mo"),
            )
        )
    )
    .select(
        [
            "codigo_op",
            "codigo_ramo_op",
            "apertura_canal_desc",
            "apertura_amparo_desc",
            "atipico",
            "pago_retenido",
            "pago_bruto",
            "aviso_retenido",
            "aviso_bruto",
        ]
    )
    .group_by(
        [
            "codigo_op",
            "codigo_ramo_op",
            "apertura_canal_desc",
            "apertura_amparo_desc",
            "atipico",
        ]
    )
    .sum()
    .with_columns(
        porcentaje_retencion=(
            (pl.col("pago_retenido") + pl.col("aviso_retenido"))
            / (pl.col("pago_bruto") + pl.col("aviso_bruto"))
        ).clip(upper_bound=1)
    )
    .drop(["pago_bruto", "aviso_bruto", "pago_retenido", "aviso_retenido"])
)

vig_contrato_083 = (
    pl.DataFrame(
        pl.date_range(
            pl.date(1990, 1, 1), ct.END_DATE, interval="1y", eager=True
        ).alias("primer_dia_ano")
    )
    .lazy()
    .with_columns(
        vigencia_contrato=pl.col("primer_dia_ano").dt.year(),
        inicio_vigencia_contrato=pl.col("primer_dia_ano").dt.offset_by("6mo"),
        fin_vigencia_contrato=pl.col("primer_dia_ano")
        .dt.offset_by("18mo")
        .dt.offset_by("-1d"),
        prioridad=pl.when(pl.col("primer_dia_ano").dt.year() < 2022)
        .then(200e6)
        .otherwise(250e6),
    )
)


inc_atip = (
    lowercase_cols(segm["Inc_Ced_Atipicos"].lazy())
    .rename({"ramo": "codigo_ramo_op", "sociedad": "codigo_op"})
    .join(
        lowercase_cols(segm["add_pe_Canal-Poliza"].lazy()).unique(),
        on=["numero_poliza", "codigo_ramo_op", "codigo_op"],
        how="left",
    )
    .join(
        lowercase_cols(segm["add_pe_Canal-Canal"].lazy()).unique(),
        on=["canal_comercial_id", "codigo_ramo_op", "compania_id"],
        how="left",
        suffix="_1",
    )
    .join(
        lowercase_cols(segm["add_pe_Canal-Sucursal"].lazy())
        .with_columns(pl.col("sucursal_id").cast(pl.String))
        .unique(),
        on=["sucursal_id", "codigo_ramo_op", "codigo_op"],
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
        lowercase_cols(segm["add_pe_Amparos"].lazy()).unique(),
        on=["codigo_ramo_op", "codigo_op", "amparo_id", "apertura_canal_desc"],
        how="left",
    )
    .with_columns(pl.col("apertura_amparo_desc").fill_null(pl.lit("RESTO")))
    .join(
        df_incurrido.select(["siniestro_id", "apertura_amparo_desc", "fecha_registro"])
        .group_by(["siniestro_id", "apertura_amparo_desc"])
        .max(),
        on=["siniestro_id", "apertura_amparo_desc"],
        how="left",
    )
    .group_by(
        [
            "codigo_op",
            "codigo_ramo_op",
            "siniestro_id",
            "apertura_canal_desc",
            "apertura_amparo_desc",
            "fecha_registro",
        ]
    )
    .agg([pl.col("pago_cedido").sum(), pl.col("aviso_cedido").sum()])
    .collect()
)


df_incurrido_2 = (
    df_incurrido.join_where(
        vig_contrato_083,
        pl.col("fecha_siniestro").is_between(
            pl.col("inicio_vigencia_contrato"), pl.col("fin_vigencia_contrato")
        ),
    )
    .join(
        pcts_retencion,
        on=[
            "codigo_op",
            "codigo_ramo_op",
            "apertura_canal_desc",
            "apertura_amparo_desc",
            "atipico",
        ],
        how="left",
    )
    .join(
        inc_atip,
        on=[
            "codigo_op",
            "codigo_ramo_op",
            "apertura_amparo_desc",
            "siniestro_id" "fecha_registro",
        ],
        how="left",
    )
    .with_columns(
        [
            pl.col(qty)
            .sum()
            .over(
                ["asegurado_id", "vigencia_contrato", "codigo_ramo_op", "codigo_op"],
                order_by="fecha_registro",
            )
            .alias(f"{qty}_acum")
            for qty in ["pago_bruto", "aviso_bruto", "pago_retenido", "aviso_retenido"]
        ]
    )
    .with_columns(
        incurrido_bruto=pl.col("pago_bruto") + pl.col("aviso_bruto"),
        incurrido_bruto_acum=pl.col("pago_bruto_acum") + pl.col("aviso_bruto_acum"),
    )
    .with_columns(
        pago_retenido_aprox=pl.when(
            (pl.col("fecha_registro").dt.month_end() != ct.END_DATE_PL.dt.month_end())
            | (
                date.today()
                > ct.END_DATE_PL.dt.month_end().dt.offset_by(
                    f"{ct.DIA_CARGA_REASEGURO}d"
                )
            )
        )
        .then(pl.col("pago_retenido"))
        .when(
            (pl.col("atipico") == 0)
            & (pl.col("codigo_ramo_op") == "083")
            & (pl.col("codigo_op") == "02")
        )
        .then(
            pl.when(pl.col("numero_poliza") == "083004273427")
            .then(pl.col("pago_bruto") * 0.2)
            .when(pl.col("nombre_tecnico") == "PILOTOS")
            .then(pl.col("pago_bruto") * 0.4)
            .when(pl.col("nombre_tecnico") == "INPEC")
            .then(pl.col("pago_bruto") * 0.25)
            .when(pl.col("apertura_canal_desc") == "Banco Agrario")
            .then(pl.col("pago_bruto") * 0.1)
            .when(
                (pl.col("apertura_canal_desc") == "Banco-L")
                & (
                    pl.col("fecha_siniestro").is_between(
                        date(2017, 11, 1), date(2021, 10, 31)
                    )
                )
            )
            .then(pl.col("pago_bruto") * 0.1)
            .when(
                pl.col("pago_bruto_acum") - pl.col("pago_bruto") < pl.col("prioridad")
            )
            .then(
                pl.col("pago_bruto").clip(
                    upper_bound=pl.col("prioridad")
                    - (pl.col("pago_bruto_acum") - pl.col("pago_bruto"))
                )
            )
            .when(
                pl.col("pago_bruto_acum") - pl.col("pago_bruto") >= pl.col("prioridad")
            )
            .then(0)
        )
        .when(pl.col("atipico") == 0)
        .then(pl.col("pago_bruto") * pl.col("porcentaje_retencion").fill_null(1))
        .when(pl.col("atipico") == 1)
        .then(pl.col("pago_bruto") - pl.col("pago_cedido_atip").fill_null(0))
    )
    .with_columns(
        aviso_retenido_aprox=pl.when(
            (pl.col("fecha_registro").dt.month_end() != ct.END_DATE_PL.dt.month_end())
            | (
                date.today()
                > ct.END_DATE_PL.dt.month_end().dt.offset_by(
                    f"{ct.DIA_CARGA_REASEGURO}d"
                )
            )
        )
        .then(pl.col("aviso_retenido"))
        .when(
            (pl.col("atipico") == 0)
            & (pl.col("codigo_ramo_op") == "083")
            & (pl.col("codigo_op") == "02")
        )
        .then(
            pl.when(pl.col("numero_poliza") == "083004273427")
            .then(pl.col("aviso_bruto") * 0.2)
            .when(pl.col("nombre_tecnico") == "PILOTOS")
            .then(pl.col("aviso_bruto") * 0.4)
            .when(pl.col("nombre_tecnico") == "INPEC")
            .then(pl.col("aviso_bruto") * 0.25)
            .when(pl.col("apertura_canal_desc") == "Banco Agrario")
            .then(pl.col("aviso_bruto") * 0.1)
            .when(
                (pl.col("apertura_canal_desc") == "Banco-L")
                & (
                    pl.col("fecha_siniestro").is_between(
                        date(2017, 11, 1), date(2021, 10, 31)
                    )
                )
            )
            .then(pl.col("aviso_bruto") * 0.1)
            .when(
                (
                    pl.col("incurrido_bruto_acum") - pl.col("incurrido_bruto")
                    < pl.col("prioridad")
                )
                & (pl.col("aviso_bruto") >= 0)
            )
            .then(
                pl.col("aviso_bruto").clip(
                    upper_bound=pl.col("prioridad")
                    - (pl.col("incurrido_bruto_acum") - pl.col("incurrido_bruto"))
                )
            )
            .when(
                (
                    pl.col("incurrido_bruto_acum") - pl.col("incurrido_bruto")
                    < pl.col("prioridad")
                )
                & (pl.col("aviso_bruto") < 0)
            )
            .then(pl.col("aviso_bruto"))
            .when(
                (
                    pl.col("incurrido_bruto_acum") - pl.col("incurrido_bruto")
                    >= pl.col("prioridad")
                )
                & (pl.col("aviso_bruto") >= 0)
            )
            .then(0)
            .when(
                (
                    pl.col("incurrido_bruto_acum") - pl.col("incurrido_bruto")
                    >= pl.col("prioridad")
                )
                & (pl.col("aviso_bruto") < 0)
                & (
                    (
                        pl.col("aviso_bruto")
                        - (
                            pl.col("prioridad")
                            - (
                                pl.col("incurrido_bruto_acum")
                                - pl.col("incurrido_bruto")
                            )
                        )
                    ).clip(upper_bound=0)
                    == 0
                )
            )
            .then(-pl.col("pago_retenido_aprox"))
            .when(
                (pl.col("incurrido_bruto_acum") - pl.col("incurrido_bruto") >= 0)
                & (pl.col("aviso_bruto") < 0)
            )
            .then(
                (
                    pl.col("aviso_bruto")
                    - (
                        pl.col("prioridad")
                        - (pl.col("incurrido_bruto_acum") - pl.col("incurrido_bruto"))
                    )
                ).clip(upper_bound=0)
            )
        )
        .when(pl.col("atipico") == 0)
        .then(pl.col("aviso_bruto") * pl.col("porcentaje_retencion").fill_null(1))
        .when(pl.col("atipico") == 1)
        .then(pl.col("aviso_bruto") - pl.col("aviso_cedido_atip").fill_null(0))
    )
).with_columns(
    pago_cedido_aprox=pl.col("pago_bruto") - pl.col("pago_retenido_aprox"),
    aviso_cedido_aprox=pl.col("aviso_bruto") - pl.col("aviso_retenido_aprox"),
    incurrido_cedido_aprox=pl.col("pago_bruto")
    - pl.col("pago_retenido_aprox")
    + pl.col("aviso_bruto")
    - pl.col("aviso_retenido_aprox"),
)

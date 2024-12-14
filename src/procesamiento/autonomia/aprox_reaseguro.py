import polars as pl
import constantes as ct
from datetime import date
from . import base_incurrido
from . import utils


def cond_no_aproximar_reaseguro() -> pl.Expr:
    return (pl.col("fecha_registro").dt.month_start() != ct.END_DATE_PL) | (
        date.today()
        > ct.END_DATE_PL.dt.month_end().dt.offset_by(f"{ct.DIA_CARGA_REASEGURO}d")
    )


def dif_vida_grupo() -> pl.Expr:
    return (
        pl.when(
            (pl.col("codigo_op") == "02")
            & (pl.col("codigo_ramo_op") == "083")
            & (
                ~pl.col("nombre_canal_comercial").is_in(
                    ["SUCURSALES", "PROMOTORAS", "CORPORATIVO", "GRAN EMPRESA"]
                )
            )
            & (pl.col("nombre_sucursal") != "BANCOLOMBIA DEUDORES CONSUMO Y OTROS")
        )
        .then(True)
        .otherwise(False)
    )


def pcts_retencion(df_incurrido: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df_incurrido.filter(
            ~((pl.col("codigo_ramo_op") == "083") & (pl.col("codigo_op") == "02"))
            & (pl.col("atipico") == 0)
            & (
                pl.col("fecha_registro").is_between(
                    ct.END_DATE_PL.dt.month_end().dt.offset_by("-13mo"),
                    ct.END_DATE_PL.dt.month_end().dt.offset_by("-1mo"),
                )
            )
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
        .agg(
            [
                pl.col("pago_bruto").sum(),
                pl.col("pago_retenido").sum(),
                pl.col("aviso_retenido").sum(),
                pl.col("aviso_bruto").sum(),
            ]
        )
        .with_columns(
            porcentaje_retencion=(
                (pl.col("pago_retenido") + pl.col("aviso_retenido"))
                / (pl.col("pago_bruto") + pl.col("aviso_bruto"))
            ).clip(upper_bound=1)
        )
        .drop(["pago_bruto", "aviso_bruto", "pago_retenido", "aviso_retenido"])
    )


def vig_contrato_083() -> pl.LazyFrame:
    return (
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


def incurridos_cedidos_atipicos(df_incurrido: pl.LazyFrame) -> pl.LazyFrame:
    segm = utils.segm()
    return (
        utils.lowercase_cols(segm["Inc_Ced_Atipicos"].lazy())
        .rename({"ramo": "codigo_ramo_op", "sociedad": "codigo_op"})
        .join(
            utils.lowercase_cols(segm["add_pe_Canal-Poliza"].lazy()).unique(),
            on=["numero_poliza", "codigo_ramo_op", "codigo_op"],
            how="left",
        )
        .join(
            utils.lowercase_cols(segm["add_pe_Canal-Canal"].lazy())
            .with_columns(pl.col("canal_comercial_id").cast(pl.String))
            .unique(),
            on=["canal_comercial_id", "codigo_ramo_op", "compania_id"],
            how="left",
            suffix="_1",
        )
        .join(
            utils.lowercase_cols(segm["add_pe_Canal-Sucursal"].lazy())
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
                pl.when(
                    pl.col("codigo_ramo_op").is_in(["081", "AAV", "083"])
                    & pl.col("codigo_op").eq(pl.lit("02"))
                )
                .then(pl.lit("Otros Banca"))
                .when(
                    pl.col("codigo_ramo_op").eq(pl.lit("083"))
                    & pl.col("codigo_op").eq(pl.lit("02"))
                )
                .then(pl.lit("Otros"))
                .otherwise(pl.lit("Resto")),
            )
        )
        .join(
            utils.lowercase_cols(segm["add_e_Amparos"].lazy()).unique(),
            on=["codigo_ramo_op", "codigo_op", "amparo_id", "apertura_canal_desc"],
            how="left",
        )
        .with_columns(pl.col("apertura_amparo_desc").fill_null(pl.lit("RESTO")))
        .join(
            df_incurrido.select(
                ["siniestro_id", "apertura_amparo_desc", "fecha_registro"]
            )
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
    )


def aprox_reaseguro(df_incurrido: pl.LazyFrame, inc_atip: pl.LazyFrame) -> pl.LazyFrame:
    df = (
        df_incurrido.join_where(
            vig_contrato_083(),
            pl.col("fecha_siniestro").is_between(
                pl.col("inicio_vigencia_contrato"), pl.col("fin_vigencia_contrato")
            ),
        )
        .join(
            pcts_retencion(df_incurrido),
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
                "siniestro_id",
                "fecha_registro",
            ],
            how="left",
            suffix="_atip",
        )
        .with_columns(
            [
                pl.col(qty)
                .sum()
                .over(
                    [
                        "asegurado_id",
                        "vigencia_contrato",
                        "codigo_ramo_op",
                        "codigo_op",
                    ],
                    order_by="fecha_registro",
                )
                .alias(f"{qty}_acum")
                for qty in [
                    "pago_bruto",
                    "aviso_bruto",
                    "pago_retenido",
                    "aviso_retenido",
                ]
            ]
        )
        .with_columns(
            incurrido_bruto=pl.col("pago_bruto") + pl.col("aviso_bruto"),
            incurrido_bruto_acum=pl.col("pago_bruto_acum") + pl.col("aviso_bruto_acum"),
        )
        .with_columns(
            pago_retenido_aprox=pl.when(cond_no_aproximar_reaseguro())
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
                    pl.col("pago_bruto_acum") - pl.col("pago_bruto")
                    < pl.col("prioridad")
                )
                .then(
                    pl.col("pago_bruto").clip(
                        upper_bound=pl.col("prioridad")
                        - (pl.col("pago_bruto_acum") - pl.col("pago_bruto"))
                    )
                )
                .when(
                    pl.col("pago_bruto_acum") - pl.col("pago_bruto")
                    >= pl.col("prioridad")
                )
                .then(0)
            )
            .when(pl.col("atipico") == 0)
            .then(pl.col("pago_bruto") * pl.col("porcentaje_retencion").fill_null(1))
            .when(pl.col("atipico") == 1)
            .then(pl.col("pago_bruto") - pl.col("pago_cedido_atip").fill_null(0))
        )
        .with_columns(
            aviso_retenido_aprox=pl.when(cond_no_aproximar_reaseguro())
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
                            - (
                                pl.col("incurrido_bruto_acum")
                                - pl.col("incurrido_bruto")
                            )
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
    return df


def cuadre_cedido_sap(
    df_incurrido: pl.LazyFrame, inc_atip: pl.LazyFrame
) -> pl.LazyFrame:
    segm = utils.segm()

    inc_atip = (
        inc_atip.with_columns(
            incurrido_cedido=pl.col("pago_cedido") + pl.col("aviso_cedido")
        )
        .select(["codigo_op", "codigo_ramo_op", "pago_cedido", "incurrido_cedido"])
        .group_by(["codigo_op", "codigo_ramo_op"])
        .sum()
        .rename(
            {
                "pago_cedido": "pago_cedido_atipicos",
                "incurrido_cedido": "incurrido_cedido_atipicos",
            }
        )
    )

    sap = (
        utils.lowercase_cols(segm["SAP_Sinis_Ced"].lazy())
        .join(inc_atip, on=["codigo_op", "codigo_ramo_op"], how="left")
        .with_columns(
            pago_cedido_tipicos=pl.col("pago_cedido")
            - pl.col("pago_cedido_atipicos").fill_null(0),
            aviso_cedido_tipicos=pl.col("pago_cedido")
            + pl.col("aviso_cedido")
            - pl.col("incurrido_cedido_atipicos").fill_null(0),
        )
        .with_columns(
            incurrido_cedido_tipicos=pl.col("pago_cedido_tipicos")
            + pl.col("aviso_cedido_tipicos")
        )
    )

    tera = (
        df_incurrido.filter(
            (pl.col("fecha_registro").dt.month_start() == ct.END_DATE_PL)
            & (pl.col("atipico") == 0)
        )
        .with_columns(dif_vida_grupo=dif_vida_grupo())
        .with_columns(
            pago_cedido_tipicos_tera=pl.when(pl.col("dif_vida_grupo"))
            .then(0)
            .otherwise(pl.col("pago_cedido_aprox")),
            incurrido_cedido_tipicos_tera=pl.when(pl.col("dif_vida_grupo"))
            .then(0)
            .otherwise(pl.col("incurrido_cedido_aprox")),
            pago_cedido_referencia_tera=pl.when(pl.col("dif_vida_grupo"))
            .then("pago_cedido_aprox")
            .otherwise(0),
            incurrido_cedido_referencia_tera=pl.when(pl.col("dif_vida_grupo"))
            .then("incurrido_cedido_aprox")
            .otherwise(0),
        )
        .select(
            [
                "codigo_op",
                "codigo_ramo_op",
                "pago_cedido_tipicos_tera",
                "pago_cedido_referencia_tera",
                "incurrido_cedido_tipicos_tera",
                "incurrido_cedido_referencia_tera",
            ]
        )
        .group_by(["codigo_op", "codigo_ramo_op"])
        .sum()
    )

    escala_sap = sap.join(
        tera, on=["codigo_op", "codigo_ramo_op"], validate="1:1"
    ).with_columns(
        [
            (
                (
                    pl.col(f"{qty}_cedido_tipicos")
                    - pl.col(f"{qty}_cedido_referencia_tera").fill_null(0)
                )
                / pl.when(pl.col(f"{qty}_cedido_tipicos_tera") != 0)
                .then(pl.col(f"{qty}_cedido_tipicos_tera"))
                .otherwise(1)
            ).alias(f"factor_escala_{qty}")
            for qty in ["pago", "incurrido"]
        ]
    )

    return (
        df_incurrido.join(escala_sap, on=["codigo_op", "codigo_ramo_op"], how="left")
        .with_columns(
            [
                pl.when(cond_no_aproximar_reaseguro())
                .then(pl.col(f"{qty}_cedido_aprox"))
                .when(dif_vida_grupo())
                .then(pl.col(f"{qty}_cedido_aprox"))
                .otherwise(
                    pl.col(f"{qty}_cedido_aprox")
                    * pl.col(f"factor_escala_{qty}").fill_null(1)
                )
                .alias(f"{qty}_cedido_aprox_cuadrado")
                for qty in ["pago", "incurrido"]
            ]
        )
        .with_columns(
            aviso_cedido_aprox_cuadrado=pl.col("incurrido_cedido_aprox_cuadrado")
            - pl.col("pago_cedido_aprox_cuadrado"),
            pago_retenido_aprox_cuadrado=pl.col("pago_bruto")
            - pl.col("pago_cedido_aprox_cuadrado"),
        )
        .with_columns(
            aviso_retenido_aprox_cuadrado=pl.col("aviso_bruto")
            - pl.col("aviso_cedido_aprox_cuadrado")
        )
    )


def main() -> pl.LazyFrame:
    df_incurrido = base_incurrido.base_incurrido()
    inc_atip = incurridos_cedidos_atipicos(df_incurrido)
    df_incurrido_reaseguro_aprox = aprox_reaseguro(df_incurrido, inc_atip)
    return cuadre_cedido_sap(df_incurrido_reaseguro_aprox, inc_atip)

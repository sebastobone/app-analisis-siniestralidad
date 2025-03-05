from datetime import date

import polars as pl

from src import utils

from . import segmentaciones
from .base_incurrido import cruzar_segmentaciones

FILTRO_083 = (pl.col("codigo_ramo_op") == "083") & (pl.col("codigo_op") == "02")


def cond_meses_ant(mes_corte: int) -> pl.Expr:
    return pl.col("fecha_registro").dt.month_start() < utils.yyyymm_to_date(mes_corte)


def aperturas_no_cuadrables_083() -> pl.Expr:
    return (
        pl.when(
            FILTRO_083
            & ~pl.col("nombre_canal_comercial").is_in(
                ["SUCURSALES", "PROMOTORAS", "CORPORATIVO", "GRAN EMPRESA"]
            )
            & (pl.col("nombre_sucursal") != "BANCOLOMBIA DEUDORES CONSUMO Y OTROS")
        )
        .then(True)
        .otherwise(False)
    )


def calcular_pcts_retencion(df_incurrido: pl.LazyFrame, mes_corte: int) -> pl.LazyFrame:
    return (
        df_incurrido.filter(
            ~FILTRO_083
            & (pl.col("atipico") == 0)
            & pl.col("fecha_registro").is_between(
                pl.lit(utils.yyyymm_to_date(mes_corte))
                .dt.month_end()
                .dt.offset_by("-13mo"),
                pl.lit(utils.yyyymm_to_date(mes_corte))
                .dt.month_end()
                .dt.offset_by("-1mo"),
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
        .agg([pl.sum("incurrido_bruto"), pl.sum("incurrido_retenido")])
        .with_columns(
            porcentaje_retencion=(
                pl.col("incurrido_retenido") / pl.col("incurrido_bruto")
            ).clip(upper_bound=1)
        )
        .drop(["incurrido_bruto", "incurrido_retenido"])
    )


def generar_vigencias_contrato_083(mes_corte) -> pl.LazyFrame:
    return (
        pl.DataFrame(
            pl.date_range(
                pl.date(1900, 1, 1),
                utils.yyyymm_to_date(mes_corte),
                interval="1y",
                eager=True,
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


def procesar_incurridos_cedidos_atipicos(segm: dict[str, pl.DataFrame]) -> pl.LazyFrame:
    return (
        utils.lowercase_columns(segm["Inc_Ced_Atipicos"])
        .lazy()
        .rename({"ramo": "codigo_ramo_op", "sociedad": "codigo_op"})
        .with_columns(
            pl.col("canal_comercial_id").cast(pl.Int32),
            pl.col("sucursal_id").cast(pl.Int32),
            pl.col("amparo_id").cast(pl.Int32),
            compania_id=pl.when(pl.col("codigo_op") == "01")
            .then(4)
            .otherwise(3)
            .cast(pl.Int32),
        )
        .pipe(cruzar_segmentaciones, segm)
        .group_by(
            [
                "codigo_op",
                "codigo_ramo_op",
                "siniestro_id",
                "apertura_canal_desc",
                "apertura_amparo_desc",
            ]
        )
        .agg([pl.sum("pago_cedido"), pl.sum("aviso_cedido")])
        .with_columns(
            incurrido_cedido=pl.col("pago_cedido") + pl.col("aviso_cedido"),
        )
    )


def agregar_fechas_atipicos(
    inc_atip: pl.LazyFrame, df_incurrido: pl.LazyFrame
) -> pl.LazyFrame:
    return inc_atip.join(
        df_incurrido.group_by(["siniestro_id", "apertura_amparo_desc"]).agg(
            pl.col("fecha_registro").max()
        ),
        on=["siniestro_id", "apertura_amparo_desc"],
        how="left",
    )


def aproximar_reaseguro_no_083(
    df_incurrido_no_083: pl.LazyFrame,
    pcts_retencion: pl.LazyFrame,
    inc_atip: pl.LazyFrame,
    mes_corte: int,
) -> pl.LazyFrame:
    return (
        df_incurrido_no_083.join(
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
            agregar_fechas_atipicos(inc_atip, df_incurrido_no_083),
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
            pago_retenido=pl.when(cond_meses_ant(mes_corte))
            .then(pl.col("pago_retenido"))
            .when(pl.col("atipico") == 0)
            .then(pl.col("pago_bruto") * pl.col("porcentaje_retencion").fill_null(1))
            .when(pl.col("atipico") == 1)
            .then(pl.col("pago_bruto") - pl.col("pago_cedido_atip").fill_null(0)),
            aviso_retenido=pl.when(cond_meses_ant(mes_corte))
            .then(pl.col("aviso_retenido"))
            .when(pl.col("atipico") == 0)
            .then(pl.col("aviso_bruto") * pl.col("porcentaje_retencion").fill_null(1))
            .when(pl.col("atipico") == 1)
            .then(pl.col("aviso_bruto") - pl.col("aviso_cedido_atip").fill_null(0)),
        )
    )


def aproximar_reaseguro_083(
    df_incurrido_083: pl.LazyFrame,
    inc_atip: pl.LazyFrame,
    mes_corte: int,
) -> pl.LazyFrame:
    return (
        df_incurrido_083.join_where(
            generar_vigencias_contrato_083(mes_corte),
            pl.col("fecha_siniestro").is_between(
                pl.col("inicio_vigencia_contrato"), pl.col("fin_vigencia_contrato")
            ),
        )
        .join(
            agregar_fechas_atipicos(inc_atip, df_incurrido_083),
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
                .over(["asegurado_id", "vigencia_contrato"], order_by="fecha_registro")
                .alias(f"{qty}_acum")
                for qty in ["pago_bruto", "aviso_bruto", "incurrido_bruto"]
            ]
        )
        .with_columns(
            pago_retenido=pl.when(cond_meses_ant(mes_corte))
            .then(pl.col("pago_retenido"))
            .when(pl.col("atipico") == 1)
            .then(pl.col("pago_bruto") - pl.col("pago_cedido_atip").fill_null(0))
            .when(pl.col("atipico") == 0)
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
        )
        .with_columns(
            aviso_retenido=pl.when(cond_meses_ant(mes_corte))
            .then(pl.col("aviso_retenido"))
            .when(pl.col("atipico") == 1)
            .then(pl.col("aviso_bruto") - pl.col("aviso_cedido_atip").fill_null(0))
            .when(pl.col("atipico") == 0)
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
                .then(-pl.col("pago_retenido"))
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
        )
    )


def limpiar_atipicos_sap(
    segm: dict[str, pl.DataFrame], inc_atip: pl.LazyFrame
) -> pl.LazyFrame:
    inc_atip = (
        inc_atip.group_by(["codigo_op", "codigo_ramo_op"])
        .agg([pl.sum("pago_cedido"), pl.sum("incurrido_cedido")])
        .sum()
    )

    return (
        utils.lowercase_columns(segm["SAP_Sinis_Ced"])
        .lazy()
        .with_columns(incurrido_cedido=pl.col("pago_cedido") + pl.col("aviso_cedido"))
        .join(inc_atip, on=["codigo_op", "codigo_ramo_op"], how="left", suffix="_atip")
        .with_columns(
            pago_cedido_tipicos_sap=pl.col("pago_cedido")
            - pl.col("pago_cedido_atip").fill_null(0),
            incurrido_cedido_tipicos_sap=pl.col("incurrido_cedido")
            - pl.col("incurrido_cedido_atip").fill_null(0),
        )
        .with_columns(
            aviso_cedido_tipicos_sap=pl.col("incurrido_cedido_tipicos_sap")
            - pl.col("pago_cedido_tipicos_sap")
        )
    )


def cuadrar_reaseguro_con_sap(
    df_incurrido: pl.LazyFrame,
    sap_tipicos: pl.LazyFrame,
    mes_corte: int,
) -> pl.LazyFrame:
    tera_tipicos = (
        df_incurrido.filter(
            (
                pl.col("fecha_registro").dt.month_start()
                == utils.yyyymm_to_date(mes_corte)
            )
            & (pl.col("atipico") == 0)
        )
        .with_columns(aperturas_no_cuadrables_083=aperturas_no_cuadrables_083())
        .with_columns(
            pago_cedido_tipicos_cuadrables_tera=pl.when(
                pl.col("aperturas_no_cuadrables_083")
            )
            .then(0)
            .otherwise(pl.col("pago_cedido")),
            incurrido_cedido_tipicos_cuadrables_tera=pl.when(
                pl.col("aperturas_no_cuadrables_083")
            )
            .then(0)
            .otherwise(pl.col("incurrido_cedido")),
            pago_cedido_tipicos_no_cuadrables_tera=pl.when(
                pl.col("aperturas_no_cuadrables_083")
            )
            .then("pago_cedido")
            .otherwise(0),
            incurrido_cedido_tipicos_no_cuadrables_tera=pl.when(
                pl.col("aperturas_no_cuadrables_083")
            )
            .then("incurrido_cedido")
            .otherwise(0),
        )
        .group_by(["codigo_op", "codigo_ramo_op"])
        .agg(
            [
                pl.sum("pago_cedido_tipicos_cuadrables_tera"),
                pl.sum("pago_cedido_tipicos_no_cuadrables_tera"),
                pl.sum("incurrido_cedido_tipicos_cuadrables_tera"),
                pl.sum("incurrido_cedido_tipicos_no_cuadrables_tera"),
            ]
        )
    )

    escala_sap = sap_tipicos.join(
        tera_tipicos, on=["codigo_op", "codigo_ramo_op"], validate="1:1"
    ).with_columns(
        [
            (
                (
                    pl.col(f"{qty}_cedido_tipicos_sap")
                    - pl.col(f"{qty}_cedido_tipicos_no_cuadrables_tera").fill_null(0)
                )
                / pl.when(pl.col(f"{qty}_cedido_tipicos_cuadrables_tera") != 0)
                .then(pl.col(f"{qty}_cedido_tipicos_cuadrables_tera"))
                .otherwise(1)
            ).alias(f"factor_escala_{qty}")
            for qty in ["pago", "incurrido"]
        ]
    )

    return (
        df_incurrido.join(escala_sap, on=["codigo_op", "codigo_ramo_op"], how="left")
        .with_columns(
            [
                pl.when(
                    cond_meses_ant(mes_corte)
                    | aperturas_no_cuadrables_083()
                    | (pl.col("atipico") == 1)
                )
                .then(pl.col(f"{qty}_cedido"))
                .otherwise(
                    pl.col(f"{qty}_cedido")
                    * pl.col(f"factor_escala_{qty}").fill_null(0)
                )
                .alias(f"{qty}_cedido")
                for qty in ["pago", "incurrido"]
            ]
        )
        .with_columns(
            aviso_cedido=pl.col("incurrido_cedido") - pl.col("pago_cedido"),
            pago_retenido=pl.col("pago_bruto") - pl.col("pago_cedido"),
        )
        .with_columns(aviso_retenido=pl.col("aviso_bruto") - pl.col("aviso_cedido"))
    )


def main(df_incurrido: pl.LazyFrame, mes_corte: int) -> pl.LazyFrame:
    segm = segmentaciones.segm()
    inc_atip = procesar_incurridos_cedidos_atipicos(segm)
    pcts_retencion = calcular_pcts_retencion(df_incurrido, mes_corte)

    df_reaseguro_aprox_no_083 = (
        aproximar_reaseguro_no_083(
            df_incurrido.filter(~FILTRO_083), pcts_retencion, inc_atip, mes_corte
        )
        .drop("porcentaje_retencion")
        .collect()
    )

    df_reaseguro_aprox_083 = (
        aproximar_reaseguro_083(df_incurrido.filter(FILTRO_083), inc_atip, mes_corte)
        .select(df_reaseguro_aprox_no_083.collect_schema().names())
        .collect()
    )

    df_reaseguro_aprox = (
        pl.DataFrame(pl.concat([df_reaseguro_aprox_no_083, df_reaseguro_aprox_083]))
        .with_columns(
            pago_cedido=pl.col("pago_bruto") - pl.col("pago_retenido"),
            aviso_cedido=pl.col("aviso_bruto") - pl.col("aviso_retenido"),
            incurrido_cedido=pl.col("pago_bruto")
            - pl.col("pago_retenido")
            + pl.col("aviso_bruto")
            - pl.col("aviso_retenido"),
        )
        .lazy()
    )

    sap_tipicos = limpiar_atipicos_sap(segm, inc_atip)
    return cuadrar_reaseguro_con_sap(df_reaseguro_aprox, sap_tipicos, mes_corte)

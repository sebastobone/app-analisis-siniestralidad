from datetime import date
from math import ceil, floor
from typing import Literal

import polars as pl

import src.constantes as ct
from src import utils


def preparar_base_siniestros(
    df: pl.LazyFrame, mes_inicio: date, mes_corte: date
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    df_sinis = df.with_columns(
        pl.col("fecha_siniestro").clip(upper_bound=pl.col("fecha_registro"))
    )

    df_sinis = df_sinis.with_columns(
        [
            (pl.col(f"pago_{attr}") + pl.col(f"aviso_{attr}")).alias(
                f"incurrido_{attr}"
            )
            for attr in ["bruto", "retenido"]
        ],
        conteo_incurrido=pl.col("conteo_incurrido") - pl.col("conteo_desistido"),
    ).select(
        ["apertura_reservas", "atipico", "fecha_siniestro", "fecha_registro"]
        + ct.COLUMNAS_QTYS
    )

    df_sinis_tipicos = df_sinis.filter(pl.col("atipico") == 0).drop("atipico")
    df_sinis_atipicos = df_sinis.filter(pl.col("atipico") == 1).drop("atipico")

    bases_fechas = []
    for tipo_fecha in ["fecha_siniestro", "fecha_registro"]:
        bases_fechas.append(
            pl.LazyFrame(
                pl.date_range(mes_inicio, mes_corte, interval="1mo", eager=True).alias(
                    tipo_fecha
                )
            )
        )

    base = (
        df_sinis_tipicos.select("apertura_reservas")
        .unique()
        .join(bases_fechas[0], how="cross")
        .join(bases_fechas[1], how="cross")
        .filter(pl.col("fecha_siniestro") <= pl.col("fecha_registro"))
    )

    df_sinis_tipicos = df_sinis_tipicos.join(
        base,
        on=["apertura_reservas", "fecha_siniestro", "fecha_registro"],
        how="full",
        coalesce=True,
    ).fill_null(0)

    return df_sinis_tipicos, df_sinis_atipicos


def mes_ult_ocurr_triangulos(
    mes_corte: date,
    origin_grain: Literal["Mensual", "Trimestral", "Semestral", "Anual"],
) -> date:
    anno = mes_corte.year
    mes = mes_corte.month
    periodicidad = ct.PERIODICIDADES[origin_grain]
    return (
        date(anno - 1, 12, 1)
        if mes < periodicidad
        else date(anno, floor(mes / periodicidad) * periodicidad, 1)
    )


def mes_prim_ocurr_periodo_act(
    mes_corte: date,
    origin_grain: Literal["Mensual", "Trimestral", "Semestral", "Anual"],
) -> date:
    anno = mes_corte.year
    mes = mes_corte.month
    periodicidad = ct.PERIODICIDADES[origin_grain]
    return date(anno, ceil(mes / periodicidad) * periodicidad - (periodicidad - 1), 1)


def construir_triangulos(
    df_tri: pl.LazyFrame,
    origin_grain: Literal["Mensual", "Trimestral", "Semestral", "Anual"],
    development_grain: Literal["Mensual", "Trimestral", "Semestral", "Anual"],
    mes_corte: date,
    tipo_analisis: Literal["triangulos", "entremes"],
) -> pl.LazyFrame:
    df_tri = (
        df_tri.filter(
            pl.col("fecha_registro")
            <= (
                mes_corte
                if tipo_analisis == "entremes"
                else mes_ult_ocurr_triangulos(mes_corte, origin_grain)
            )
        )
        .with_columns(
            periodo_ocurrencia=utils.date_to_yyyymm_pl(
                pl.col("fecha_siniestro"), origin_grain
            ),
            periodo_desarrollo=utils.date_to_yyyymm_pl(
                pl.col("fecha_registro"), development_grain
            ),
        )
        .drop(["fecha_siniestro", "fecha_registro"])
        .group_by(["apertura_reservas", "periodo_ocurrencia", "periodo_desarrollo"])
        .sum()
        .sort(["apertura_reservas", "periodo_ocurrencia", "periodo_desarrollo"])
        .with_columns(
            [
                pl.col(qty_column)
                .cum_sum()
                .over(["apertura_reservas", "periodo_ocurrencia"])
                for qty_column in ct.COLUMNAS_QTYS
            ],
            index_desarrollo=pl.col("periodo_desarrollo")
            .cum_count()
            .over(["apertura_reservas", "periodo_ocurrencia"]),
            periodicidad_ocurrencia=pl.lit(origin_grain),
            periodicidad_desarrollo=pl.lit(development_grain),
        )
        .with_columns(
            diagonal=pl.when(
                pl.col("index_desarrollo")
                == pl.col("index_desarrollo")
                .max()
                .over(
                    [
                        "apertura_reservas",
                        "periodo_ocurrencia",
                        "periodicidad_desarrollo",
                    ]
                )
            )
            .then(1)
            .otherwise(0)
        )
        .select(
            [
                "apertura_reservas",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "periodicidad_desarrollo",
                "periodo_desarrollo",
                "diagonal",
                "index_desarrollo",
            ]
            + ct.COLUMNAS_QTYS
        )
    )

    return df_tri


def construir_diagonales_triangulo(
    df_tri: pl.LazyFrame,
    origin_grain: Literal["Mensual", "Trimestral", "Semestral", "Anual"],
    mes_inicio: date,
    mes_corte: date,
    base_output: Literal["atipicos", "ultima_ocurrencia"],
) -> pl.LazyFrame:
    df_diagonales = (
        df_tri.filter(
            (
                pl.col("fecha_siniestro")
                >= (
                    mes_prim_ocurr_periodo_act(mes_corte, origin_grain)
                    if base_output == "ultima_ocurrencia"
                    else mes_inicio
                )
            )
            & (pl.col("fecha_registro") <= mes_corte)
        )
        .with_columns(
            periodo_ocurrencia=utils.date_to_yyyymm_pl(
                pl.col("fecha_siniestro"), "Mensual"
            ),
            periodicidad_triangulo=pl.lit(origin_grain),
            periodicidad_ocurrencia=pl.lit("Mensual"),
        )
        .group_by(
            [
                "apertura_reservas",
                "periodicidad_triangulo",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
        )
        .agg([pl.col(qty_column).sum() for qty_column in ct.COLUMNAS_QTYS])
        .sort(
            [
                "apertura_reservas",
                "periodicidad_triangulo",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
        )
    )

    if base_output == "atipicos":
        df_diagonales = df_diagonales.drop("periodicidad_triangulo")

    return df_diagonales


def generar_bases_siniestros(
    df: pl.LazyFrame,
    tipo_analisis: Literal["triangulos", "entremes"],
    mes_inicio: date,
    mes_corte: date,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    df_sinis_tipicos, df_sinis_atipicos = preparar_base_siniestros(
        df, mes_inicio, mes_corte
    )

    if tipo_analisis == "triangulos":
        base_triangulos = pl.concat(
            [
                construir_triangulos(
                    df_sinis_tipicos, "Mensual", "Mensual", mes_corte, tipo_analisis
                ),
                construir_triangulos(
                    df_sinis_tipicos,
                    "Trimestral",
                    "Trimestral",
                    mes_corte,
                    tipo_analisis,
                ),
                construir_triangulos(
                    df_sinis_tipicos, "Semestral", "Semestral", mes_corte, tipo_analisis
                ),
                construir_triangulos(
                    df_sinis_tipicos, "Anual", "Anual", mes_corte, tipo_analisis
                ),
            ]
        ).collect()
        base_ult_ocurr = pl.DataFrame(
            schema=[
                "apertura_reservas",
                "periodicidad_triangulo",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
            + ct.COLUMNAS_QTYS,
        )
    else:
        base_triangulos = pl.concat(
            [
                construir_triangulos(
                    df_sinis_tipicos, "Trimestral", "Mensual", mes_corte, tipo_analisis
                ),
                construir_triangulos(
                    df_sinis_tipicos, "Semestral", "Mensual", mes_corte, tipo_analisis
                ),
                construir_triangulos(
                    df_sinis_tipicos, "Anual", "Mensual", mes_corte, tipo_analisis
                ),
            ]
        ).collect()
        base_ult_ocurr = pl.concat(
            [
                construir_diagonales_triangulo(
                    df_sinis_tipicos,
                    "Trimestral",
                    mes_inicio,
                    mes_corte,
                    "ultima_ocurrencia",
                ),
                construir_diagonales_triangulo(
                    df_sinis_tipicos,
                    "Semestral",
                    mes_inicio,
                    mes_corte,
                    "ultima_ocurrencia",
                ),
                construir_diagonales_triangulo(
                    df_sinis_tipicos,
                    "Anual",
                    mes_inicio,
                    mes_corte,
                    "ultima_ocurrencia",
                ),
            ]
        ).collect()

    base_atipicos = construir_diagonales_triangulo(
        df_sinis_atipicos, "Mensual", mes_inicio, mes_corte, "atipicos"
    ).collect()

    return base_triangulos, base_ult_ocurr, base_atipicos

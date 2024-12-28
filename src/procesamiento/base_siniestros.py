from datetime import date
from math import ceil, floor
from typing import Literal

import polars as pl

import src.constantes as ct
from src import utils

COLUMNAS_QTYS = [
    "pago_bruto",
    "pago_retenido",
    "incurrido_bruto",
    "incurrido_retenido",
    "conteo_pago",
    "conteo_incurrido",
    "conteo_desistido",
]


def aperturas(negocio: str) -> None:
    return (
        pl.scan_parquet("data/raw/siniestros.parquet")
        .with_columns(ramo_desc=utils.complementar_col_ramo_desc())
        .filter(pl.col("fecha_registro") >= pl.col("fecha_siniestro"))
        .select(["apertura_reservas", "ramo_desc"] + ct.columnas_aperturas(negocio))
        .drop(["codigo_op", "codigo_ramo_op"])
        .unique()
        .sort("apertura_reservas")
        .collect()
        .write_csv("data/processed/aperturas.csv", separator="\t")
    )


def preparar_base_siniestros(
    df: pl.LazyFrame, mes_inicio: int, mes_corte: int
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    df_sinis = df.with_columns(
        pl.col("fecha_siniestro").clip(upper_bound=pl.col("fecha_registro")),
        ramo_desc=utils.complementar_col_ramo_desc(),
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
        [
            "apertura_reservas",
            "atipico",
            "fecha_siniestro",
            "fecha_registro",
        ]
        + COLUMNAS_QTYS
    )

    df_sinis_tipicos = df_sinis.filter(pl.col("atipico") == 0).drop("atipico")
    df_sinis_atipicos = df_sinis.filter(pl.col("atipico") == 1).drop("atipico")

    bases_fechas = []
    for tipo_fecha in ["fecha_siniestro", "fecha_registro"]:
        bases_fechas.append(
            pl.LazyFrame(
                pl.date_range(
                    utils.yyyymm_to_date(mes_inicio),
                    utils.yyyymm_to_date(mes_corte),
                    interval="1mo",
                    eager=True,
                ).alias(tipo_fecha)
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


def mes_ult_ocurr_triangulos(mes_corte: date, origin_grain: str) -> date:
    anno = mes_corte.year
    mes = mes_corte.month
    periodicidad = ct.PERIODICIDADES[origin_grain]
    return (
        date(anno - 1, 12, 1)
        if mes < periodicidad
        else date(anno, floor(mes / periodicidad) * periodicidad, 1)
    )


def mes_prim_ocurr_periodo_act(mes_corte: date, origin_grain: str) -> date:
    anno = mes_corte.year
    mes = mes_corte.month
    periodicidad = ct.PERIODICIDADES[origin_grain]
    return date(anno, ceil(mes / periodicidad) * periodicidad - (periodicidad - 1), 1)


def construir_triangulos(
    df_tri: pl.LazyFrame,
    origin_grain: str,
    development_grain: str,
    mes_corte: date,
    tipo_analisis: str,
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
        .group_by(
            [
                "apertura_reservas",
                "periodo_ocurrencia",
                "periodo_desarrollo",
            ]
        )
        .sum()
        .sort(["apertura_reservas", "periodo_ocurrencia", "periodo_desarrollo"])
        .with_columns(
            [
                pl.col(qty_column)
                .cum_sum()
                .over(["apertura_reservas", "periodo_ocurrencia"])
                for qty_column in COLUMNAS_QTYS
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
            + COLUMNAS_QTYS
        )
    )

    return df_tri


def construir_diagonales_triangulo(
    df_tri: pl.LazyFrame,
    origin_grain: str,
    mes_inicio: int,
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
                    else utils.yyyymm_to_date(mes_inicio)
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
        .agg([pl.col(qty_column).sum() for qty_column in COLUMNAS_QTYS])
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


def guardar_archivos(df: pl.DataFrame, nombre_archivo: str) -> None:
    df.write_parquet(f"data/processed/{nombre_archivo}.parquet")


def generar_bases_siniestros(
    df: pl.LazyFrame, tipo_analisis: str, mes_inicio: int, mes_corte: int
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    df_sinis_tipicos, df_sinis_atipicos = preparar_base_siniestros(
        df, mes_inicio, mes_corte
    )

    mes_corte_dt = utils.yyyymm_to_date(mes_corte)

    if tipo_analisis == "triangulos":
        base_triangulos = pl.concat(
            [
                construir_triangulos(
                    df_sinis_tipicos, "Mensual", "Mensual", mes_corte_dt, tipo_analisis
                ),
                construir_triangulos(
                    df_sinis_tipicos,
                    "Trimestral",
                    "Trimestral",
                    mes_corte_dt,
                    tipo_analisis,
                ),
                construir_triangulos(
                    df_sinis_tipicos,
                    "Semestral",
                    "Semestral",
                    mes_corte_dt,
                    tipo_analisis,
                ),
                construir_triangulos(
                    df_sinis_tipicos, "Anual", "Anual", mes_corte_dt, tipo_analisis
                ),
            ]
        ).collect()
        base_ult_ocurr = pl.DataFrame(schema=base_triangulos.schema)
    else:
        base_triangulos = pl.concat(
            [
                construir_triangulos(
                    df_sinis_tipicos,
                    "Trimestral",
                    "Mensual",
                    mes_corte_dt,
                    tipo_analisis,
                ),
                construir_triangulos(
                    df_sinis_tipicos,
                    "Semestral",
                    "Mensual",
                    mes_corte_dt,
                    tipo_analisis,
                ),
                construir_triangulos(
                    df_sinis_tipicos, "Anual", "Mensual", mes_corte_dt, tipo_analisis
                ),
            ]
        ).collect()
        base_ult_ocurr = pl.concat(
            [
                construir_diagonales_triangulo(
                    df_sinis_tipicos,
                    "Trimestral",
                    mes_inicio,
                    mes_corte_dt,
                    "ultima_ocurrencia",
                ),
                construir_diagonales_triangulo(
                    df_sinis_tipicos,
                    "Semestral",
                    mes_inicio,
                    mes_corte_dt,
                    "ultima_ocurrencia",
                ),
                construir_diagonales_triangulo(
                    df_sinis_tipicos,
                    "Anual",
                    mes_inicio,
                    mes_corte_dt,
                    "ultima_ocurrencia",
                ),
            ]
        ).collect()
        guardar_archivos(base_ult_ocurr, "base_ultima_ocurrencia")

    guardar_archivos(base_triangulos, "base_triangulos")

    base_atipicos = construir_diagonales_triangulo(
        df_sinis_atipicos, "Mensual", mes_inicio, mes_corte_dt, "atipicos"
    ).collect()
    guardar_archivos(base_atipicos, "base_atipicos")

    return base_triangulos, base_ult_ocurr, base_atipicos

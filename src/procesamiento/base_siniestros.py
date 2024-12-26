import polars as pl
from datetime import date
from math import floor, ceil
import src.constantes as ct
from src import utils


def aperturas(negocio: str) -> None:
    return (
        pl.scan_parquet("data/raw/siniestros.parquet")
        .with_columns(ramo_desc=utils.col_ramo_desc())
        .filter(pl.col("fecha_registro") >= pl.col("fecha_siniestro"))
        .select(["apertura_reservas", "ramo_desc"] + ct.columnas_aperturas(negocio))
        .drop(["codigo_op", "codigo_ramo_op"])
        .unique()
        .sort("apertura_reservas")
        .collect()
        .write_csv("data/processed/aperturas.csv", separator="\t")
    )


def sinis_prep(
    df: pl.LazyFrame, mes_inicio: int, mes_corte: int
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    df_sinis = df.with_columns(
        pl.col("fecha_siniestro").clip(upper_bound=pl.col("fecha_registro")),
        ramo_desc=utils.col_ramo_desc(),
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
            "pago_bruto",
            "pago_retenido",
            "incurrido_bruto",
            "incurrido_retenido",
            "conteo_pago",
            "conteo_incurrido",
            "conteo_desistido",
        ]
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


def triangulos(
    df_tri: pl.LazyFrame,
    origin_grain: str,
    development_grain: str,
    mes_corte: int,
    tipo_analisis: str,
) -> pl.LazyFrame:
    anno = utils.yyyymm_to_date(mes_corte).year
    mes = utils.yyyymm_to_date(mes_corte).month
    periodicidad = ct.PERIODICIDADES[origin_grain]

    mes_ult_ocurr_triangulos = (
        date(anno - 1, 12, 1)
        if mes < periodicidad
        else date(anno, floor(mes / periodicidad) * periodicidad, 1)
    )

    df_tri = (
        df_tri.filter(
            pl.col("fecha_registro")
            <= (
                utils.yyyymm_to_date(mes_corte)
                if tipo_analisis == "entremes"
                else mes_ult_ocurr_triangulos
            )
        )
        .with_columns(
            periodo_ocurrencia=utils.date_to_yyyymm(
                pl.col("fecha_siniestro"), origin_grain
            ),
            periodo_desarrollo=utils.date_to_yyyymm(
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
                for qty_column in [
                    "pago_bruto",
                    "pago_retenido",
                    "incurrido_bruto",
                    "incurrido_retenido",
                    "conteo_pago",
                    "conteo_incurrido",
                    "conteo_desistido",
                ]
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
                "pago_bruto",
                "pago_retenido",
                "incurrido_bruto",
                "incurrido_retenido",
                "conteo_pago",
                "conteo_incurrido",
                "conteo_desistido",
            ]
        )
    )

    return df_tri


def diagonales(
    df_tri: pl.LazyFrame,
    origin_grain: str,
    mes_inicio: int,
    mes_corte: int,
    ultima_ocurr: bool = False,
) -> pl.LazyFrame:
    anno = utils.yyyymm_to_date(mes_corte).year
    mes = utils.yyyymm_to_date(mes_corte).month
    periodicidad = ct.PERIODICIDADES[origin_grain]

    mes_prim_ocurr_periodo_act = date(
        anno, ceil(mes / periodicidad) * periodicidad - (periodicidad - 1), 1
    )

    df_diagonales = (
        df_tri.filter(
            pl.col("fecha_siniestro")
            >= (
                mes_prim_ocurr_periodo_act
                if ultima_ocurr
                else utils.yyyymm_to_date(mes_inicio)
            )
        )
        .with_columns(
            periodo_ocurrencia=utils.date_to_yyyymm(
                pl.col("fecha_siniestro"), "Mensual"
            ),
            periodicidad_triangulo=pl.lit(origin_grain),
            periodicidad_ocurrencia=pl.lit("Mensual"),
        )
        .select(
            [
                "apertura_reservas",
                "periodicidad_triangulo",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "pago_bruto",
                "incurrido_bruto",
                "pago_retenido",
                "incurrido_retenido",
                "conteo_pago",
                "conteo_incurrido",
                "conteo_desistido",
            ]
        )
        .group_by(
            [
                "apertura_reservas",
                "periodicidad_triangulo",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
        )
        .sum()
        .sort(
            [
                "apertura_reservas",
                "periodicidad_triangulo",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
        )
    )

    if not ultima_ocurr:
        df_diagonales = df_diagonales.drop("periodicidad_triangulo")

    return df_diagonales


def bases_siniestros(tipo_analisis: str, mes_inicio: int, mes_corte: int) -> None:
    df_sinis_tipicos, df_sinis_atipicos = sinis_prep(
        pl.scan_parquet("data/raw/siniestros.parquet"), mes_inicio, mes_corte
    )

    if tipo_analisis == "triangulos":
        base_triangulos = pl.concat(
            [
                triangulos(
                    df_sinis_tipicos, "Mensual", "Mensual", mes_corte, tipo_analisis
                ),
                triangulos(
                    df_sinis_tipicos,
                    "Trimestral",
                    "Trimestral",
                    mes_corte,
                    tipo_analisis,
                ),
                triangulos(
                    df_sinis_tipicos, "Semestral", "Semestral", mes_corte, tipo_analisis
                ),
                triangulos(
                    df_sinis_tipicos, "Anual", "Anual", mes_corte, tipo_analisis
                ),
            ]
        )
    else:
        base_triangulos = pl.concat(
            [
                triangulos(
                    df_sinis_tipicos, "Trimestral", "Mensual", mes_corte, tipo_analisis
                ),
                triangulos(
                    df_sinis_tipicos, "Semestral", "Mensual", mes_corte, tipo_analisis
                ),
                triangulos(
                    df_sinis_tipicos, "Anual", "Mensual", mes_corte, tipo_analisis
                ),
            ]
        )
        base_ult_ocurr = pl.concat(
            [
                diagonales(df_sinis_tipicos, "Trimestral", mes_inicio, mes_corte, True),
                diagonales(df_sinis_tipicos, "Semestral", mes_inicio, mes_corte, True),
                diagonales(df_sinis_tipicos, "Anual", mes_inicio, mes_corte, True),
            ]
        )
        base_ult_ocurr.collect().write_parquet(
            "data/processed/base_ultima_ocurrencia.parquet"
        )

    base_triangulos.collect().write_parquet("data/processed/base_triangulos.parquet")
    diagonales(
        df_sinis_atipicos, "Mensual", mes_inicio, mes_corte
    ).collect().write_parquet("data/processed/base_atipicos.parquet")

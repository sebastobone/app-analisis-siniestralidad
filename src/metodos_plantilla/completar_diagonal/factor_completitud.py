from datetime import date

import polars as pl

from src import constantes as ct
from src import utils

from . import chainladder as cl


def calcular_factores_completitud(
    aperturas: pl.LazyFrame, mes_corte: date
) -> pl.DataFrame:
    base_triangulos = (
        pl.scan_parquet("data/processed/base_triangulos.parquet")
        .join(
            aperturas.select(["apertura_reservas", "periodicidad_ocurrencia"]),
            on=["apertura_reservas", "periodicidad_ocurrencia"],
        )
        .filter(
            (pl.col("periodicidad_desarrollo") == "Mensual") & (pl.col("atipico") == 0)
        )
        .drop("atipico")
        .collect()
    )

    factores_completitud_aperturas = []
    for apertura in base_triangulos.get_column("apertura_reservas").unique():
        base_apertura = base_triangulos.filter(pl.col("apertura_reservas") == apertura)
        periodicidad_apertura = (
            base_apertura.get_column("periodicidad_ocurrencia").unique().item()
        )
        meses_entre_triangulos = ct.PERIODICIDADES[periodicidad_apertura]

        base_factores_completitud = (
            base_apertura.select(
                ["apertura_reservas", "periodicidad_ocurrencia", "periodo_ocurrencia"]
            )
            .unique()
            .with_columns(
                numero_periodo_ocurrencia=pl.col("periodo_ocurrencia")
                .cum_count()
                .over(
                    "apertura_reservas",
                    order_by="periodo_ocurrencia",
                    descending=True,
                )
                - 1
            )
        )

        for cantidad in ct.COLUMNAS_QTYS[:4]:
            triangulo = cl.construir_triangulo(base_apertura, cantidad)
            factores = cl.calcular_factores_desarrollo(
                triangulo, 1, 12 // meses_entre_triangulos
            )
            factores_acumulados = cl.calcular_factores_acumulados(factores)

            factor_completitud = calcular_factor_completitud(
                factores_acumulados,
                "promedio_ponderado_ventana",
                meses_entre_triangulos,
                cantidad,
                mes_corte,
            )

            base_factores_completitud = base_factores_completitud.join(
                factor_completitud, on="numero_periodo_ocurrencia", how="left"
            ).fill_null(1)

        factores_completitud_aperturas.append(
            base_factores_completitud.with_columns(
                # Para que el join despues no bote la ocurrencia mas reciente
                periodicidad_ocurrencia=pl.when(
                    pl.col("numero_periodo_ocurrencia") == 0
                )
                .then(pl.lit("Mensual"))
                .otherwise(pl.col("periodicidad_ocurrencia")),
                periodo_ocurrencia=pl.when(pl.col("numero_periodo_ocurrencia") == 0)
                .then(pl.col("periodo_ocurrencia") - (meses_entre_triangulos - 1))
                .otherwise(pl.col("periodo_ocurrencia")),
            ).drop("numero_periodo_ocurrencia")
        )

    return pl.concat(factores_completitud_aperturas)


def calcular_factor_completitud(
    factores_acumulados: pl.DataFrame,
    factor_seleccionado: str,
    meses_entre_triangulos: int,
    cantidad: str,
    mes_corte: date,
) -> pl.DataFrame:
    tabla_factor_seleccionado = factores_acumulados.select(
        ["altura", factor_seleccionado]
    ).with_columns(
        mes_del_periodo=pl.col("altura") % meses_entre_triangulos + 1,
        numero_periodo_ocurrencia=pl.col("altura") // meses_entre_triangulos,
    )

    factor_acumulado_ultimo_mes = tabla_factor_seleccionado.filter(
        pl.col("mes_del_periodo") == meses_entre_triangulos
    )

    return (
        tabla_factor_seleccionado.join(
            factor_acumulado_ultimo_mes,
            on="numero_periodo_ocurrencia",
            how="left",
            suffix="_ultimo_mes",
        )
        .with_columns(
            (1 / pl.col(factor_seleccionado)).alias(
                f"porcentaje_desarrollo_{cantidad}"
            ),
            (
                1
                / (
                    pl.col(factor_seleccionado)
                    / pl.col(f"{factor_seleccionado}_ultimo_mes")
                )
            ).alias(f"factor_completitud_{cantidad}"),
        )
        .filter(
            pl.col("mes_del_periodo")
            == utils.mes_del_periodo(mes_corte, 1, meses_entre_triangulos)
        )
        .select(
            [
                "numero_periodo_ocurrencia",
                f"porcentaje_desarrollo_{cantidad}",
                f"factor_completitud_{cantidad}",
            ]
        )
    )

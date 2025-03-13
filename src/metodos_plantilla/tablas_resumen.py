from typing import Literal

import polars as pl

from src import constantes as ct
from src import utils
from src.metodos_plantilla import insumos as ins


def generar_tablas_resumen(
    negocio: str,
    tipo_analisis: Literal["triangulos", "entremes"],
    aperturas: pl.LazyFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    diagonales = ins.df_triangulos()
    atipicos = ins.df_atipicos()
    expuestos = ins.df_expuestos()
    primas = ins.df_primas()

    diagonales = (
        diagonales.filter(pl.col("diagonal") == 1)
        .join(
            aperturas.select(["apertura_reservas", "periodicidad_ocurrencia"]),
            on=["apertura_reservas", "periodicidad_ocurrencia"],
        )
        .select(
            ["apertura_reservas", "periodicidad_ocurrencia", "periodo_ocurrencia"]
            + ct.COLUMNAS_QTYS
        )
        .pipe(unificar_tablas, negocio, aperturas, expuestos, primas)
    )

    if tipo_analisis == "entremes":
        ult_ocurr = (
            ins.df_ult_ocurr()
            .join(
                aperturas.select(
                    pl.col("apertura_reservas"),
                    pl.col("periodicidad_ocurrencia").alias("periodicidad_triangulo"),
                ),
                on=["apertura_reservas", "periodicidad_triangulo"],
            )
            .drop("periodicidad_triangulo")
            .pipe(unificar_tablas, negocio, aperturas, expuestos, primas)
        )

        diagonales = (
            diagonales.filter(
                pl.col("periodo_ocurrencia")
                != pl.col("periodo_ocurrencia").max().over("apertura_reservas")
            )
            .collect()
            .vstack(ult_ocurr.collect())
            .lazy()
        )

    tabla_entremes = diagonales.drop(
        utils.obtener_nombres_aperturas(negocio, "siniestros")
    ).collect()

    tabla_resumen = (
        diagonales.with_columns(
            frecuencia_ultimate=0,
            conteo_ultimate=0,
            severidad_ultimate_bruto=0,
            severidad_ultimate_retenido=0,
            plata_ultimate_bruto=0,
            plata_ultimate_contable_bruto=0,
            plata_ultimate_retenido=0,
            plata_ultimate_contable_retenido=0,
            aviso_bruto=pl.col("incurrido_bruto") - pl.col("pago_bruto"),
            aviso_retenido=pl.col("incurrido_retenido") - pl.col("pago_retenido"),
        )
        .sort(["apertura_reservas", "periodo_ocurrencia"])
        .collect()
    )

    tabla_atipicos = (
        atipicos.pipe(unificar_tablas, negocio, aperturas, expuestos, primas)
        .with_columns(
            frecuencia_ultimate=pl.col("conteo_incurrido") / pl.col("expuestos"),
            conteo_ultimate=pl.col("conteo_incurrido"),
            severidad_ultimate_bruto=pl.col("incurrido_bruto")
            / pl.col("conteo_incurrido"),
            severidad_ultimate_retenido=pl.col("incurrido_retenido")
            / pl.col("conteo_incurrido"),
            plata_ultimate_bruto=pl.col("incurrido_bruto"),
            plata_ultimate_contable_bruto=pl.col("incurrido_bruto"),
            plata_ultimate_retenido=pl.col("incurrido_retenido"),
            plata_ultimate_contable_retenido=pl.col("incurrido_retenido"),
            aviso_bruto=pl.col("incurrido_bruto") - pl.col("pago_bruto"),
            aviso_retenido=pl.col("incurrido_retenido") - pl.col("pago_retenido"),
            ibnr_bruto=0,
            ibnr_contable_bruto=0,
            ibnr_retenido=0,
            ibnr_contable_retenido=0,
        )
        .sort(["apertura_reservas", "periodo_ocurrencia"])
        .collect()
    )

    return tabla_resumen, tabla_atipicos, tabla_entremes


def unificar_tablas(
    df: pl.LazyFrame,
    negocio: str,
    aperturas: pl.LazyFrame,
    expuestos: pl.LazyFrame,
    primas: pl.LazyFrame,
) -> pl.LazyFrame:
    return (
        df.join(aperturas.drop("periodicidad_ocurrencia"), on="apertura_reservas")
        .select(
            aperturas.collect_schema().names()
            + ["periodo_ocurrencia"]
            + ct.COLUMNAS_QTYS
        )
        .join(
            expuestos.drop("vigentes"),
            on=utils.obtener_nombres_aperturas(negocio, "expuestos") + ["periodicidad_ocurrencia", "periodo_ocurrencia"],
            how="left",
        )
        .join(primas, on=utils.obtener_nombres_aperturas(negocio, "primas") + ["periodicidad_ocurrencia", "periodo_ocurrencia"], how="left")
        .fill_null(0)
    )


def df_aperturas() -> pl.LazyFrame:
    return pl.scan_parquet("data/raw/siniestros.parquet")

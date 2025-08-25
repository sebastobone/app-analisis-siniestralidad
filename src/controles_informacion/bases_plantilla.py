from typing import Literal

import polars as pl
import xlsxwriter

from src import constantes as ct
from src import utils
from src.models import Parametros


def generar_evidencia_info_plantilla(
    p: Parametros, resumen: pl.DataFrame, atipicos: pl.DataFrame
) -> None:
    siniestros = validar_base_siniestros(resumen, atipicos)
    primas = validar_base_primas_expuestos(p.negocio, resumen, "primas")
    expuestos = validar_base_primas_expuestos(p.negocio, resumen, "expuestos")

    carpeta = "data/controles_informacion/"
    wb_name = f"{p.mes_corte}_{p.tipo_analisis}_consistencia_informacion_plantilla.xlsx"
    with xlsxwriter.Workbook(carpeta + wb_name) as wb:
        siniestros.write_excel(wb, "Siniestros")
        primas.write_excel(wb, "Primas")
        expuestos.write_excel(wb, "Expuestos")


def validar_base_siniestros(
    resumen: pl.DataFrame, atipicos: pl.DataFrame
) -> pl.DataFrame:
    aperturas = ["apertura_reservas", "atipico"]

    original = (
        pl.read_parquet("data/raw/siniestros.parquet")
        .group_by(aperturas)
        .agg(
            [
                pl.sum(qty_column)
                for qty_column in ct.COLUMNAS_VALORES_TERADATA["siniestros"]
            ]
        )
        .with_columns(
            conteo_incurrido=pl.col("conteo_incurrido") - pl.col("conteo_desistido")
        )
    )

    base_plantilla = (
        resumen.select(["apertura_reservas"] + ct.COLUMNAS_QTYS)
        .with_columns(atipico=0)
        .vstack(
            atipicos.select(["apertura_reservas"] + ct.COLUMNAS_QTYS).with_columns(
                atipico=1
            )
        )
        .with_columns(
            [
                (pl.col(f"incurrido_{attr}") - pl.col(f"pago_{attr}")).alias(
                    f"aviso_{attr}"
                )
                for attr in ["bruto", "retenido"]
            ]
        )
        .group_by(aperturas)
        .agg(
            [
                pl.sum(qty_column)
                for qty_column in ct.COLUMNAS_VALORES_TERADATA["siniestros"]
            ]
        )
    )

    return base_plantilla.join(
        original, on=aperturas, how="full", suffix="_teradata", coalesce=True
    ).with_columns(
        [
            (pl.col(col) - pl.col(f"{col}_teradata")).alias(f"diferencia_{col}")
            for col in ct.COLUMNAS_VALORES_TERADATA["siniestros"]
        ]
    )


def validar_base_primas_expuestos(
    negocio: str, resumen: pl.DataFrame, qty: Literal["primas", "expuestos"]
) -> pl.DataFrame:
    aperturas = utils.obtener_nombres_aperturas(negocio, qty)

    original_group = pl.read_parquet(f"data/raw/{qty}.parquet").group_by(aperturas)
    base_group = resumen.group_by(aperturas)

    if qty == "primas":
        columnas_valores = ct.COLUMNAS_VALORES_TERADATA[qty]
        original = original_group.agg(
            [pl.sum(columna_valor) for columna_valor in columnas_valores]
        )
        base = base_group.agg(
            [pl.sum(columna_valor) for columna_valor in columnas_valores]
        )
    else:
        columnas_valores = ["expuestos"]
        original = original_group.agg(
            [pl.mean(columna_valor) for columna_valor in columnas_valores]
        )
        base = base_group.agg(
            [pl.mean(columna_valor) for columna_valor in columnas_valores]
        )

    return original.join(
        base, on=aperturas, how="full", suffix="_teradata", coalesce=True
    ).with_columns(
        [
            (pl.col(col) - pl.col(f"{col}_teradata")).alias(f"diferencia_{col}")
            for col in columnas_valores
        ]
    )

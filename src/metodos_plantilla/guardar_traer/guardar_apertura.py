import time

import polars as pl
import xlwings as xw
from src import utils
from src.logger_config import logger
from src.models import ModosPlantilla, RangeDimension

from .rangos_parametros import obtener_rangos_parametros


def guardar_apertura(wb: xw.Book, modos: ModosPlantilla) -> None:
    s = time.time()

    hoja_plantilla = modos.plantilla.capitalize()
    dimensiones_triangulo = utils.obtener_dimensiones_triangulo(
        wb.sheets[hoja_plantilla]
    )

    guardar_parametros(
        wb.sheets[hoja_plantilla], modos.apertura, modos.atributo, dimensiones_triangulo
    )

    if modos.plantilla != "completar_diagonal":
        guardar_ultimate(
            wb,
            modos.plantilla,
            modos.apertura,
            modos.atributo,
            dimensiones_triangulo.height,
        )
    else:
        guardar_factores_completitud(
            wb, modos.apertura, modos.atributo, dimensiones_triangulo.height
        )

    logger.success(
        utils.limpiar_espacios_log(
            f"""
            Parametros y resultados para {modos.apertura} - {modos.atributo} guardados.
            """
        )
    )

    wb.sheets["Main"]["A1"].value = "GUARDAR_APERTURA"
    wb.sheets["Main"]["A2"].value = time.time() - s


def guardar_ultimate(
    wb: xw.Book, plantilla: str, apertura: str, atributo: str, num_ocurrencias: int
):
    nombre_columna_destino = (
        f"{plantilla}_ultimate_{atributo}"
        if plantilla != "frecuencia"
        else f"{plantilla}_ultimate"
    )
    wb.macro("GuardarVector")(
        plantilla.capitalize(),
        "Resumen",
        apertura,
        atributo,
        "ultimate",
        nombre_columna_destino,
        num_ocurrencias,
    )


def guardar_factores_completitud(
    wb: xw.Book, apertura: str, atributo: str, num_ocurrencias: int
):
    for metodologia in ["pago", "incurrido"]:
        wb.macro("GuardarVector")(
            "Completar_diagonal",
            "Entremes",
            apertura,
            atributo,
            f"factor_completitud_{metodologia}",
            f"factor_completitud_{metodologia}_{atributo}",
            num_ocurrencias,
        )


def guardar_parametros(
    hoja: xw.Sheet, apertura: str, atributo: str, dimensiones_triangulo: RangeDimension
) -> None:
    for nombre_rango, valores_rango in obtener_rangos_parametros(
        hoja, dimensiones_triangulo, "Ninguna"
    ).items():
        pl.DataFrame(valores_rango.formula).transpose().write_parquet(
            f"data/db/{apertura}_{atributo}_{hoja.name}_{nombre_rango}.parquet"
        )

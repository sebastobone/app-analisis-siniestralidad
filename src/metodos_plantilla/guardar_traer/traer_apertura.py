import time

import polars as pl
import xlwings as xw
from src import utils
from src.logger_config import logger
from src.models import ModosPlantilla, RangeDimension

from .rangos_parametros import obtener_rangos_parametros


def traer_apertura(wb: xw.Book, modos: ModosPlantilla) -> None:
    s = time.time()

    hoja_plantilla = modos.plantilla.capitalize()
    dimensiones_triangulo = utils.obtener_dimensiones_triangulo(
        wb.sheets[hoja_plantilla]
    )

    traer_parametros(
        wb.sheets[hoja_plantilla], modos.apertura, modos.atributo, dimensiones_triangulo
    )

    wb.sheets["Main"]["A1"].value = "TRAER_PLANTILLA"
    wb.sheets["Main"]["A2"].value = time.time() - s


def traer_parametros(
    hoja: xw.Sheet, apertura: str, atributo: str, dimensiones_triangulo: RangeDimension
) -> None:
    for range_name, range_values in obtener_rangos_parametros(
        hoja, dimensiones_triangulo, "Ninguna"
    ).items():
        try:
            range_values.formula = pl.read_parquet(
                f"data/db/{apertura}_{atributo}_{hoja.name}_{range_name}.parquet"
            ).rows()
        except FileNotFoundError:
            logger.exception(
                utils.limpiar_espacios_log(
                    f"""
                    No se encontraron formulas para la apertura {apertura}
                    con el atributo {atributo} en la plantilla {hoja.name}.
                    Para traer un analisis, primero tiene que haberse guardado.
                    """
                )
            )
            raise

    logger.success(f"Parametros para {apertura} - {atributo} traidos.")

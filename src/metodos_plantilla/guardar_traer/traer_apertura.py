import time

import polars as pl
import xlwings as xw
from src import utils
from src.logger_config import logger
from src.models import EstructuraApertura, ModosPlantilla

from .estructura_apertura import obtener_estructura_apertura
from .rangos_parametros import obtener_rangos_parametros


def traer_apertura(wb: xw.Book, modos: ModosPlantilla, mes_corte: int) -> None:
    s = time.time()

    plantilla_name = f"Plantilla_{modos.plantilla.capitalize()}"
    estructura_apertura = obtener_estructura_apertura(wb, plantilla_name, mes_corte)

    traer_parametros(wb.sheets[plantilla_name], estructura_apertura)

    wb.sheets["Main"]["A1"].value = "TRAER_PLANTILLA"
    wb.sheets["Main"]["A2"].value = time.time() - s


def traer_parametros(hoja: xw.Sheet, estructura_apertura: EstructuraApertura) -> None:
    apertura = estructura_apertura.apertura
    atributo = estructura_apertura.atributo
    for range_name, range_values in obtener_rangos_parametros(
        hoja,
        estructura_apertura.dimensiones_triangulo,
        estructura_apertura.mes_del_periodo,
        hoja["C4"].value,
        hoja["C3"].value,
    ).items():
        try:
            range_values.formula = pl.read_csv(
                f"data/db/{apertura}_{atributo}_{hoja.name}_{range_name}.csv",
                separator="\t",
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

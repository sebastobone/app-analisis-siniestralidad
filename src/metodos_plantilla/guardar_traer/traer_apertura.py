import time

import polars as pl
import xlwings as xw
from src import constantes as ct
from src import utils
from src.logger_config import logger
from src.models import RangeDimension

from .estructura_apertura import obtener_estructura_apertura
from .rangos_parametros import obtener_rangos_parametros


def traer_apertura(wb: xw.Book, plantilla: ct.LISTA_PLANTILLAS, mes_corte: int) -> None:
    s = time.time()

    plantilla_name = f"Plantilla_{plantilla.capitalize()}"
    apertura, atributo, dimensiones_triangulo, mes_del_periodo = (
        obtener_estructura_apertura(wb, plantilla_name, mes_corte)
    )

    traer_parametros(
        wb.sheets[plantilla_name],
        apertura,
        atributo,
        dimensiones_triangulo,
        mes_del_periodo,
    )

    logger.success(f"Parametros para {apertura} - {atributo} traidos.")

    wb.sheets["Main"]["A1"].value = "TRAER_PLANTILLA"
    wb.sheets["Main"]["A2"].value = time.time() - s


def traer_parametros(
    hoja: xw.Sheet,
    apertura: str,
    atributo: str,
    dimensiones_triangulo: RangeDimension,
    mes_del_periodo: int,
) -> None:
    for range_name, range_values in obtener_rangos_parametros(
        hoja,
        dimensiones_triangulo,
        mes_del_periodo,
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

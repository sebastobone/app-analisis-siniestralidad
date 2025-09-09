import time

import polars as pl
import xlwings as xw
from src import constantes as ct
from src import utils
from src.logger_config import logger
from src.models import ModosPlantilla, Parametros, RangeDimension

from .rangos_parametros import obtener_rangos_parametros


def traer_apertura(wb: xw.Book, p: Parametros, modos: ModosPlantilla) -> None:
    s = time.time()

    hoja_plantilla = modos.plantilla.capitalize()
    dimensiones_triangulo = utils.obtener_dimensiones_triangulo(
        wb.sheets[hoja_plantilla]
    )
    tipo_indexacion, _ = utils.obtener_parametros_indexacion(p.negocio, modos.apertura)

    traer_parametros(
        wb.sheets[hoja_plantilla],
        modos.apertura,
        modos.atributo,
        tipo_indexacion,
        dimensiones_triangulo,
    )

    logger.info(f"Tiempo de traida: {round(time.time() - s, 2)} segundos.")


def traer_parametros(
    hoja: xw.Sheet,
    apertura: str,
    atributo: str,
    tipo_indexacion: ct.TIPOS_INDEXACION,
    dimensiones_triangulo: RangeDimension,
) -> None:
    for range_name, range_values in obtener_rangos_parametros(
        hoja, dimensiones_triangulo, tipo_indexacion
    ).items():
        try:
            range_values.formula = pl.read_parquet(
                f"data/db/{hoja.book.name}_{apertura}_{atributo}_{hoja.name}_{range_name}.parquet"
            ).rows()
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                utils.limpiar_espacios_log(
                    f"""
                    No se encontraron formulas para la apertura {apertura}
                    con el atributo {atributo} en la plantilla {hoja.name}.
                    Para traer un analisis, primero tiene que haberse guardado.
                    """
                )
            ) from exc

    logger.success(f"Parametros para {apertura} - {atributo} traidos.")

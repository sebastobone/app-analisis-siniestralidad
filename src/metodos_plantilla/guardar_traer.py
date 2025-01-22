import polars as pl
import xlwings as xw

from src import utils
from src.logger_config import logger
from src.metodos_plantilla.rangos_parametros import obtener_rangos_parametros
from src.models import RangeDimension


def guardar(
    hoja: xw.Sheet,
    apertura: str,
    atributo: str,
    dimensiones_triangulo: RangeDimension,
    mes_del_periodo: int,
) -> None:
    for nombre_rango, valores_rango in obtener_rangos_parametros(
        hoja,
        dimensiones_triangulo,
        mes_del_periodo,
        hoja["C4"].value,
        hoja["C3"].value,
    ).items():
        pl.DataFrame(valores_rango.formula).transpose().write_csv(
            f"data/db/{apertura}_{atributo}_{hoja.name}_{nombre_rango}.csv",
            separator="\t",
        )


def traer(
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

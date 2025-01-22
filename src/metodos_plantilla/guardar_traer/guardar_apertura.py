import time

import polars as pl
import xlwings as xw
from src import constantes as ct
from src.logger_config import logger
from src.models import RangeDimension

from .estructura_apertura import obtener_estructura_apertura
from .rangos_parametros import obtener_rangos_parametros


def guardar_apertura(
    wb: xw.Book, plantilla: ct.LISTA_PLANTILLAS, mes_corte: int
) -> None:
    s = time.time()

    plantilla_name = f"Plantilla_{plantilla.capitalize()}"
    apertura, atributo, dimensiones_triangulo, mes_del_periodo = (
        obtener_estructura_apertura(wb, plantilla_name, mes_corte)
    )

    guardar_parametros(
        wb.sheets[plantilla_name],
        apertura,
        atributo,
        dimensiones_triangulo,
        mes_del_periodo,
    )

    guardar_ultimate(
        wb,
        plantilla_name,
        dimensiones_triangulo.height,
        mes_del_periodo,
        apertura,
        atributo,
    )

    logger.success(f"Parametros y resultados para {apertura} - {atributo} guardados.")

    wb.sheets["Main"]["A1"].value = "GUARDAR_APERTURA"
    wb.sheets["Main"]["A2"].value = time.time() - s


def guardar_ultimate(
    wb: xw.Book,
    plantilla_name: str,
    num_ocurrencias: int,
    mes_del_periodo: int,
    apertura: str,
    atributo: str,
):
    cols_ultimate = {
        "Plantilla_Frec": "frec_ultimate",
        "Plantilla_Seve": f"seve_ultimate_{atributo}",
        "Plantilla_Plata": f"plata_ultimate_{atributo}",
        "Plantilla_Entremes": f"plata_ultimate_{atributo}",
    }

    wb.macro("guardar_ultimate")(
        plantilla_name,
        num_ocurrencias,
        mes_del_periodo,
        cols_ultimate[plantilla_name],
        apertura,
        atributo,
    )


def guardar_parametros(
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

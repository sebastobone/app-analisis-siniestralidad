import time

import polars as pl
import xlwings as xw
from src import utils
from src.logger_config import logger
from src.models import EstructuraApertura, ModosPlantilla

from .estructura_apertura import obtener_estructura_apertura
from .rangos_parametros import obtener_rangos_parametros


def guardar_apertura(wb: xw.Book, modos: ModosPlantilla, mes_corte: int) -> None:
    s = time.time()

    plantilla_name = f"Plantilla_{modos.plantilla.capitalize()}"
    estructura_apertura = obtener_estructura_apertura(
        wb, modos.apertura, modos.atributo, plantilla_name, mes_corte
    )

    guardar_parametros(wb.sheets[plantilla_name], estructura_apertura)
    guardar_ultimate(wb, plantilla_name, estructura_apertura)

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
    wb: xw.Book, plantilla_name: str, estructura_apertura: EstructuraApertura
):
    num_ocurrencias = estructura_apertura.dimensiones_triangulo.height
    atributo = estructura_apertura.atributo
    cols_ultimate = {
        "Plantilla_Frec": "frec_ultimate",
        "Plantilla_Seve": f"seve_ultimate_{atributo}",
        "Plantilla_Plata": f"plata_ultimate_{atributo}",
        "Plantilla_Entremes": f"plata_ultimate_{atributo}",
    }

    wb.macro("guardar_ultimate")(
        plantilla_name,
        num_ocurrencias,
        estructura_apertura.mes_del_periodo,
        cols_ultimate[plantilla_name],
        estructura_apertura.apertura,
        atributo,
    )


def guardar_parametros(hoja: xw.Sheet, estructura_apertura: EstructuraApertura) -> None:
    apertura = estructura_apertura.apertura
    atributo = estructura_apertura.atributo
    for nombre_rango, valores_rango in obtener_rangos_parametros(
        hoja,
        estructura_apertura.dimensiones_triangulo,
        estructura_apertura.mes_del_periodo,
        hoja["C4"].value,
        hoja["C3"].value,
    ).items():
        pl.DataFrame(valores_rango.formula).transpose().write_csv(
            f"data/db/{apertura}_{atributo}_{hoja.name}_{nombre_rango}.csv",
            separator="\t",
        )

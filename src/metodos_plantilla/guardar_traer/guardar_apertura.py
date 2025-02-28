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

    if modos.plantilla != "completar_diagonal":
        plantilla_name = f"Plantilla_{modos.plantilla.capitalize()}"
    else:
        plantilla_name = "Completar_diagonal"

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
    destinos = {
        "Plantilla_Frec": ["Aux_Totales", "ultimate", "frec_ultimate"],
        "Plantilla_Seve": ["Aux_Totales", "ultimate", f"seve_ultimate_{atributo}"],
        "Plantilla_Plata": ["Aux_Totales", "ultimate", f"plata_ultimate_{atributo}"],
        "Completar_diagonal": [
            "Plantilla_Entremes",
            "factor_completitud_pago",
            f"factor_completitud_pago_{atributo}",
        ],
    }

    wb.macro("guardar_vector")(
        plantilla_name,
        destinos[plantilla_name][0],
        estructura_apertura.apertura,
        estructura_apertura.atributo,
        destinos[plantilla_name][1],
        destinos[plantilla_name][2],
        num_ocurrencias,
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

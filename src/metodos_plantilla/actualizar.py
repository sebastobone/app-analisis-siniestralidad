import time

import polars as pl
import xlwings as xw

from src import constantes as ct
from src import utils
from src.logger_config import logger
from src.metodos_plantilla.generar import crear_triangulo_base_plantilla
from src.metodos_plantilla.guardar_traer.rangos_parametros import (
    obtener_indice_en_rango,
)
from src.models import ModosPlantilla, Parametros


def actualizar_plantillas(wb: xw.Book, p: Parametros, modos: ModosPlantilla):
    if modos.plantilla == "severidad":
        modos_frec = modos.model_copy(
            update={"plantilla": "frecuencia", "atributo": "bruto"}
        )
        actualizar_plantilla(wb, p, modos_frec)
    actualizar_plantilla(wb, p, modos)


def actualizar_plantilla(wb: xw.Book, p: Parametros, modos: ModosPlantilla) -> None:
    s = time.time()

    hoja = wb.sheets[modos.plantilla.capitalize()]
    verificar_plantilla_generada(hoja)

    apertura_actual = obtener_apertura_actual(wb, modos.plantilla)
    verificar_misma_periodicidad(p.negocio, apertura_actual, modos.apertura)

    cantidades = (
        ["pago", "incurrido"]
        if hoja.name != "Frecuencia"
        else ["conteo_pago", "conteo_incurrido"]
    )

    aperturas = utils.obtener_aperturas(p.negocio, "siniestros")

    triangulo = crear_triangulo_base_plantilla(
        pl.scan_parquet("data/processed/base_triangulos.parquet"),
        modos.apertura,
        modos.atributo,
        aperturas,
        cantidades,
    )

    hoja.cells(ct.FILA_INI_PLANTILLAS, ct.COL_OCURRS_PLANTILLAS).value = triangulo

    fila_apertura = obtener_indice_en_rango("apertura", hoja.range("A1:A10000"))
    hoja.cells(fila_apertura, 2).value = modos.apertura
    hoja.cells(fila_apertura + 1, 2).value = modos.atributo

    logger.success(
        f"Plantilla {hoja.name} actualizada para {modos.apertura} - {modos.atributo}."
    )
    logger.info(f"Tiempo de actualizacion: {round(time.time() - s, 2)} segundos.")


def verificar_plantilla_generada(hoja: xw.Sheet) -> None:
    if "apertura" not in hoja.range("A1:A10000").value:
        raise PlantillaNoGeneradaError(
            f"""
            La plantilla {hoja.name} no esta generada, entonces no se puede actualizar.
            """
        )


def obtener_apertura_actual(wb: xw.Book, plantilla: str) -> str:
    hoja_plantilla = wb.sheets[plantilla.capitalize()]
    fila_apertura = obtener_indice_en_rango(
        "apertura", hoja_plantilla.range("A1:A10000")
    )
    return hoja_plantilla.cells(fila_apertura, 2).value


def verificar_misma_periodicidad(
    negocio: str, apertura_actual: str, apertura_nueva: str
) -> None:
    aperturas = utils.obtener_aperturas(negocio, "siniestros").filter(
        pl.col("apertura_reservas").is_in([apertura_actual, apertura_nueva])
    )
    if len(aperturas.get_column("periodicidad_ocurrencia").unique()) != 1:
        raise PeriodicidadDiferenteError(
            f"""
            Las aperturas {apertura_actual} y {apertura_nueva}
            no tienen la misma periodicidad de ocurrencia. Debe
            generar la plantilla de la nueva apertura.
            """
        )


class PlantillaNoGeneradaError(Exception):
    pass


class PeriodicidadDiferenteError(Exception):
    pass

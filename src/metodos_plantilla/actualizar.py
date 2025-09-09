import time

import polars as pl
import xlwings as xw

from src import constantes as ct
from src import utils
from src.logger_config import logger
from src.metodos_plantilla import generar, indexaciones
from src.models import ModosPlantilla, Parametros


def actualizar_plantillas(wb: xw.Book, p: Parametros, modos: ModosPlantilla):
    if modos.plantilla == "severidad":
        modos_frec = modos.model_copy(
            update={"plantilla": "frecuencia", "atributo": "bruto"}
        )
        actualizar_plantilla(wb, p, modos_frec)

        tipo_indexacion, _ = utils.obtener_parametros_indexacion(
            p.negocio, modos.apertura
        )
        if tipo_indexacion == "Por fecha de movimiento":
            indexaciones.traer_frecuencia_indexacion_movimiento(wb, p, modos_frec)

    actualizar_plantilla(wb, p, modos)


def actualizar_plantilla(wb: xw.Book, p: Parametros, modos: ModosPlantilla) -> None:
    s = time.time()

    hoja = wb.sheets[modos.plantilla.capitalize()]
    verificar_plantilla_generada(hoja)

    apertura_actual = obtener_apertura_actual(wb, modos.plantilla)
    verificar_misma_periodicidad(p.negocio, apertura_actual, modos.apertura)

    if modos.plantilla == "severidad":
        verificar_misma_indexacion(p.negocio, apertura_actual, modos.apertura)

    cantidades = (
        ["pago", "incurrido"]
        if hoja.name != "Frecuencia"
        else ["conteo_pago", "conteo_incurrido"]
    )

    aperturas = utils.obtener_aperturas(p.negocio, "siniestros")

    triangulo = generar.crear_triangulo_base_plantilla(
        pl.scan_parquet("data/processed/base_triangulos.parquet"),
        modos.apertura,
        modos.atributo,
        aperturas,
        cantidades,
    )

    hoja.cells(ct.FILA_INI_PLANTILLAS + 1, ct.COL_OCURRS_PLANTILLAS).value = triangulo
    hoja.cells(1, 1).value = modos.apertura
    hoja.cells(2, 1).value = modos.atributo

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
    return hoja_plantilla.cells(1, 1).value


def verificar_misma_periodicidad(
    negocio: str, apertura_actual: str, apertura_nueva: str
) -> None:
    aperturas = utils.obtener_aperturas(negocio, "siniestros").filter(
        pl.col("apertura_reservas").is_in([apertura_actual, apertura_nueva])
    )
    if len(aperturas.get_column("periodicidad_ocurrencia").unique()) != 1:
        raise PeriodicidadDiferenteError(
            utils.limpiar_espacios_log(
                f"""
                Las aperturas {apertura_actual} y {apertura_nueva}
                no tienen la misma periodicidad de ocurrencia. Debe
                generar la plantilla de la nueva apertura.
                """
            )
        )


def verificar_misma_indexacion(
    negocio: str, apertura_actual: str, apertura_nueva: str
) -> None:
    tipo_indexacion_actual, _ = utils.obtener_parametros_indexacion(
        negocio, apertura_actual
    )
    tipo_indexacion_nueva, _ = utils.obtener_parametros_indexacion(
        negocio, apertura_nueva
    )
    if tipo_indexacion_actual != tipo_indexacion_nueva:
        raise IndexacionDiferenteError(
            utils.limpiar_espacios_log(
                f"""
                Las aperturas {apertura_actual} y {apertura_nueva}
                no tienen el mismo tipo de indexacion. Debe
                generar la plantilla de la nueva apertura.
                """
            )
        )


class PlantillaNoGeneradaError(Exception):
    pass


class PeriodicidadDiferenteError(Exception):
    pass


class IndexacionDiferenteError(Exception):
    pass

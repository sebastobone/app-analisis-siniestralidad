import xlwings as xw

import src.constantes as ct
from src import utils
from src.metodos_plantilla.guardar_traer import traer_apertura
from src.models import ModosPlantilla, Parametros


def traer_frecuencia_indexacion_movimiento(
    wb: xw.Book, p: Parametros, modos: ModosPlantilla
) -> None:
    try:
        traer_apertura.traer_apertura(wb, p, modos)
    except FileNotFoundError as exc:
        mensaje_error = utils.limpiar_espacios_log(
            f"""
            No se han guardado resultados de frecuencia para la apertura
            {modos.apertura}. Para generar la plantilla de severidad
            con indexacion por fecha de movimiento, se necesitan resultados
            almacenados de frecuencia.
            """
        )
        raise FileNotFoundError(mensaje_error) from exc


def validar_medida_indexacion(
    hoja: xw.Sheet, tipo_indexacion: ct.TIPOS_INDEXACION, medida: str
) -> None:
    if tipo_indexacion != "Ninguna" and medida not in hoja.range("A1:AA1").value:
        raise ValueError(
            utils.limpiar_espacios_log(
                f"""
                La medida de indexacion "{medida}" no se encuentra en la hoja
                Indexaciones. Por favor, verifique que la medida este escrita
                correctamente y que exista en la hoja.
                """
            )
        )

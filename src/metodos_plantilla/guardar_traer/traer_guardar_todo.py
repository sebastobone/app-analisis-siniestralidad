import time

import xlwings as xw
from src import constantes as ct
from src.logger_config import logger
from src.metodos_plantilla import tablas_resumen
from src.metodos_plantilla.generar import generar_plantilla

from .guardar_apertura import guardar_apertura
from .traer_apertura import traer_apertura


def traer_y_guardar_todas_las_aperturas(
    wb: xw.Book,
    plantilla: ct.LISTA_PLANTILLAS,
    mes_corte: int,
    negocio: str,
    traer: bool = False,
) -> None:
    s = time.time()

    plantilla_name = f"Plantilla_{plantilla.capitalize()}"
    aperturas = tablas_resumen.obtener_tabla_aperturas(negocio).get_column(
        "apertura_reservas"
    )
    atributos = (
        ["Bruto", "Retenido"] if plantilla_name != "Plantilla_Frec" else ["Bruto"]
    )

    for apertura in aperturas.to_list():
        for atributo in atributos:
            wb.sheets[plantilla_name]["C2"].value = apertura
            wb.sheets[plantilla_name]["C3"].value = atributo
            generar_plantilla(wb, plantilla, mes_corte)
            if traer:
                traer_apertura(wb, plantilla, mes_corte)
            guardar_apertura(wb, plantilla, mes_corte)

    if traer:
        logger.success("Todas las aperturas se han traido y guardado correctamente.")
    else:
        logger.success("Todas las aperturas se han guardado correctamente.")

    wb.sheets["Main"]["A1"].value = "TRAER_GUARDAR_TODO" if traer else "GUARDAR_TODO"
    wb.sheets["Main"]["A2"].value = time.time() - s

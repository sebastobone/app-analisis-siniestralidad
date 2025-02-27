import asyncio
import time

import xlwings as xw
from src import utils
from src.logger_config import logger
from src.metodos_plantilla.generar import generar_plantilla
from src.models import ModosPlantilla

from .guardar_apertura import guardar_apertura
from .traer_apertura import traer_apertura


async def traer_y_guardar_todas_las_aperturas(
    wb: xw.Book,
    modos: ModosPlantilla,
    mes_corte: int,
    negocio: str,
    traer: bool = False,
) -> None:
    s = time.time()

    plantilla_name = f"Plantilla_{modos.plantilla.capitalize()}"
    aperturas = (
        utils.obtener_aperturas(negocio, "siniestros")
        .get_column("apertura_reservas")
        .to_list()
    )
    atributos = (
        ["Bruto", "Retenido"] if plantilla_name != "Plantilla_Frec" else ["Bruto"]
    )

    num_apertura = 0
    for apertura in aperturas:
        for atributo in atributos:
            wb.sheets[plantilla_name]["C2"].value = apertura
            wb.sheets[plantilla_name]["C3"].value = atributo
            generar_plantilla(wb, modos, mes_corte)
            if traer:
                traer_apertura(wb, modos, mes_corte)
            guardar_apertura(wb, modos, mes_corte)

            await asyncio.sleep(0)

            logger.info(
                utils.limpiar_espacios_log(
                    f"""
                    Apertura {num_apertura + 1} de {len(aperturas) * len(atributos)}
                    terminada.
                    """
                )
            )
            num_apertura += 1

    if traer:
        logger.success("Todas las aperturas se han traido y guardado correctamente.")
    else:
        logger.success("Todas las aperturas se han guardado correctamente.")

    wb.sheets["Main"]["A1"].value = "TRAER_GUARDAR_TODO" if traer else "GUARDAR_TODO"
    wb.sheets["Main"]["A2"].value = time.time() - s

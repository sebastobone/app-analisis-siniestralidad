import asyncio
import time

import xlwings as xw
from src import utils
from src.logger_config import logger
from src.metodos_plantilla import actualizar, generar
from src.models import ModosPlantilla, Parametros

from .guardar_apertura import guardar_apertura
from .traer_apertura import traer_apertura


async def traer_y_guardar_todas_las_aperturas(
    wb: xw.Book,
    p: Parametros,
    modos: ModosPlantilla,
    traer: bool = False,
) -> None:
    s = time.time()

    aperturas = (
        utils.obtener_aperturas(p.negocio, "siniestros")
        .get_column("apertura_reservas")
        .to_list()
    )
    atributos = ["bruto", "retenido"] if modos.plantilla != "frecuencia" else ["bruto"]

    num_apertura = 0
    for apertura in aperturas:
        for atributo in atributos:
            modos_actual = modos.model_copy()
            modos_actual.apertura = apertura
            modos_actual.atributo = atributo  # type: ignore

            try:
                actualizar.actualizar_plantillas(wb, p, modos_actual)
            except (
                actualizar.PlantillaNoGeneradaError,
                actualizar.PeriodicidadDiferenteError,
            ):
                generar.generar_plantillas(wb, p, modos_actual)
            if traer:
                traer_apertura(wb, modos_actual)
            guardar_apertura(wb, modos_actual)

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

    logger.info(f"Tiempo total: {round(time.time() - s, 2)} segundos.")

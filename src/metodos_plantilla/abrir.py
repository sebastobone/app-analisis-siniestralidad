import os
import shutil

import xlwings as xw

from src.logger_config import logger


def abrir_plantilla(plantilla_path: str) -> xw.Book:
    if not os.path.exists(plantilla_path):
        shutil.copyfile("plantillas/plantilla.xlsm", plantilla_path)
        logger.info(f"Nueva plantilla creada en {plantilla_path}.")

    wb = xw.Book(plantilla_path)

    wb.macro("eliminar_modulos")()
    wb.macro("crear_modulos")()

    return wb

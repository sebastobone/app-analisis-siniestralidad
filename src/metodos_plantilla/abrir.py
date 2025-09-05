import shutil
from pathlib import Path

import xlwings as xw

from src.logger_config import logger


def abrir_plantilla(plantilla_path: str) -> xw.Book:
    if not Path(plantilla_path).exists():
        shutil.copyfile("plantillas/plantilla.xlsm", plantilla_path)
        logger.info(f"Nueva plantilla creada en {plantilla_path}.")

    wb = xw.Book(plantilla_path)
    wb.activate(steal_focus=True)
    return wb

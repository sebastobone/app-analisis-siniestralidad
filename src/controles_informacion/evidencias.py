import shutil
from datetime import datetime

import mss
import xlwings as xw

from src import utils
from src.logger_config import logger
from src.models import Parametros


async def generar_evidencias_parametros(p: Parametros) -> None:
    mes_corte = utils.date_to_yyyymm(p.mes_corte)

    original_file = f"data/segmentacion_{p.negocio}.xlsx"
    stored_file = (
        f"data/controles_informacion/{mes_corte}_segmentacion_{p.negocio}.xlsx"
    )

    shutil.copyfile(original_file, stored_file)

    with xw.App(visible=False) as xl_app:
        wb = xl_app.books.open(stored_file)

        sheet_name = "CONTROL_EXTRACCION"
        sheet = wb.sheets.add(name=sheet_name)

        sheet["A1"].value = "Fecha y hora del fin de la extraccion"
        sheet["A2"].value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        wb.save(stored_file)
        wb.close()

    with mss.mss() as sct:
        sct.shot(output=f"data/controles_informacion/{mes_corte}_extraccion.png")

    logger.success("Evidencias de controles generadas exitosamente.")

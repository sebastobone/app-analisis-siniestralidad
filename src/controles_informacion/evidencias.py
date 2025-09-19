import shutil
from datetime import datetime

import mss
import openpyxl as xl

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

    wb = xl.load_workbook(stored_file)
    sheet_name = "CONTROL_EXTRACCION"
    wb.create_sheet(title=sheet_name)

    wb[sheet_name]["A1"] = "Fecha y hora del fin de la extraccion"
    wb[sheet_name]["A2"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    wb.save(stored_file)
    wb.close()

    with mss.mss() as sct:
        sct.shot(output=f"data/controles_informacion/{mes_corte}_extraccion.png")

    logger.success("Evidencias de controles generadas exitosamente.")

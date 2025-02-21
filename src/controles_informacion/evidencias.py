import asyncio
import os

from src.logger_config import logger


async def generar_evidencias_parametros(negocio: str, mes_corte: int):
    import shutil
    from datetime import datetime

    import openpyxl as xl
    import pyautogui

    original_file = f"data/segmentacion_{negocio}.xlsx"
    stored_file = f"data/controles_informacion/{mes_corte}_segmentacion_{negocio}.xlsx"

    shutil.copyfile(original_file, stored_file)

    wb = xl.load_workbook(stored_file)
    sheet_name = "CONTROL_EXTRACCION"
    wb.create_sheet(title=sheet_name)

    wb[sheet_name]["A1"] = "Fecha y hora del fin de la extraccion"
    wb[sheet_name]["A2"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    wb.save(stored_file)
    wb.close()

    # Reloj
    if os.name == "nt":
        pyautogui.hotkey("winleft", "alt", "d")
        await asyncio.sleep(0.5)

    pyautogui.screenshot(f"data/controles_informacion/{mes_corte}_extraccion.png")

    if os.name == "nt":
        pyautogui.hotkey("winleft", "alt", "d")

    logger.success("Evidencias de controles generadas exitosamente.")

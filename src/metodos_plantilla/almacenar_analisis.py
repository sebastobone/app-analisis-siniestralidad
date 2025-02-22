import time

import xlwings as xw

from src import utils
from src.logger_config import logger


def almacenar_analisis(wb: xw.Book, nombre_plantilla: str, mes_corte: int) -> None:
    s = time.time()

    df_resultados = utils.sheet_to_dataframe(wb, "Aux_Totales").with_columns(
        atipico=0, mes_corte=mes_corte
    )
    df_resultados_atipicos = utils.sheet_to_dataframe(wb, "Atipicos").with_columns(
        atipico=1, mes_corte=mes_corte
    )
    if df_resultados_atipicos.shape[0] != 0:
        df_resultados = df_resultados.vstack(df_resultados_atipicos)

    ruta = f"output/resultados/{nombre_plantilla}_{mes_corte}.parquet"
    df_resultados.write_parquet(ruta)

    logger.success(f"Analisis almacenado en {ruta}.")

    wb.sheets["Main"]["A1"].value = "ALMACENAR_ANALISIS"
    wb.sheets["Main"]["A2"].value = time.time() - s

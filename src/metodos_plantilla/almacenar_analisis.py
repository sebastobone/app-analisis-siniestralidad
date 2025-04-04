import time

import polars as pl
import xlwings as xw

from src import utils
from src.logger_config import logger


def almacenar_analisis(
    wb: xw.Book, nombre_plantilla: str, mes_corte: int, tipo_analisis: str
) -> None:
    s = time.time()

    df_resultados = utils.sheet_to_dataframe(wb, "Resumen").with_columns(
        atipico=0, mes_corte=mes_corte, tipo_analisis=pl.lit(tipo_analisis)
    )
    df_resultados_atipicos = utils.sheet_to_dataframe(wb, "Atipicos").with_columns(
        atipico=1, mes_corte=mes_corte, tipo_analisis=pl.lit(tipo_analisis)
    )
    if df_resultados_atipicos.shape[0] != 0:
        df_resultados = df_resultados.vstack(df_resultados_atipicos)

    ruta = f"output/resultados/{nombre_plantilla}_{mes_corte}_{tipo_analisis}.parquet"
    df_resultados.write_parquet(ruta)

    logger.success(f"Analisis almacenado en {ruta}.")
    logger.info(f"Tiempo para almacenamiento: {round(time.time() - s, 2)} segundos.")

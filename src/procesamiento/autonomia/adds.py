import os

import polars as pl
import xlwings as xw

from src import utils
from src.controles_informacion.sap import consolidar_sap


async def cantidades_sap(hojas_afo: list[str], mes_corte: int) -> pl.DataFrame:
    return (
        await consolidar_sap(
            ["Generales", "Vida"],
            hojas_afo,
            mes_corte,
        )
    ).filter(
        (pl.col("fecha_registro") == utils.yyyymm_to_date(mes_corte))
        & (
            pl.col("codigo_ramo_op").is_in(
                ["025", "069", "081", "083", "084", "086", "095", "096", "181"]
            )
        )
    )


def crear_hoja_segmentacion(df: pl.DataFrame, nombre_hoja: str) -> None:
    xw.Book(f"{os.getcwd()}/data/segmentacion_autonomia.xlsx").set_mock_caller()
    wb = xw.Book.caller()

    try:
        wb.sheets.add(name=nombre_hoja)
    except ValueError:
        wb.sheets[nombre_hoja].delete()
        wb.sheets.add(name=nombre_hoja)

    wb.sheets[nombre_hoja].range("A1:A500").number_format = "@"
    wb.sheets[nombre_hoja].range("B1:B500").number_format = "@"
    wb.sheets[nombre_hoja]["A1"].options(index=False).value = df.to_pandas()


async def sap_sinis_ced(mes_corte: int) -> None:
    df_sinis = await cantidades_sap(["pago_cedido", "aviso_cedido"], mes_corte)
    crear_hoja_segmentacion(df_sinis, "SAP_Sinis_Ced")


async def sap_primas_ced(mes_corte: int) -> None:
    df_primas = (
        await cantidades_sap(
            [
                "prima_bruta",
                "prima_retenida",
                "prima_bruta_devengada",
                "prima_retenida_devengada",
                "rpnd_bruto",
                "rpnd_cedido",
            ],
            mes_corte,
        )
    ).with_columns(
        rpnd_retenido=pl.col("rpnd_bruto") - pl.col("rpnd_cedido"),
        prima_cedida=pl.col("prima_bruta") - pl.col("prima_retenida"),
        fecha_registro=pl.col("fecha_registro").dt.strftime("%Y%m").cast(pl.Int32),
    )
    crear_hoja_segmentacion(df_primas, "add_p_SAP_Primas_Ced")

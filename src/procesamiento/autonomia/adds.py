import polars as pl
from controles_informacion.controles_informacion import read_sap
import constantes as ct
import xlwings as xw
import pandas as pd
import os

df = (
    read_sap(
        ["Generales", "Vida"],
        ["pago_cedido", "aviso_cedido"],
        int(ct.PARAMS_FECHAS[1][1]),
    )
    .filter(pl.col("mes_mov") == int(ct.PARAMS_FECHAS[1][1]))
    .collect()
)

xw.Book(f"{os.getcwd()}/data/segmentacion_autonomia.xlsx").set_mock_caller()
wb = xw.Book.caller()

try:
    wb.sheets.add(name="SAP_Sinis_Ced", after="Fechas")
except ValueError:
    wb.sheets["SAP_Sinis_Ced"].delete()
    wb.sheets.add(name="SAP_Sinis_Ced", after="Fechas")

wb.sheets["SAP_Sinis_Ced"].range("A1:A500").number_format = "@"
wb.sheets["SAP_Sinis_Ced"].range("B1:B500").number_format = "@"
wb.sheets["SAP_Sinis_Ced"]["A1"].options(index=False).value = df.to_pandas()

# sap_primas = ctrl.read_sap(['Generales', 'Vida'], ['PRIMAS_EMI', 'PRIMAS_DEV', 'RPND'], ['BRUTO', 'RETENIDO', 'CEDIDO'], int(mes_corte))
# sap_primas = sap_primas[sap_primas['Mes_Mov'] == int(mes_corte)]
# sap_primas.to_csv('sap_primas_cedidas_rpnd.txt', sep='\t', index=False)

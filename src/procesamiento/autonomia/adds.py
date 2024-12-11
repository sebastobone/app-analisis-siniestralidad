import polars as pl
from controles_informacion.controles_informacion import read_sap
import constantes as ct
import xlsxwriter

df = read_sap(
    ["Generales", "Vida"], ["pago_cedido", "aviso_cedido"], int(ct.PARAMS_FECHAS[1][1])
).filter(pl.col("mes_mov") == int(ct.PARAMS_FECHAS[1][1])).collect()

with xlsxwriter.Workbook("data/segmentacion_autonomia.xlsx") as wb:
    df.write_excel(workbook=wb, worksheet="add_s_SAP_Sinis_Ced")

# xw.Book(f"{os.getcwd()}/data/segmentacion_autonomia.xlsx").set_mock_caller()
# wb = xw.Book.caller()
# wb.sheets.add(name="add_s_SAP_Sinis_Ced")
# wb.sheets["add_s_SAP_Sinis_Ced"]["A1"].options(index=False).value = df

# sap_primas = ctrl.read_sap(['Generales', 'Vida'], ['PRIMAS_EMI', 'PRIMAS_DEV', 'RPND'], ['BRUTO', 'RETENIDO', 'CEDIDO'], int(mes_corte))
# sap_primas = sap_primas[sap_primas['Mes_Mov'] == int(mes_corte)]
# sap_primas.to_csv('sap_primas_cedidas_rpnd.txt', sep='\t', index=False)

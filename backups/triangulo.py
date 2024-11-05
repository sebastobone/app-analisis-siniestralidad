# mypy: ignore-errors

import time
import xlwings as xw

wb = xw.Book.caller()

s = time.time()

xw.apps.active.api.Calculation = xw.constants.Calculation.xlCalculationManual

ws = wb.sheets["Plantilla_Frec"]

num_ocurrencias = 21
num_alturas = 21
sep_triangulos = 5
col_aperturas_auxtot = 1
col_ocurrs_auxtot = 6
col_expuestos_auxtot = 14
nombre = "Frecuencia"

celda_row = 33
celda_col = 6

ws.range("F7:AV30").copy()
cell = ws.range((celda_row, celda_col))
cell.paste(paste="values")
cell.paste(paste="formats")
cell.value = nombre

rng = ws.range(
    ws.cells(celda_row + 3, celda_col + 1),
    ws.cells(celda_row + 3 + num_ocurrencias - 1, celda_col + num_alturas * 2),
)

rng.formula = f'=IF(R[{-num_ocurrencias - sep_triangulos}]C = "", "", IFERROR(R[{-num_ocurrencias - sep_triangulos}]C / SUMIFS(Aux_Totales!C{col_expuestos_auxtot}, Aux_Totales!C{col_ocurrs_auxtot}, RC6, Aux_Totales!C{col_aperturas_auxtot}, Aperturas!R2C1), 0))'
rng.number_format = "0.0000%"

# Check for the "Ratios" condition
if nombre == "Ratios" and "Entremes" not in ws.name:
    # Apply formatting to the range
    rng_header = ws.range(
        ws.cells(celda_row, celda_col - 1), ws.cells(celda_row + 2, celda_col - 1)
    )
    rng_header.font.bold = True
    rng_header.color = (
        255,
        235,
        205,
    )  # Equivalent of xlThemeColorAccent1 with tint

    ws.cells(celda_row + 2, celda_col - 1).value = "Periodo"

    ws.range(
        ws.cells(celda_row + 3, celda_col - 1),
        ws.cells(celda_row + 2 + num_ocurrencias, celda_col - 1),
    ).formula = f"=COUNTA(R{celda_row + 2 + num_ocurrencias}C[1]:RC[1]) - 1"

    # Green color scale
    rng_color = ws.range(
        ws.cells(celda_row + 3, celda_col + 1),
        ws.cells(celda_row + 3, celda_col + 1).end("down"),
    )
    color_scale = rng_color.api.FormatConditions.AddColorScale(ColorScaleType=2)
    color_scale.SetFirstPriority()

    color_scale.ColorScaleCriteria(1).Type = xw.constants.ConditionValueLowestValue
    color_scale.ColorScaleCriteria(1).FormatColor.Color = 16776444  # Light green
    color_scale.ColorScaleCriteria(2).Type = xw.constants.ConditionValueHighestValue
    color_scale.ColorScaleCriteria(2).FormatColor.Color = 8109667  # Dark green

    # Red text for excluded factors
    ws.api.ReferenceStyle = xw.constants.ReferenceStyleR1C1
    formula_expr = f"=R[{num_ocurrencias + sep_triangulos}]C=0"
    rng_color.api.FormatConditions.Add(
        Type=xw.constants.ConditionExpression, Formula1=formula_expr
    )
    ws.api.ReferenceStyle = xw.constants.ReferenceStyleA1

    rng_color.api.FormatConditions(2).SetFirstPriority()
    rng_color.api.FormatConditions(1).Font.Color = -16776961  # Red text
    rng_color.api.FormatConditions(1).StopIfTrue = False

    # Copy formatting to other columns
    rng_color.api.Copy()
    for i in range(2, num_alturas * 2):
        ws.cells(celda_row + 3, celda_col + i).api.PasteSpecial(
            Paste=xw.constants.PasteFormats
        )

    # Check for the "EXCLUSIONES" condition
    if nombre == "EXCLUSIONES":
        rng_exclusion = ws.range(
            ws.cells(celda_row + 3, celda_col + 1),
            ws.cells(celda_row + 2 + num_ocurrencias, celda_col + 1).end("right"),
        )

        # Apply conditional formatting for cell value equals 0
        exclusion_format = rng_exclusion.api.FormatConditions.Add(
            Type=xw.constants.ConditionCellValue,
            Operator=xw.constants.ConditionOperatorEqual,
            Formula1="=0",
        )
        exclusion_format.SetFirstPriority()
        exclusion_format.Font.Color = -16383844  # Dark blue text
        exclusion_format.Interior.Color = 13551615  # Light red fill
        exclusion_format.StopIfTrue = False

xw.apps.active.api.Calculation = xw.constants.Calculation.xlCalculationAutomatic
wb.sheets["Modos"]["A2"].value = time.time() - s

Function color_format(Rng)
    With Rng
        .Font.Bold = True
        .Interior.ThemeColor = xlThemeColorAccent1
        .Interior.TintAndShade = 0.799981688894314
        .HorizontalAlignment = xlCenter
        .VerticalAlignment = xlBottom
    End With
End Function



Function color_columnas_triangulo(ws, num_alturas)

    With ws.Range(ws.Cells(7, 6), ws.Cells(8, 6)).Interior
        .ThemeColor = xlThemeColorAccent1
        .TintAndShade = 0.799981688894314
    End With

    With ws.Range(ws.Cells(7, 7), ws.Cells(8, 7 + num_alturas - 1)).Interior
        .ThemeColor = xlThemeColorAccent3
        .TintAndShade = 0.599999
    End With
    
    With ws.Range(ws.Cells(7, 7 + num_alturas), ws.Cells(8, 7 + num_alturas * 2 - 1)).Interior
        .ThemeColor = xlThemeColorAccent2
        .TintAndShade = 0.599999
    End With

End Function



Function last_row(ws, col_ocurrs_plantillas) As Long
    last_row = ws.Cells(Rows.Count, col_ocurrs_plantillas).End(xlUp).Row
End Function



Function triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, nombre, formula, format)
    
    fila_triangulo = last_row(ws, col_ocurrs_plantillas) + sep_triangulos + 1
    
    ws.Range(ws.Cells(fila_ini_plantillas, col_ocurrs_plantillas), _
             ws.Cells(fila_ini_plantillas + header_triangulos + num_ocurrencias - 1, col_ocurrs_plantillas + num_alturas * 2)).Copy
    
    With ws.Cells(fila_triangulo, col_ocurrs_plantillas)
        .PasteSpecial Paste:=xlPasteValues
        .PasteSpecial Paste:=xlPasteFormats
        .value = nombre
    End With
    
    Set rango_valores = ws.Range(ws.Cells(fila_triangulo + header_triangulos, col_ocurrs_plantillas + 1), _
                                 ws.Cells(fila_triangulo + header_triangulos + num_ocurrencias - 1, col_ocurrs_plantillas + num_alturas * 2))
    
    rango_valores.FormulaR1C1 = formula
    rango_valores.NumberFormat = format
    
    
    If nombre = "Ratios" And ws.Name <> "Plantilla_Entremes" Then
        
        'Mapa de color verde
        Set Rng = ws.Range(ws.Cells(fila_triangulo + header_triangulos, col_ocurrs_plantillas + 1), ws.Cells(fila_triangulo + header_triangulos + num_ocurrencias - 1, col_ocurrs_plantillas + 1))
        Rng.FormatConditions.AddColorScale ColorScaleType:=2
        Rng.FormatConditions(1).SetFirstPriority
        
        With Rng.FormatConditions(1).ColorScaleCriteria(1)
            .Type = xlConditionValueLowestValue
            .FormatColor.Color = 16776444
        End With
        
        With Rng.FormatConditions(1).ColorScaleCriteria(2)
            .Type = xlConditionValueHighestValue
            .FormatColor.Color = 8109667
        End With
        
        'Texto rojo para factores excluidos
        Application.ScreenUpdating = True
        Application.ReferenceStyle = xlR1C1
        Rng.FormatConditions.Add Type:=xlExpression, Formula1:="=R[" & num_ocurrencias + header_triangulos + sep_triangulos & "]C = 0"
        Application.ReferenceStyle = xlA1
        Application.ScreenUpdating = False
        Rng.FormatConditions(2).SetFirstPriority
        
        With Rng.FormatConditions(1).Font
            .Color = -16776961
        End With
        Rng.FormatConditions(1).StopIfTrue = False
        
        Rng.Copy
        For altura = 2 To num_alturas * 2
            ws.Cells(fila_triangulo + header_triangulos, col_ocurrs_plantillas + altura).PasteSpecial Paste:=xlPasteFormats
        Next altura

    ElseIf nombre = "Exclusiones" Then
    
        Set Rng = ws.Range(ws.Cells(fila_triangulo + header_triangulos, col_ocurrs_plantillas + 1), _
                           ws.Cells(fila_triangulo + header_triangulos + num_ocurrencias - 1, col_ocurrs_plantillas + num_alturas * 2))
        
        Rng.FormatConditions.Add Type:=xlCellValue, Operator:=xlEqual, Formula1:="=0"
        Rng.FormatConditions(1).SetFirstPriority
        With Rng.FormatConditions(1).Font
            .Color = -16383844
        End With
        With Rng.FormatConditions(1).Interior
            .PatternColorIndex = xlAutomatic
            .Color = 13551615
        End With
        Rng.FormatConditions(1).StopIfTrue = False
        
    End If
    
End Function



Function factores_desarrollo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, fila_ind_altura)
    
    fila_factores = last_row(ws, col_ocurrs_plantillas) + sep_triangulos + 1

    ws.Cells(fila_factores, col_ocurrs_plantillas - 4).value = "Periodo inicial"
    ws.Cells(fila_factores, col_ocurrs_plantillas - 3).value = "Periodo final"
    ws.Cells(fila_factores, col_ocurrs_plantillas - 2).value = "Percentil"
    
    Set Rng = ws.Range(ws.Cells(fila_factores, col_ocurrs_plantillas - 4), ws.Cells(fila_factores, col_ocurrs_plantillas - 2))
    Call color_format(Rng)
    
    With ws.Cells(fila_factores, col_ocurrs_plantillas + 1)
        .value = "Pago"
        .Font.Bold = True
    End With
    
    With ws.Range(ws.Cells(fila_factores, col_ocurrs_plantillas + 1), ws.Cells(fila_factores, col_ocurrs_plantillas + num_alturas))
        .Interior.ThemeColor = xlThemeColorAccent3
        .Interior.TintAndShade = 0.599999
    End With
    
    With ws.Cells(fila_factores, col_ocurrs_plantillas + num_alturas + 1)
        .value = "Incurrido"
        .Font.Bold = True
    End With
    
    With ws.Range(ws.Cells(fila_factores, col_ocurrs_plantillas + num_alturas + 1), ws.Cells(fila_factores, col_ocurrs_plantillas + num_alturas * 2))
        .Interior.ThemeColor = xlThemeColorAccent2
        .Interior.TintAndShade = 0.599999
    End With
        
    fila_ratios = WorksheetFunction.Match("Ratios", ws.Range("F:F"), 0) + header_triangulos
    fila_exclusiones = WorksheetFunction.Match("Exclusiones", ws.Range("F:F"), 0) + header_triangulos
    fila_val = fila_valores(ws, fila_ini_plantillas, sep_triangulos)

    Dim nombres(1 To 17) As Variant
    nombres(1) = "PROMEDIO"
    nombres(2) = "MEDIANA"
    nombres(3) = "PROMEDIO PONDERADO"
    nombres(4) = "MINIMO"
    nombres(5) = "MAXIMO"
    nombres(6) = "PERCENTIL 1"
    nombres(7) = "PERCENTIL 2"
    nombres(8) = "PROMEDIO VENTANA"
    nombres(9) = "MEDIANA VENTANA"
    nombres(10) = "PROMEDIO PONDERADO VENTANA"
    nombres(11) = "MINIMO VENTANA"
    nombres(12) = "MAXIMO VENTANA"
    nombres(13) = "PERCENTIL 1 VENTANA"
    nombres(14) = "PERCENTIL 2 VENTANA"
    nombres(15) = "FACTORES SELECCIONADOS"
    nombres(16) = "FACTORES ACUMULADOS"
    nombres(17) = "DESARROLLO"
    
    Dim per_ini(1 To 17) As Variant
    per_ini(1) = ""
    per_ini(2) = ""
    per_ini(3) = ""
    per_ini(4) = ""
    per_ini(5) = ""
    per_ini(6) = ""
    per_ini(7) = ""
    per_ini(8) = 1
    per_ini(9) = 1
    per_ini(10) = 1
    per_ini(11) = 1
    per_ini(12) = 1
    per_ini(13) = 1
    per_ini(14) = 1
    per_ini(15) = ""
    per_ini(16) = ""
    per_ini(17) = ""
    
    Dim per_fin(1 To 17) As Variant
    per_fin(1) = ""
    per_fin(2) = ""
    per_fin(3) = ""
    per_fin(4) = ""
    per_fin(5) = ""
    per_fin(6) = ""
    per_fin(7) = ""
    per_fin(8) = 4
    per_fin(9) = 4
    per_fin(10) = 4
    per_fin(11) = 4
    per_fin(12) = 4
    per_fin(13) = 4
    per_fin(14) = 4
    per_fin(15) = ""
    per_fin(16) = ""
    per_fin(17) = ""
    
    Dim percentil(1 To 17) As Variant
    percentil(1) = ""
    percentil(2) = ""
    percentil(3) = ""
    percentil(4) = ""
    percentil(5) = ""
    percentil(6) = 0.3
    percentil(7) = 0.7
    percentil(8) = ""
    percentil(9) = ""
    percentil(10) = ""
    percentil(11) = ""
    percentil(12) = ""
    percentil(13) = 0.3
    percentil(14) = 0.7
    percentil(15) = ""
    percentil(16) = ""
    percentil(17) = ""
    
    
    corr_ultima_altura = "=+IF(R" & fila_ini_plantillas + header_triangulos - 1 & "C = " & num_alturas & ", 1, "
    array_ratios = " R" & fila_ratios & "C : R" & fila_ratios + num_ocurrencias - 1 & "C "
    array_exclus = " R" & fila_exclusiones & "C : R" & fila_exclusiones + num_ocurrencias - 1 & "C "
    array_valores_C1 = " R" & fila_val & "C[1] : R" & fila_val + num_ocurrencias - 1 & "C[1] "
    array_valores_C0 = " R" & fila_val & "C    : R" & fila_val + num_ocurrencias - 1 & "C    "
    num_indice = " COUNT(" & array_valores_C1 & ") "
    
    'Vector con factores filtrados por exclusiones
    base_full = "FILTER(" + array_ratios + ", " + array_exclus + " > 0)"
    
    'Vector con factores filtrados por exclusiones y ventana temporal
    base_ventana = "FILTER(" & _
                          "INDEX(" + array_ratios + ", MAX(" + num_indice + " - RC2 + 1, 1)) : INDEX(" + array_ratios + ", MAX(" + num_indice + " - RC3 + 1, 1)), " & _
                          "INDEX(" + array_exclus + ", MAX(" + num_indice + " - RC2 + 1, 1)) : INDEX(" + array_exclus + ", MAX(" + num_indice + " - RC3 + 1, 1)) > 0)"
    
    Dim formulas(1 To 17) As Variant
    formulas(1) = corr_ultima_altura + "AVERAGE(" + base_full + "))"
    formulas(2) = corr_ultima_altura + "MEDIAN(" + base_full + "))"
    
    'En el promedio ponderado solamente varia la columna entre numerador y denominador
    ff1 = "SUM(FILTER(" & _
                    "INDEX(" + array_valores_C1 + ", MAX( " + num_indice + ", 1)) : R" & fila_val & "C[1], " & _
                    "INDEX(" + array_exclus + ", MAX( " + num_indice + ", 1)) : R" & fila_exclusiones & "C > 0)) / "
    ff2 = "SUM(FILTER(" & _
                    "INDEX(" + array_valores_C0 + ", MAX( " + num_indice + ", 1)) : R" & fila_val & "C, " & _
                    "INDEX(" + array_exclus + ", MAX( " + num_indice + ", 1)) : R" & fila_exclusiones & "C > 0)) ) "
    formulas(3) = corr_ultima_altura + ff1 + ff2
    
    formulas(4) = corr_ultima_altura + "MIN(" + base_full + "))"
    formulas(5) = corr_ultima_altura + "MAX(" + base_full + "))"
    formulas(6) = corr_ultima_altura + "PERCENTILE.INC(" + base_full + ", RC4))"
    formulas(7) = corr_ultima_altura + "PERCENTILE.INC(" + base_full + ", RC4))"
    
    formulas(8) = corr_ultima_altura + "AVERAGE(" + base_ventana + "))"
    formulas(9) = corr_ultima_altura + "MEDIAN(" + base_ventana + "))"
    
    'En el promedio ponderado solamente varia la columna entre numerador y denominador
    fv1 = "SUM(FILTER(" & _
                "INDEX(" + array_valores_C1 + ", MAX(" + num_indice + " - RC2 + 1, 1)) : INDEX(" + array_valores_C1 + ", MAX(" + num_indice + " - RC3 + 1, 1)), " & _
                "INDEX(" + array_exclus + ", MAX(" + num_indice + " - RC2 + 1, 1)) : INDEX(" + array_exclus + ", MAX(" + num_indice + " - RC3 + 1, 1)) > 0)) / "
    fv2 = "SUM(FILTER(" & _
                "INDEX(" + array_valores_C0 + ", MAX(" + num_indice + " - RC2 + 1, 1)) : INDEX(" + array_valores_C0 + ", MAX(" + num_indice + " - RC3 + 1, 1)), " & _
                "INDEX(" + array_exclus + ", MAX(" + num_indice + " - RC2 + 1, 1)) : INDEX(" + array_exclus + ", MAX(" + num_indice + " - RC3 + 1, 1)) > 0)) ) "
    formulas(10) = corr_ultima_altura + "" + fv1 + fv2
    
    formulas(11) = corr_ultima_altura + "MIN(" + base_ventana + "))"
    formulas(12) = corr_ultima_altura + "MAX(" + base_ventana + "))"
    formulas(13) = corr_ultima_altura + "PERCENTILE.INC(" + base_ventana + ", RC4))"
    formulas(14) = corr_ultima_altura + "PERCENTILE.INC(" + base_ventana + ", RC4))"
    formulas(15) = "=+IFERROR(R[-5]C, 1)"
    formulas(16) = corr_ultima_altura + "R[-1]C * RC[1])"
    formulas(17) = corr_ultima_altura + "1 / R[-1]C)"
    
    ws.Range(ws.Cells(fila_factores + 1, col_ocurrs_plantillas - 4), ws.Cells(fila_factores + UBound(nombres), col_ocurrs_plantillas - 4)) = Application.Transpose(per_ini)
    ws.Range(ws.Cells(fila_factores + 1, col_ocurrs_plantillas - 3), ws.Cells(fila_factores + UBound(nombres), col_ocurrs_plantillas - 3)) = Application.Transpose(per_fin)
    ws.Range(ws.Cells(fila_factores + 1, col_ocurrs_plantillas - 2), ws.Cells(fila_factores + UBound(nombres), col_ocurrs_plantillas - 2)) = Application.Transpose(percentil)
    ws.Range(ws.Cells(fila_factores + 1, col_ocurrs_plantillas), ws.Cells(fila_factores + UBound(nombres), col_ocurrs_plantillas)) = Application.Transpose(nombres)
    ws.Range(ws.Cells(fila_factores + 1, col_ocurrs_plantillas + 1), ws.Cells(fila_factores + UBound(nombres), col_ocurrs_plantillas + 1)) = Application.Transpose(formulas)
    
    ws.Range(ws.Cells(fila_factores + 1, col_ocurrs_plantillas + 1), ws.Cells(fila_factores + UBound(nombres) - 1, col_ocurrs_plantillas + 1)).NumberFormat = "#,##0.0000"
    ws.Cells(fila_factores + UBound(nombres), col_ocurrs_plantillas + 1).NumberFormat = "0.00%"
    ws.Cells(fila_factores + UBound(nombres) - 2, col_ocurrs_plantillas + 1).Font.Bold = True
        
    ws.Range(ws.Cells(fila_factores + 1, col_ocurrs_plantillas + 1), ws.Cells(fila_factores + UBound(formulas), col_ocurrs_plantillas + 1)).Copy
    ws.Range(ws.Cells(fila_factores + 1, col_ocurrs_plantillas + 1), ws.Cells(fila_factores + UBound(formulas), col_ocurrs_plantillas + num_alturas * 2)).PasteSpecial Paste:=xlPasteAll
    
    With ws.Range(ws.Cells(fila_factores + 1, col_ocurrs_plantillas), ws.Cells(fila_factores + UBound(nombres), col_ocurrs_plantillas)).Interior
        .ThemeColor = xlThemeColorAccent1
        .TintAndShade = 0.799981688894314
    End With
    
End Function



Sub limpiar_plantilla(ws_name)
    Set ws = Worksheets(ws_name)
    For Each co In ws.ChartObjects
        co.Delete
    Next co
    ws.Cells.FormatConditions.Delete
    ws.Range(ws.Cells(7, 1), ws.Cells(1000, 1000)).Delete Shift:=xlUp
End Sub



Function estructura_factores(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas)

    desref_filas = desref_filas_triangulo(num_ocurrencias, header_triangulos, sep_triangulos)
    fila_ind_altura = fila_ini_plantillas + header_triangulos - 1

    Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Ratios", "=IF(OR(R[" & desref_filas & "]C[1] = """", R" & fila_ind_altura & "C[1] < R" & fila_ind_altura & "C), """", IFERROR(R[" & desref_filas & "]C[1]/R[" & desref_filas & "]C, """"))", "#,##0.0000")
    Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Exclusiones", "=IF(OR(R[" & desref_filas * 2 & "]C[1] = """", R" & fila_ind_altura & "C[1] < R" & fila_ind_altura & "C), """", 1)", "#,##0")
    Call factores_desarrollo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, fila_ind_altura)
    
    If Not InStr(ws.Name, "Entremes") <> 0 Then
        fila_fact_sel = WorksheetFunction.Match("FACTORES SELECCIONADOS", ws.Range("F:F"), 0)
        Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Base", "=IF(R[" & (desref_filas - sep_triangulos) * 2 - 16 & "]C = """", R" & fila_fact_sel & "C,R[" & (desref_filas - sep_triangulos) * 2 - 16 & "]C)", "#,##0.0000")
    End If

End Function



Sub excluir_factor()

    Dim celda_actual As Range
    Dim celda_modificar As Range
    
    Set celda_actual = ActiveCell
    Set celda_modificar = celda_actual.Offset(num_ocurrencias + sep_triangulos, 0)
    celda_modificar.value = 0

End Sub



Sub incluir_factor()

    Dim celda_actual As Range
    Dim celda_modificar As Range
    
    Set celda_actual = ActiveCell
    Set celda_modificar = celda_actual.Offset(num_ocurrencias + 5, 0)
    celda_modificar.value = 1

End Sub



Sub formulas_aux_totales(num_filas_auxtot)
    ws_auxtot.Range(ws_auxtot.Cells(2, col_auxtot("conteo_ultimate")), ws_auxtot.Cells(num_filas_auxtot + 1, col_auxtot("conteo_ultimate"))).Formula2R1C1 = "=RC" & col_auxtot("frec_ultimate") & " * RC" & col_auxtot("expuestos") & ""
    ws_auxtot.Range(ws_auxtot.Cells(2, col_auxtot("ibnr_bruto")), ws_auxtot.Cells(num_filas_auxtot + 1, col_auxtot("ibnr_bruto"))).Formula2R1C1 = "=RC" & col_auxtot("plata_ultimate_bruto") & " - RC" & col_auxtot("incurrido_bruto") & ""
    ws_auxtot.Range(ws_auxtot.Cells(2, col_auxtot("ibnr_retenido")), ws_auxtot.Cells(num_filas_auxtot + 1, col_auxtot("ibnr_retenido"))).Formula2R1C1 = "=RC" & col_auxtot("plata_ultimate_retenido") & " - RC" & col_auxtot("incurrido_retenido") & ""
    ws_auxtot.Range(ws_auxtot.Cells(2, col_auxtot("ibnr_contable_bruto")), ws_auxtot.Cells(num_filas_auxtot + 1, col_auxtot("ibnr_contable_bruto"))).Formula2R1C1 = "=RC" & col_auxtot("plata_ultimate_contable_bruto") & " - RC" & col_auxtot("incurrido_bruto") & ""
    ws_auxtot.Range(ws_auxtot.Cells(2, col_auxtot("ibnr_contable_retenido")), ws_auxtot.Cells(num_filas_auxtot + 1, col_auxtot("ibnr_contable_retenido"))).Formula2R1C1 = "=RC" & col_auxtot("plata_ultimate_contable_retenido") & " - RC" & col_auxtot("incurrido_retenido") & ""
End Sub

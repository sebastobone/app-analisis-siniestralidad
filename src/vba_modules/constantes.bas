' HOJAS

Function ws_main() As Worksheet
    Set ws_main = Worksheets("Main")
End Function

Function ws_frec() As Worksheet
    Set ws_frec = Worksheets("Plantilla_Frec")
End Function

Function ws_seve() As Worksheet
    Set ws_seve = Worksheets("Plantilla_Seve")
End Function

Function ws_plat() As Worksheet
    Set ws_plat = Worksheets("Plantilla_Plata")
End Function

Function ws_entremes() As Worksheet
    Set ws_entremes = Worksheets("Plantilla_Entremes")
End Function

Function ws_auxtot() As Worksheet
    Set ws_auxtot = Worksheets("Aux_Totales")
End Function

Function ws_auxant() As Worksheet
    Set ws_auxant = Worksheets("Aux_Anterior")
End Function

Function ws_atipicos() As Worksheet
    Set ws_atipicos = Worksheets("Atipicos")
End Function



' APERTURAS

Function atributo_fem(atributo) As String
    atributo_fem = WorksheetFunction.Substitute(WorksheetFunction.Substitute(atributo, "bruto", "bruta"), "retenido", "retenida")
End Function



' FILAS

Function fila_valores(ws, fila_ini_plantillas, header_triangulos) As Integer
    If ws.Name = "Plantilla_Seve" Then
        fila_valores = WorksheetFunction.Match("Severidad", ws_seve.Range("F:F"), 0) + header_triangulos
    ElseIf ws.Name = "Plantilla_Frec" Then
        fila_valores = WorksheetFunction.Match("Frecuencia", ws_frec.Range("F:F"), 0) + header_triangulos
    Else
        fila_valores = fila_ini_plantillas + header_triangulos
    End If
End Function

Function desref_filas_triangulo(num_ocurrencias, header_triangulos, sep_triangulos) As Integer
    desref_filas_triangulo = - (num_ocurrencias + header_triangulos + sep_triangulos)
End Function



' COLUMNAS

Function col_auxtot(lookup_string) As Integer
    col_auxtot = WorksheetFunction.Match(lookup_string, ws_auxtot.Range("1:1"), 0)
End Function

Function col_auxant(lookup_string) As Integer
    col_auxant = WorksheetFunction.Match(lookup_string, ws_auxant.Range("1:1"), 0)
End Function

Function col_entremes(lookup_string) As Integer
    col_entremes = WorksheetFunction.Match(lookup_string, ws_entremes.Range("1:1"), 0)
End Function



' NUMEROS

Function num_filas_auxtot() As Integer
    num_filas_auxtot = ws_auxtot.Range(ws_auxtot.Cells(2, 1), ws_auxtot.Cells(2, 1).End(xlDown)).Rows.Count
End Function

Function mes_actual() As Double
    mes_actual = ws_main.Cells(4, 2).value
End Function

Function mes_anterior() As Double
    mes_anterior = ws_main.Cells(5, 2).value
End Function

Function num_meses_periodo_fn(num_ocurrencias, num_alturas) As Integer
    num_meses_periodo_fn = WorksheetFunction.RoundUp(num_alturas / num_ocurrencias, 0)
End Function

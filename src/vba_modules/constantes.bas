' HOJAS

Function ws_frecuencia() As Worksheet
    Set ws_frecuencia = Worksheets("Frecuencia")
End Function

Function ws_severidad() As Worksheet
    Set ws_severidad = Worksheets("Severidad")
End Function

Function ws_plata() As Worksheet
    Set ws_plata = Worksheets("Plata")
End Function

Function ws_entremes() As Worksheet
    Set ws_entremes = Worksheets("Entremes")
End Function

Function ws_completar_diagonal() As Worksheet
    Set ws_completar_diagonal = Worksheets("Completar_diagonal")
End Function

Function ws_resumen() As Worksheet
    Set ws_resumen = Worksheets("Resumen")
End Function

Function ws_historico() As Worksheet
    Set ws_historico = Worksheets("Historico")
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
    If ws.Name = "Severidad" Then
        fila_valores = WorksheetFunction.Match("Severidad", ws_severidad.Range("F:F"), 0) + header_triangulos
    ElseIf ws.Name = "Frecuencia" Then
        fila_valores = WorksheetFunction.Match("Frecuencia", ws_frecuencia.Range("F:F"), 0) + header_triangulos
    Else
        fila_valores = fila_ini_plantillas + header_triangulos
    End If
End Function


Function NumFilasEntreTriangulos(num_ocurrencias, header_triangulos, sep_triangulos) As Integer
    NumFilasEntreTriangulos = - (num_ocurrencias + header_triangulos + sep_triangulos)
End Function



' COLORES

Function blanco() As Long
    blanco = RGB(255, 255, 255)
End Function

Function gris_oscuro() As Long
    gris_oscuro = RGB(101, 104, 103)
End Function

Function gris_claro() As Long
    gris_claro = RGB(226, 226, 226)
End Function

Function azul_oscuro() As Long
    azul_oscuro = RGB(0, 51, 160)
End Function

Function azul_claro() As Long
    azul_claro = RGB(45, 113, 255)
End Function

Function amarillo_oscuro() As Long
    amarillo_oscuro = RGB(119, 122, 14)
End Function

Function amarillo_claro() As Long
    amarillo_claro = RGB(180, 184, 20)
End Function

Function violeta_oscuro() As Long
    violeta_oscuro = RGB(128, 0, 128)
End Function

Function violeta_claro() As Long
    violeta_claro = RGB(192, 0, 192)
End Function

Function naranja_oscuro() As Long
    naranja_oscuro = RGB(179, 103, 0)
End Function

Function naranja_claro() As Long
    naranja_claro = RGB(237, 139, 0)
End Function

Function cian_claro() As Long
    cian_claro = RGB(0, 174, 199)
End Function

Function verde_oscuro() As Long
    verde_oscuro = RGB(89, 142, 23)
End Function

Function verde_claro() As Long
    verde_claro = RGB(120, 190, 32)
End Function


' FORMATOS

Function formato_plata() As String
    formato_plata = "$#,##0"
End Function

Function formato_porcentaje() As String
    formato_porcentaje = "0.00%"
End Function

Function formato_numero() As String
    formato_numero = "#,##0"
End Function


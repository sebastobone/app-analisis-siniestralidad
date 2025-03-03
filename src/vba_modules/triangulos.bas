Sub CrearConsolidadoTriangulos(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo)

	fila_dllo = WorksheetFunction.Match("DESARROLLO", ws.Range("F:F"), 0)
	fila_evo = WorksheetFunction.Match("Evolucion", ws.Range("F:F"), 0)
	fila_tabla = fila_evo + header_triangulos + num_ocurrencias + sep_triangulos

	If ws.Name = "Frecuencia" Then
		formato = formato_porcentaje()
	Else
		formato = formato_plata()
	End If

	fila_val = fila_valores(ws, fila_ini_plantillas, header_triangulos)
	num_indice = "COUNTA(R" & fila_tabla + 1 & "C1:RC1)"

	With ws.Cells(fila_tabla - 2, 1)
		.Value = "metodologia"
		.Font.Bold = True
		.Font.Color = blanco()
		.Interior.Color = gris_oscuro()
	End With

	With ws.Cells(fila_tabla - 2, 2)
		.Value = "pago"
		.Interior.Color = gris_claro()
	End With

	celda_metodologia = " R" & fila_tabla - 2 & "C2 "

	col_ocurrencia = crear_columna(ws, fila_tabla, 1, "ocurrencia", "=INDEX(R" & fila_val & "C" & col_ocurrs_plantillas & ":R" & fila_val + num_ocurrencias - 1 & "C" & col_ocurrs_plantillas & ", ROW() - " & fila_tabla & ")", "", False, num_ocurrencias, gris_oscuro(), blanco())
	col_pago = crear_columna(ws, fila_tabla, 2, "pago_" + atributo, "=INDEX(R" & fila_val & "C" & col_ocurrs_plantillas + 1 & ":R" & fila_val + num_ocurrencias - 1 & "C" & col_ocurrs_plantillas + num_alturas & ", " + num_indice + ", " & num_alturas & " - (" + num_indice + " - 1))", formato, False, num_ocurrencias, cian_claro(), blanco())
	col_incurrido = crear_columna(ws, fila_tabla, 3, "incurrido_" + atributo, "=INDEX(R" & fila_val & "C" & col_ocurrs_plantillas + num_alturas + 1 & ":R" & fila_val + num_ocurrencias - 1 & "C" & col_ocurrs_plantillas + num_alturas * 2 & ", " + num_indice + ", " & num_alturas & " - (" + num_indice + " - 1))", formato, False, num_ocurrencias, amarillo_claro(), blanco())

	desref_filas_diagonal = " IF(" & celda_metodologia & " = ""pago"", " & num_alturas & ", " & num_alturas * 2 & ") - " + num_indice + " + 1 "
	pct_desarrollo = "INDEX(R" & fila_dllo & "C" & col_ocurrs_plantillas + 1 & " : R" & fila_dllo & "C" & col_ocurrs_plantillas + num_alturas * 2 & ", 1, " + desref_filas_diagonal + ")"
	
	desref_filas_tri = NumFilasEntreTriangulos(num_ocurrencias, header_triangulos, sep_triangulos)
	ult_chain_ladder = "IF(" & celda_metodologia & " = ""pago"", R[" & desref_filas_tri * 2 + 1 & "]C" & col_ocurrs_plantillas + num_alturas & ", R[" & desref_filas_tri * 2 + 1 & "]C" & col_ocurrs_plantillas + num_alturas * 2 & ")"
	
	If ws.Name = "Plata" Then
		indicador = "RC[2] * RC[4]"
	Else
		indicador = "RC[2]"
	End If

	formula_ult = _
		"=IF(RC5 = ""chain-ladder"", " & _
		ult_chain_ladder + ", " & _
		"IF(RC5 = ""bornhuetter-ferguson"", " & _
		" IF(" & celda_metodologia & " = ""pago"", RC2, RC3) + " & indicador  & " * (1 - " + pct_desarrollo + "), " & _
		indicador & _
		"))"

	col_ultimate = crear_columna(ws, fila_tabla, 4, "ultimate", formula_ult, formato, True, num_ocurrencias, gris_oscuro(), blanco())
	col_metodologia = crear_columna(ws, fila_tabla, 5, "metodologia", "=""chain-ladder"" ", "", True, num_ocurrencias, gris_oscuro(), blanco())
	col_indicador = crear_columna(ws, fila_tabla, 6, "indicador", "", "", True, num_ocurrencias, gris_oscuro(), blanco())
	col_comentarios = crear_columna(ws, fila_tabla, 7, "comentarios", "", "", True, num_ocurrencias, gris_oscuro(), blanco())

	col_factor_escala = 8
	
	If ws.Name = "Plata" Then
		col_prima_devengada = crear_columna(ws, fila_tabla, 8, "prima_devengada", "=SUMIFS(Resumen!C" & obtener_numero_columna(ws_resumen, "prima_" + atributo_fem(atributo) + "_devengada") & ", Resumen!C" & obtener_numero_columna(ws_resumen, "periodo_ocurrencia") & ", RC1, Resumen!C1, """& apertura &""")", formato, False, num_ocurrencias, gris_oscuro(), blanco())
		col_indicador = crear_columna(ws, fila_tabla, 6, "indicador", "=IFERROR( " + ult_chain_ladder + " / RC[2], 0)", formato_porcentaje(), True, num_ocurrencias, gris_oscuro(), blanco())
		col_factor_escala = 9
	End If

	col_factor_escala = crear_columna(ws, fila_tabla, col_factor_escala, "factor_escala", "=IFERROR(RC4 / RC[1], 1)", "", False, num_ocurrencias, gris_oscuro(), blanco())
	col_ultimate_chain_ladder = crear_columna(ws, fila_tabla, col_factor_escala + 1, "ultimate_chain_ladder", "=" +ult_chain_ladder, formato, False, num_ocurrencias, gris_oscuro(), blanco())

End Sub



Sub GenerarFrecuencia(num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo, mes_del_periodo, num_meses_periodo)

	Application.ScreenUpdating = False
	Application.Calculation = xlCalculationManual

	Set ws = ws_frecuencia()

	Call FormatearTriangulo(ws, fila_ini_plantillas, header_triangulos, col_ocurrs_plantillas, num_ocurrencias, num_alturas, formato_numero())

	desref_filas = NumFilasEntreTriangulos(num_ocurrencias, header_triangulos, sep_triangulos)

	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Frecuencia", "=IF(R[" & desref_filas & "]C = """", """", IFERROR(R[" & desref_filas & "]C / SUMIFS(Resumen!C" & obtener_numero_columna(ws_resumen, "expuestos") & ", Resumen!C" & obtener_numero_columna(ws_resumen, "periodo_ocurrencia") & ", RC" & col_ocurrs_plantillas & ", Resumen!C" & obtener_numero_columna(ws_resumen, "apertura_reservas") & ", """ & apertura & """), 0))", "0.0000%")
	Call CrearEstructuraFactores(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas)

	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion Chain-Ladder", "=+IF(R[" & desref_filas * 4 - sep_triangulos * 2 - 16 & "]C = """", RC[-1] * R[" & desref_filas & "]C[-1], R[" & desref_filas * 4 - sep_triangulos * 2 - 16 & "]C)", "#,##0.0000%")
	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion", "=IF(R[" & desref_filas * 5 - sep_triangulos * 2 - 16 & "]C = """", R[" & desref_filas & "]C * R[" & num_ocurrencias + sep_triangulos + 1 & "]C8, R[" & desref_filas * 5 - sep_triangulos * 2 - 16 & "]C)", "#,##0.0000%")
	
	Call CrearConsolidadoTriangulos(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura,atributo)
	Call GraficarUltimate(ws, num_ocurrencias, "Frecuencia")
	' Call grafica_analisis_factores

	Application.ScreenUpdating = True
	Application.Calculation = xlCalculationAutomatic

End Sub



Sub GenerarSeveridad(num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo, mes_del_periodo, num_meses_periodo)

	Application.ScreenUpdating = False
	Application.Calculation = xlCalculationManual

	Set ws = ws_severidad()

	Call FormatearTriangulo(ws, fila_ini_plantillas, header_triangulos, col_ocurrs_plantillas, num_ocurrencias, num_alturas, formato_plata())
	
	tipo_index = "Ninguna"
	medida_index = "Ninguna"
	
	'''TRIANGULOS INSUMO
	desref_filas = NumFilasEntreTriangulos(num_ocurrencias, header_triangulos, sep_triangulos)
	desfase_triangulo_frec = 1
	
	' If tipo_index = "Por fecha de ocurrencia" Then
	'   col_medida_index = WorksheetFunction.Match(medida_index, Range("Vectores_Index!1:1"), 0)
	' 	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Unidad Indexacion", "=+XLOOKUP(VALUE(RC" & col_ocurrs_plantillas & "), Vectores_Index!C1, Vectores_Index!C" & col_medida_index & ")", "#,##0")
	' 	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Severidad Base", "=+IF(R[" & desref_filas * 2 & "]C = """", """", IFERROR(R[" & desref_filas * 2 & "]C / R[" & desref_filas & "]C, 0))", "#,##0")
	' 	desfase_triangulo_frec = 3
		
	' ElseIf tipo_index = "Por fecha de pago" Then
	'   col_medida_index = WorksheetFunction.Match(medida_index, Range("Vectores_Index!1:1"), 0)
	' 	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Plata Desacumulada", "=+IF(R[" & desref_filas & "]C = """", """", IF(R9C = 0, R[" & desref_filas & "]C, R[" & desref_filas & "]C - R[" & desref_filas & "]C[-1]))", "#,##0")
	' 	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Unidad Indexacion", "=+XLOOKUP(YEAR(EOMONTH(DATE(LEFT(RC" & col_ocurrs_plantillas & ", 4), RIGHT(RC" & col_ocurrs_plantillas & ", 2), 1), R9C * " & num_meses_periodo & ")) * 100 + MONTH(EOMONTH(DATE(LEFT(RC" & col_ocurrs_plantillas & ", 4), RIGHT(RC" & col_ocurrs_plantillas & ", 2), 1), R9C * " & num_meses_periodo & ")), Vectores_Index!C1,Vectores_Index!C" & col_medida_index & ")", "#,##0")
	' 	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Plata Desacumulada Unitaria", "=+IF(R[" & desref_filas * 2 & "]C = """", """", R[" & desref_filas * 2 & "]C / R[" & desref_filas & "]C)", "#,##0")
	' 	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Plata Acumulada Unitaria", "=+IF(R[" & desref_filas & "]C = """", """", IF(R9C = 0, R[" & desref_filas & "]C, R[" & desref_filas & "]C + RC[-1]))", "#,##0")
	' 	desfase_triangulo_frec = 5
		
	' End If

	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Severidad", "=+IF(R[" & desref_filas & "]C = """", """", IFERROR(R[" & desref_filas & "]C / Frecuencia!R[" & desref_filas * desfase_triangulo_frec & "]C, 0))", "#,##0")
	
	
	'''FACTORES
	Call CrearEstructuraFactores(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas)
	
	
	'''EVOLUCION
	fila_evo = last_row(ws, col_ocurrs_plantillas) + sep_triangulos + 1
	fila_noevo = WorksheetFunction.Match("Severidad", ws.Range("F:F"), 0)
	
	If tipo_index = "Ninguna" Then
		Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion Chain-Ladder", "=+IF(R[" & fila_noevo - fila_evo & "]C = """", RC[-1] * R[" & desref_filas & "]C[-1], R[" & fila_noevo - fila_evo & "]C)", "#,##0")
		
	ElseIf tipo_index = "Por fecha de ocurrencia" Then
		Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion Unitaria", "=+IF(R[" & fila_noevo - fila_evo & "]C = """", RC[-1] * R[" & desref_filas & "]C[-1], R[" & fila_noevo - fila_evo & "]C)", "#,##0")
		Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion Chain-Ladder", "=+R[" & desref_filas & "]C * R[" & desref_filas * 7 - 20 & "]C", "#,##0")
		
		' ElseIf tipo_index = "Por fecha de pago" Then
		'     Call CrearTriangulo(ws, num_ocurrencias, "Evolucion Unitaria", "=+IF(R[" & fila_noevo - fila_evo & "]C="""",RC[-1]*R[" & -sep_triangulos - num_ocurrencias & "]C[-1],R[" & fila_noevo - fila_evo & "]C)", "#,##0.00")
		
		'     fila_evo_frec = WorksheetFunction.Match("Evolucion", Range("Frecuencia!F:F"), 0)
		'     fila_fact_frec = WorksheetFunction.Match("FACTORES SELECCIONADOS", Range("Frecuencia!F:F"), 0)
		
		'     col_expuestos = WorksheetFunction.Match("Expuestos 2", Range("Resumen!5:5"), 0)
		'     col_periodos = WorksheetFunction.Match("Container 2", Range("Resumen!5:5"), 0)
		'     col_aperturas = WorksheetFunction.Match("Project Name", Range("Resumen!5:5"), 0)
		'     row_apert = WorksheetFunction.Match("Project Name", Range("Inicio!B:B"), 0)
		
		'     Call CrearTriangulo(ws, num_ocurrencias, "Conteo Evolucionado", "=+Frecuencia!R[" & fila_evo_frec - fila_evo - num_ocurrencias - sep_triangulos & "]C * SUMIFS(Resumen!C" & col_expuestos & ", Resumen!C" & col_periodos & ", RC6, Resumen!C" & col_aperturas & ", Inicio!R" & row_apert + 1 & "C2)", "#,##0.00")
		'     Call CrearTriangulo(ws, num_ocurrencias, "Plata Acumulada Unitaria Evolucionada", "=+R[" & (-sep_triangulos - num_ocurrencias) * 2 & "]C*R[" & -sep_triangulos - num_ocurrencias & "]C", "#,##0")
		'     Call CrearTriangulo(ws, num_ocurrencias, "Plata Desacumulada Unitaria Evolucionada", "=+IF(R9C=0,R[" & -sep_triangulos - num_ocurrencias & "]C,R[" & -sep_triangulos - num_ocurrencias & "]C-R[" & -sep_triangulos - num_ocurrencias & "]C[-1])", "#,##0")
		'     Call CrearTriangulo(ws, num_ocurrencias, "Plata Desacumulada Evolucionada", "=+R[" & -sep_triangulos - num_ocurrencias & "]C * R[" & (-sep_triangulos - num_ocurrencias) * 11 - 20 & "]C", "#,##0")
		'     Call CrearTriangulo(ws, num_ocurrencias, "Plata Acumulada Evolucionada", "=+IF(R9C=0,R[" & -sep_triangulos - num_ocurrencias & "]C,R[" & -sep_triangulos - num_ocurrencias & "]C+RC[-1])", "#,##0")
		'     Call CrearTriangulo(ws, num_ocurrencias, "Evolucion Chain-Ladder", "=+IFERROR(R[" & -sep_triangulos - num_ocurrencias & "]C / R[" & (-sep_triangulos - num_ocurrencias) * 5 & "]C, 0)", "#,##0")
		
	End If
	
	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion", "=IF(R[" & fila_noevo - fila_evo + desref_filas * 2 & "]C="""", R[" & desref_filas & "]C * R[" & num_ocurrencias + sep_triangulos & "]C8, R[" & desref_filas & "]C)", "#,##0")
	
	
	'''GRAFICAS
	Call CrearConsolidadoTriangulos(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo)
	Call GraficarUltimate(ws, num_ocurrencias, "Severidad " + atributo)
	' Call grafica_analisis_factores
	
	Application.ScreenUpdating = True
	Application.Calculation = xlCalculationAutomatic

End Sub



Sub GenerarPlata(num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo, mes_del_periodo, num_meses_periodo)

	Application.ScreenUpdating = False
	Application.Calculation = xlCalculationManual

	Set ws = ws_plata()

	Call FormatearTriangulo(ws, fila_ini_plantillas, header_triangulos, col_ocurrs_plantillas, num_ocurrencias, num_alturas, formato_plata())
	
	desref_filas = NumFilasEntreTriangulos(num_ocurrencias, header_triangulos, sep_triangulos)

	Call CrearEstructuraFactores(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas)

	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion Chain-Ladder", "=IF(R[" & desref_filas * 4 - sep_triangulos * 2 - 16 & "]C = """", RC[-1] * R[" & desref_filas & "]C[-1], R[" & desref_filas * 4 - sep_triangulos * 2 - 16 & "]C)", "$#,##0")
	Call CrearTriangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion", "=IF(R[" & desref_filas * 5 - sep_triangulos * 2 - 16 & "]C = """", R[" & desref_filas & "]C * R[" & num_ocurrencias + sep_triangulos + 1 & "]C9, R[" & desref_filas * 5 - sep_triangulos * 2 - 16 & "]C)", "$#,##0")

	Call CrearConsolidadoTriangulos(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo)
	Call GraficarUltimate(ws, num_ocurrencias, "Plata " + atributo)
	' Call grafica_analisis_factores
	
	Application.ScreenUpdating = True
	Application.Calculation = xlCalculationAutomatic
	
End Sub
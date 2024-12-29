Function columna_consolidado(ws, num_ocurrencias, fila_tabla, num_columna, nombre, formula, formato)

	With ws.Cells(fila_tabla, num_columna)
		.Value = nombre
		.Font.Bold = True
		.Interior.ThemeColor = xlThemeColorAccent1
		.Interior.TintAndShade = 0.799981688894314
		.HorizontalAlignment = xlCenter
		.VerticalAlignment = xlBottom
	End With

	With ws.Range(ws.Cells(fila_tabla + 1, num_columna), ws.Cells(fila_tabla + num_ocurrencias, num_columna))
		.Formula2R1C1 = formula
		.NumberFormat = formato

		If nombre = "Comentarios" Then
			.Interior.ThemeColor = xlThemeColorAccent4
			.Interior.TintAndShade = 0.799981688894314
		ElseIf nombre = "Metodologia" Then
			.Validation.Add Type : = xlValidateList, AlertStyle : = xlValidAlertStop, Formula1 : = "Chain-Ladder,Bornhuetter-Ferguson,Indicador"
		End If
	End With

End Function



Sub consolidado_triangulos(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo)

	fila_dllo = WorksheetFunction.Match("DESARROLLO", ws.Range("F:F"), 0)
	fila_evo = WorksheetFunction.Match("Evolucion", ws.Range("F:F"), 0)
	fila_tabla = fila_evo + header_triangulos + num_ocurrencias + sep_triangulos

	If ws.Name = "Plantilla_Frec" Then
		formato = "0.0000%"
	Else
		formato = "$#,##0"
	End If

	fila_val = fila_valores(ws, fila_ini_plantillas, header_triangulos)
	num_indice = "COUNTA(R" & fila_tabla + 1 & "C1:RC1)"

	Call columna_consolidado(ws, num_ocurrencias, fila_tabla, 1, "Ocurrencia", "=INDEX(R" & fila_val & "C" & col_ocurrs_plantillas & ":R" & fila_val + num_ocurrencias - 1 & "C" & col_ocurrs_plantillas & ", ROW() - " & fila_tabla & ")", "")
	Call columna_consolidado(ws, num_ocurrencias, fila_tabla, 2, "Pagos", "=INDEX(R" & fila_val & "C" & col_ocurrs_plantillas + 1 & ":R" & fila_val + num_ocurrencias - 1 & "C" & col_ocurrs_plantillas + num_alturas & ", " + num_indice + ", " & num_alturas & " - (" + num_indice + " - 1))", formato)
	Call columna_consolidado(ws, num_ocurrencias, fila_tabla, 3, "Incurridos", "=INDEX(R" & fila_val & "C" & col_ocurrs_plantillas + num_alturas + 1 & ":R" & fila_val + num_ocurrencias - 1 & "C" & col_ocurrs_plantillas + num_alturas * 2 & ", " + num_indice + ", " & num_alturas & " - (" + num_indice + " - 1))", formato)
	
	desref_filas_diagonal = " IF(R2C3 = ""Pago"", " & num_alturas & ", " & num_alturas * 2 & ") - " + num_indice + " + 1 "
	pct_desarrollo = "INDEX(R" & fila_dllo & "C" & col_ocurrs_plantillas + 1 & " : R" & fila_dllo & "C" & col_ocurrs_plantillas + num_alturas * 2 & ", 1, " + desref_filas_diagonal + ")"
	
	desref_filas_tri = desref_filas_triangulo(num_ocurrencias, header_triangulos, sep_triangulos)
	ult_chain_ladder = "IF(R2C3 = ""Pago"", R[" & desref_filas_tri * 2 + 1 & "]C" & col_ocurrs_plantillas + num_alturas & ", R[" & desref_filas_tri * 2 + 1 & "]C" & col_ocurrs_plantillas + num_alturas * 2 & ")"
	
	formula_ult = _
		"=IF(RC5 = ""Chain-Ladder"", " & _
		ult_chain_ladder + ", " & _
		"IF(RC5 = ""Bornhuetter-Ferguson"", " & _
		" IF(R2C3 = ""Pago"", RC2, RC3) + RC[2] * (1 - " + pct_desarrollo + "), " & _
		"RC[2]" & _
		"))"
	
	Call columna_consolidado(ws, num_ocurrencias, fila_tabla, 4, "Ultimate", formula_ult, formato)
	Call columna_consolidado(ws, num_ocurrencias, fila_tabla, 5, "Metodologia", "Chain-Ladder", "")
	Call columna_consolidado(ws, num_ocurrencias, fila_tabla, 6, "Indicador", "=" + ult_chain_ladder, formato)
	Call columna_consolidado(ws, num_ocurrencias, fila_tabla, 7, "Comentarios", "", "")

	col_factor_escala = 8
	
	If ws.Name = "Plantilla_Plata" Then

		Call columna_consolidado(ws, num_ocurrencias, fila_tabla, 8, "Prima Devengada", "=SUMIFS(IF("""& atributo &""" = ""Bruto"", Aux_Totales!C" & col_auxtot("prima_bruta_devengada") & ", Aux_Totales!C" & col_auxtot("prima_retenida_devengada") & "), Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C1, """& apertura &""")", formato)
		Call columna_consolidado(ws, num_ocurrencias, fila_tabla, 6, "Indicador", "=IFERROR( " + ult_chain_ladder + " / RC[2], 0)", "0.00%")
		
		formula_ult = _
			"=IF(RC5 = ""Chain-Ladder"", " & _
			ult_chain_ladder + ", " & _
			"IF(RC5 = ""Bornhuetter-Ferguson"", " & _
			" IF(R2C3 = ""Pago"", RC2, RC3) + RC[2] * RC[4] * (1 - " + pct_desarrollo + "), " & _
			"RC[2] * RC[4]" & _
			"))"

		Call columna_consolidado(ws, num_ocurrencias, fila_tabla, 4, "Ultimate", formula_ult, formato)

		col_factor_escala = 9
		
	End If

	Call columna_consolidado(ws, num_ocurrencias, fila_tabla, col_factor_escala, "Factor escala", "=IFERROR(RC4 / RC[1], 1)", "")
	Call columna_consolidado(ws, num_ocurrencias, fila_tabla, col_factor_escala + 1, "Ult Chain-Ladder", "=" + ult_chain_ladder, formato)

End Sub



Sub generar_Plantilla_Frec(num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo, mes_del_periodo)

	Application.ScreenUpdating = False
	Application.Calculation = xlCalculationManual

	Set ws = ws_frec()

	Call color_columnas_triangulo(ws, num_alturas)

	desref_filas = desref_filas_triangulo(num_ocurrencias, header_triangulos, sep_triangulos)

	Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Frecuencia", "=IF(R[" & desref_filas & "]C = """", """", IFERROR(R[" & desref_filas & "]C / SUMIFS(Aux_Totales!C" & col_auxtot("expuestos") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC" & col_ocurrs_plantillas & ", Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """ & apertura & """), 0))", "0.0000%")
	Call estructura_factores(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas)

	Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion Chain-Ladder", "=+IF(R[" & desref_filas * 4 - sep_triangulos * 2 - 16 & "]C = """", RC[-1] * R[" & desref_filas & "]C[-1], R[" & desref_filas * 4 - sep_triangulos * 2 - 16 & "]C)", "#,##0.0000%")
	Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion", "=IF(R[" & desref_filas * 5 - sep_triangulos * 2 - 16 & "]C = """", R[" & desref_filas & "]C * R[" & num_ocurrencias + sep_triangulos + 1 & "]C8, R[" & desref_filas * 5 - sep_triangulos * 2 - 16 & "]C)", "#,##0.0000%")
	
	Call consolidado_triangulos(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura,atributo)
	Call grafica_ultimate(ws, num_ocurrencias, "Frecuencia")
	' Call grafica_analisis_factores

	Application.ScreenUpdating = True
	Application.Calculation = xlCalculationAutomatic

End Sub



Sub generar_Plantilla_Seve(num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo, mes_del_periodo)

	Application.ScreenUpdating = False
	Application.Calculation = xlCalculationManual

	Set ws = ws_seve()

	Call color_columnas_triangulo(ws, num_alturas)
	
	tipo_index = ws.Range("C3").value
	medida_index = ws.Range("C4").value
	col_medida_index = WorksheetFunction.Match(medida_index, Range("Vectores_Index!1:1"), 0)
	
	
	'''TRIANGULOS INSUMO
	desref_filas = desref_filas_triangulo(num_ocurrencias, header_triangulos, sep_triangulos)
	desfase_triangulo_frec = 1
	
	If tipo_index = "Por fecha de ocurrencia" Then
		Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Unidad Indexacion", "=+XLOOKUP(VALUE(RC" & col_ocurrs_plantillas & "), Vectores_Index!C1, Vectores_Index!C" & col_medida_index & ")", "#,##0")
		Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Severidad Base", "=+IF(R[" & desref_filas * 2 & "]C = """", """", IFERROR(R[" & desref_filas * 2 & "]C / R[" & desref_filas & "]C, 0))", "#,##0")
		desfase_triangulo_frec = 3
		
	ElseIf tipo_index = "Por fecha de pago" Then
		Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Plata Desacumulada", "=+IF(R[" & desref_filas & "]C = """", """", IF(R9C = 0, R[" & desref_filas & "]C, R[" & desref_filas & "]C - R[" & desref_filas & "]C[-1]))", "#,##0")
		Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Unidad Indexacion", "=+XLOOKUP(YEAR(EOMONTH(DATE(LEFT(RC" & col_ocurrs_plantillas & ", 4), RIGHT(RC" & col_ocurrs_plantillas & ", 2), 1), R9C * " & num_meses_periodo & ")) * 100 + MONTH(EOMONTH(DATE(LEFT(RC" & col_ocurrs_plantillas & ", 4), RIGHT(RC" & col_ocurrs_plantillas & ", 2), 1), R9C * " & num_meses_periodo & ")), Vectores_Index!C1,Vectores_Index!C" & col_medida_index & ")", "#,##0")
		Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Plata Desacumulada Unitaria", "=+IF(R[" & desref_filas * 2 & "]C = """", """", R[" & desref_filas * 2 & "]C / R[" & desref_filas & "]C)", "#,##0")
		Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Plata Acumulada Unitaria", "=+IF(R[" & desref_filas & "]C = """", """", IF(R9C = 0, R[" & desref_filas & "]C, R[" & desref_filas & "]C + RC[-1]))", "#,##0")
		desfase_triangulo_frec = 5
		
	End If

	Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Severidad", "=+IF(R[" & desref_filas & "]C = """", """", IFERROR(R[" & desref_filas & "]C / Plantilla_Frec!R[" & desref_filas * desfase_triangulo_frec & "]C, 0))", "#,##0")
	
	
	'''FACTORES
	Call estructura_factores(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas)
	
	
	'''EVOLUCION
	fila_evo = last_row(ws, col_ocurrs_plantillas) + sep_triangulos + 1
	fila_noevo = WorksheetFunction.Match("Severidad", ws.Range("F:F"), 0)
	
	If tipo_index = "Ninguna" Then
		Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion Chain-Ladder", "=+IF(R[" & fila_noevo - fila_evo & "]C = """", RC[-1] * R[" & desref_filas & "]C[-1], R[" & fila_noevo - fila_evo & "]C)", "#,##0")
		
	ElseIf tipo_index = "Por fecha de ocurrencia" Then
		Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion Unitaria", "=+IF(R[" & fila_noevo - fila_evo & "]C = """", RC[-1] * R[" & desref_filas & "]C[-1], R[" & fila_noevo - fila_evo & "]C)", "#,##0")
		Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion Chain-Ladder", "=+R[" & desref_filas & "]C * R[" & desref_filas * 7 - 20 & "]C", "#,##0")
		
		' ElseIf tipo_index = "Por fecha de pago" Then
		'     Call triangulo(ws, num_ocurrencias, "Evolucion Unitaria", "=+IF(R[" & fila_noevo - fila_evo & "]C="""",RC[-1]*R[" & -sep_triangulos - num_ocurrencias & "]C[-1],R[" & fila_noevo - fila_evo & "]C)", "#,##0.00")
		
		'     fila_evo_frec = WorksheetFunction.Match("Evolucion", Range("Plantilla_Frec!F:F"), 0)
		'     fila_fact_frec = WorksheetFunction.Match("FACTORES SELECCIONADOS", Range("Plantilla_Frec!F:F"), 0)
		
		'     col_expuestos = WorksheetFunction.Match("Expuestos 2", Range("Aux_Totales!5:5"), 0)
		'     col_periodos = WorksheetFunction.Match("Container 2", Range("Aux_Totales!5:5"), 0)
		'     col_aperturas = WorksheetFunction.Match("Project Name", Range("Aux_Totales!5:5"), 0)
		'     row_apert = WorksheetFunction.Match("Project Name", Range("Inicio!B:B"), 0)
		
		'     Call triangulo(ws, num_ocurrencias, "Conteo Evolucionado", "=+Plantilla_Frec!R[" & fila_evo_frec - fila_evo - num_ocurrencias - sep_triangulos & "]C * SUMIFS(Aux_Totales!C" & col_expuestos & ", Aux_Totales!C" & col_periodos & ", RC6, Aux_Totales!C" & col_aperturas & ", Inicio!R" & row_apert + 1 & "C2)", "#,##0.00")
		'     Call triangulo(ws, num_ocurrencias, "Plata Acumulada Unitaria Evolucionada", "=+R[" & (-sep_triangulos - num_ocurrencias) * 2 & "]C*R[" & -sep_triangulos - num_ocurrencias & "]C", "#,##0")
		'     Call triangulo(ws, num_ocurrencias, "Plata Desacumulada Unitaria Evolucionada", "=+IF(R9C=0,R[" & -sep_triangulos - num_ocurrencias & "]C,R[" & -sep_triangulos - num_ocurrencias & "]C-R[" & -sep_triangulos - num_ocurrencias & "]C[-1])", "#,##0")
		'     Call triangulo(ws, num_ocurrencias, "Plata Desacumulada Evolucionada", "=+R[" & -sep_triangulos - num_ocurrencias & "]C * R[" & (-sep_triangulos - num_ocurrencias) * 11 - 20 & "]C", "#,##0")
		'     Call triangulo(ws, num_ocurrencias, "Plata Acumulada Evolucionada", "=+IF(R9C=0,R[" & -sep_triangulos - num_ocurrencias & "]C,R[" & -sep_triangulos - num_ocurrencias & "]C+RC[-1])", "#,##0")
		'     Call triangulo(ws, num_ocurrencias, "Evolucion Chain-Ladder", "=+IFERROR(R[" & -sep_triangulos - num_ocurrencias & "]C / R[" & (-sep_triangulos - num_ocurrencias) * 5 & "]C, 0)", "#,##0")
		
	End If
	
	Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion", "=IF(R[" & fila_noevo - fila_evo + desref_filas * 2 & "]C="""", R[" & desref_filas & "]C * R[" & num_ocurrencias + sep_triangulos & "]C8, R[" & desref_filas & "]C)", "#,##0")
	
	
	'''GRAFICAS
	Call consolidado_triangulos(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo)
	Call grafica_ultimate(ws, num_ocurrencias, "Severidad " + atributo)
	' Call grafica_analisis_factores
	
	Application.ScreenUpdating = True
	Application.Calculation = xlCalculationAutomatic

End Sub



Sub generar_Plantilla_Plata(num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo, mes_del_periodo)

	' Application.ScreenUpdating = False
	Application.Calculation = xlCalculationManual

	Set ws = ws_plat()

	Call color_columnas_triangulo(ws, num_alturas)
	
	desref_filas = desref_filas_triangulo(num_ocurrencias, header_triangulos, sep_triangulos)

	Call estructura_factores(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas)

	Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion Chain-Ladder", "=IF(R[" & desref_filas * 4 - sep_triangulos * 2 - 16 & "]C = """", RC[-1] * R[" & desref_filas & "]C[-1], R[" & desref_filas * 4 - sep_triangulos * 2 - 16 & "]C)", "$#,##0")
	Call triangulo(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, "Evolucion", "=IF(R[" & desref_filas * 5 - sep_triangulos * 2 - 16 & "]C = """", R[" & desref_filas & "]C * R[" & num_ocurrencias + sep_triangulos + 1 & "]C9, R[" & desref_filas * 5 - sep_triangulos * 2 - 16 & "]C)", "$#,##0")

	Call consolidado_triangulos(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo)
	Call grafica_ultimate(ws, num_ocurrencias, "Plata " + atributo)
	' Call grafica_analisis_factores
	
	Application.ScreenUpdating = True
	Application.Calculation = xlCalculationAutomatic
	
End Sub
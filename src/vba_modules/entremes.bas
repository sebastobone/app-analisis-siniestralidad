Function titulo_met_entremes(primera_columna, fila_tabla, titulo)

	Set ws = ws_entremes()

	col_ini = WorksheetFunction.Match(primera_columna, ws.Range(" " & fila_tabla & ":" & fila_tabla & " "), 0)
	ws.Cells(fila_tabla - 1, col_ini).value = titulo
	num_cols = ws.Range(ws.Cells(fila_tabla, col_ini), ws.Cells(fila_tabla, col_ini).End(xlToRight)).Columns.Count - 1
	
	With ws.Range(ws.Cells(fila_tabla - 1, col_ini), ws.Cells(fila_tabla - 1, col_ini + num_cols))
		.Font.Bold = True
		.Font.Color = vbWhite
		.Interior.ThemeColor = xlThemeColorAccent1
	End With

End Function



Function columnas_entremes(nombre, formula, formato, num_ocurrencias, modificable, desfase, num_row)
	
	Set ws = ws_entremes()
	
	num_col = ws.Cells(num_row, ws.Columns.Count).End(xlToLeft).Column + desfase
	Set Rng = ws.Cells(num_row, num_col)
	Rng.value = nombre
	Call color_format(Rng)
	
	With ws.Range(ws.Cells(num_row + 1, num_col), ws.Cells(num_row + num_ocurrencias - 1, num_col))
		.Formula2R1C1 = formula
		.NumberFormat = formato
		
		If modificable Then
			.Interior.ThemeColor = xlThemeColorAccent4
			.Interior.TintAndShade = 0.799981688894314
		End If
		
	End With

End Function



Sub generar_Plantilla_Entremes(num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo, mes_del_periodo)

	' Application.ScreenUpdating = False
	Application.Calculation = xlCalculationManual
	
	Set ws = ws_entremes()

	Call color_columnas_triangulo(ws, fila_ini_plantillas, col_ocurrs_plantillas, num_alturas)

	Call estructura_factores(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas)

	''' FACTOR COMPLETITUD
	fila_factor_compl = last_row(ws, col_ocurrs_plantillas) + sep_triangulos
	num_meses_periodo = num_meses_periodo_fn(num_ocurrencias, num_alturas)
	fila_ind_altura = fila_ini_plantillas + header_triangulos - 1
	
	With ws.Range(ws.Cells(fila_factor_compl, col_ocurrs_plantillas + 1), ws.Cells(fila_factor_compl, col_ocurrs_plantillas + num_alturas))
		.Formula2R1C1 = "=IF(R" & fila_ind_altura & "C <= " & num_alturas - num_meses_periodo & ", R[" & - sep_triangulos & "]C / INDEX(R[" & - sep_triangulos & "]C" & col_ocurrs_plantillas + 1 & ":R[" & - sep_triangulos & "]C" & col_ocurrs_plantillas + num_alturas & ", 1, ROUNDUP(COUNT(R[" & - sep_triangulos & "]C" & col_ocurrs_plantillas + 1 & ":R[" & - sep_triangulos & "]C) / " & num_meses_periodo & ", 0) * " & num_meses_periodo & "), 100%)"
		.NumberFormat = "0.00%"
	End With
	
	With ws.Range(ws.Cells(fila_factor_compl, col_ocurrs_plantillas + num_alturas + 1), ws.Cells(fila_factor_compl, col_ocurrs_plantillas + num_alturas * 2))
		.Formula2R1C1 = "=IF(R" & fila_ind_altura & "C <= " & num_alturas - num_meses_periodo & ", R[" & - sep_triangulos & "]C / INDEX(R[" & - sep_triangulos & "]C" & col_ocurrs_plantillas + num_alturas + 1 & ":R[" & - sep_triangulos & "]C" & col_ocurrs_plantillas + num_alturas * 2 & ", 1, ROUNDUP(COUNT(R[" & - sep_triangulos & "]C" & col_ocurrs_plantillas + num_alturas + 1 & ":R[" & - sep_triangulos & "]C) / " & num_meses_periodo & ", 0) * " & num_meses_periodo & "), 100%)"
		.NumberFormat = "0.00%"
	End With
	
	ws.Cells(fila_factor_compl - sep_triangulos, col_ocurrs_plantillas).Copy
	ws.Cells(fila_factor_compl, col_ocurrs_plantillas).PasteSpecial
	ws.Cells(fila_factor_compl, col_ocurrs_plantillas).value = "FACTOR COMPLETITUD DIAGONAL"
	
	
	''' BASE
	fila_tabla = WorksheetFunction.Match("DESARROLLO", ws.Range("F:F"), 0) + sep_triangulos * 2 + 1
	
	ws.Activate
	Call columnas_entremes("Ocurrencia", "=FILTER(Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", Aux_Totales!C" & col_auxtot("apertura_reservas") & " = """& apertura &""")", "", 2, False, 0, fila_tabla)
	Call columnas_entremes("Pagos", "=SUMIFS(Aux_Totales!C" & col_auxtot("pago_" & atributo) & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &""")", "$#,##0", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
	Call columnas_entremes("Incurrido", "=SUMIFS(Aux_Totales!C" & col_auxtot("incurrido_" & atributo) & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &""")", "$#,##0", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
	Call columnas_entremes("Ultimate", "=0", "$#,##0", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
	
	Call grafica_ultimate(ws, num_ocurrencias + mes_del_periodo - 1, "Plata " + atributo)


	''' METODOLOGIA POR OCURRENCIA
	Call columnas_entremes("Metodologia", "=0", "", num_ocurrencias + mes_del_periodo, True, 1, fila_tabla)
	With ws.Range(ws.Cells(fila_tabla + 1, 5), ws.Cells(fila_tabla + num_ocurrencias - 1, 5))
		.Validation.Add Type : = xlValidateList, AlertStyle : = xlValidAlertStop, Formula1 : = "Completar diagonal,Bornhuetter-Ferguson,Mantener,%SUE,Sin IBNR"
		.value = "Completar diagonal"
	End With
	With ws.Range(ws.Cells(fila_tabla + num_ocurrencias, 5), ws.Cells(fila_tabla + num_ocurrencias + mes_del_periodo - 1, 5))
		.Validation.Add Type : = xlValidateList, AlertStyle : = xlValidAlertStop, Formula1 : = "Mantener,Estimar"
		.value = "Estimar"
	End With
	
	col_primas = col_auxtot("prima_" & atributo_fem(atributo) & "_devengada")
	Call columnas_entremes("Prima Devengada", "=SUMIFS(Aux_Totales!C" & col_primas & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &""")", "$#,##0", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
	col_prim = 6
	Call columnas_entremes("% SUE", "=IFERROR(RC[-3] / RC[-1], 0)", "0.00%", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
	Call columnas_entremes("Ajuste SUE", "=RC[-4] - RC[4]", "#,##0", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
	

	''' ALERTA
	Call columnas_entremes("Alerta", "=IFERROR(IF(ABS(RC[14] / RC[3] - 1) > 0.05, ""Completar diagonal ajusta mas de 5%"", """"), """")", "#,##0", num_ocurrencias, False, 1, fila_tabla)
	Call columnas_entremes("Comentarios", "", "", num_ocurrencias + mes_del_periodo, True, 1, fila_tabla)



	'' INFORMACION DEL CIERRE ANTERIOR
	col_primas_ant = col_auxant("Suma de prima_" & atributo_fem(atributo) & "_devengada")
	col_ultim_ant = col_auxant("Suma de plata_ultimate_" & atributo)
	col_ultim_cont_ant = col_auxant("Suma de plata_ultimate_contable_" & atributo)
	col_ocurrs_ant = col_auxant("periodo_ocurrencia")
	col_atipicos_ant = col_auxant("atipico")
	col_aperturas_ant = col_auxant("apertura_reservas")
	col_mes_corte_ant = col_auxant("mes_corte")

	Call columnas_entremes("Ultim Actuarial Ant", _
		"=SUMIFS(Aux_Anterior!C" & col_ultim_ant & ", Aux_Anterior!C" & col_ocurrs_ant & ", RC1, Aux_Anterior!C" & col_aperturas_ant & ", """& apertura &""", Aux_Anterior!C" & col_mes_corte_ant & ", " & mes_anterior() & ", Aux_Anterior!C" & col_atipicos_ant & ", 0)", _
		"$#,##0", num_ocurrencias + mes_del_periodo - 1, False, 2, fila_tabla)
	Call columnas_entremes("Ultim Contable Ant", "=SUMIFS(Aux_Anterior!C" & col_ultim_cont_ant & ", Aux_Anterior!C" & col_ocurrs_ant & ", RC1, Aux_Anterior!C" & col_aperturas_ant & ", """& apertura &""", Aux_Anterior!C" & col_mes_corte_ant & ", " & mes_anterior() & ", Aux_Anterior!C" & col_atipicos_ant & ", 0)", "$#,##0", num_ocurrencias + mes_del_periodo - 1, False, 1, fila_tabla)
	Call columnas_entremes("Prima Dev Anterior", "=SUMIFS(Aux_Anterior!C" & col_primas_ant & ", Aux_Anterior!C" & col_ocurrs_ant & ", RC1, Aux_Anterior!C" & col_aperturas_ant & ", """& apertura &""", Aux_Anterior!C" & col_mes_corte_ant & ", " & mes_anterior() & ", Aux_Anterior!C" & col_atipicos_ant & ", 0)", "$#,##0", num_ocurrencias + mes_del_periodo - 1, False, 1, fila_tabla)
	Call columnas_entremes("% SUE Anterior", "=IFERROR(RC[-3] / RC[-1],0)", "0.00%", num_ocurrencias + mes_del_periodo - 1, False, 1, fila_tabla)


	''' METODOLOGIA 1
	fila_val = fila_valores(ws, fila_ini_plantillas, header_triangulos)
	Call columnas_entremes("Pagos Triangulo", "=INDEX(R" & fila_val & "C" & col_ocurrs_plantillas + 1 & ":R" & fila_val + num_ocurrencias - 1 & "C" & col_ocurrs_plantillas + num_alturas & ", COUNTA(R" & fila_tabla + 1 & "C1:RC1), " & num_alturas & " - (COUNTA(R" & fila_tabla + 1 & "C1:RC1) - 1) * " & num_meses_periodo & " - " & mes_del_periodo & ")", "$#,##0", num_ocurrencias, False, 2, fila_tabla)
	Call columnas_entremes("Incurridos Triangulo", "=INDEX(R" & fila_val & "C" & col_ocurrs_plantillas + num_alturas + 1 & ":R" & fila_val + num_ocurrencias - 1 & "C" & col_ocurrs_plantillas + num_alturas * 2 & ", COUNTA(R" & fila_tabla + 1 & "C1:RC1), " & num_alturas & " - (COUNTA(R" & fila_tabla + 1 & "C1:RC1) - 1) * " & num_meses_periodo & " - " & mes_del_periodo & ")", "$#,##0", num_ocurrencias, False, 1, fila_tabla)
	Call columnas_entremes("Velocidad Triangulo", "=IFERROR(IF(R4C3 = ""Pago"", R[-1]C[-2], R[-1]C[-1]) / R[-1]C[-7], 100%)", "0.00%", num_ocurrencias, False, 1, fila_tabla)
	
	Call columnas_entremes("Factor completitud", "=IFERROR(INDEX(IF(R4C3 = ""Pago"", R" & fila_tabla - 3 & "C7:R" & fila_tabla - 3 & "C" & 6 + num_alturas & ", R" & fila_tabla - 3 & "C" & 7 + num_alturas & ":R" & fila_tabla - 3 & "C" & 6 + num_alturas * 2 & "), 1, (COUNTA(R" & fila_tabla + num_ocurrencias & "C1:RC1) - 1) * " & num_meses_periodo & " + " & mes_del_periodo & "), 100%)", "0.00%", num_ocurrencias, True, 1, fila_tabla)
	Call columnas_entremes("Diagonal completa", "=IF(R4C3 = ""Pago"", RC2, RC3) / RC[-1]", "$#,##0", num_ocurrencias, False, 1, fila_tabla)
	Call columnas_entremes("Velocidad actual", "=RC[-2] * RC[-3]", "0.00%", num_ocurrencias, False, 1, fila_tabla)
	
	Call columnas_entremes("Ultimate", "=RC[-2] / RC[-4]", "$#,##0", num_ocurrencias, False, 1, fila_tabla)
	Call columnas_entremes("% SUE", "=RC[-1] / RC" & col_prim & " ", "0.00%", num_ocurrencias, False, 1, fila_tabla)
	Call columnas_entremes("Ajuste SUE", "=RC[-2] - RC[-12]", "#,##0", num_ocurrencias, False, 1, fila_tabla)
	
	Call titulo_met_entremes("Pagos Triangulo", fila_tabla, "Metodologia 1: Mantener velocidades, completar diagonal")


	''' METODOLOGIA 2: BF
	Call columnas_entremes("% SUE BF", "=RC[-12]", "0.00%", num_ocurrencias, True, 2, fila_tabla)
	Call columnas_entremes("Velocidad BF", "=RC[-6]", "0.00%", num_ocurrencias, True, 1, fila_tabla)
	Call columnas_entremes("Ultimate", "=IF(R4C3 = ""Pago"", RC2, RC3) + RC[-2] * RC" & col_prim & " * (1 - RC[-1])", "$#,##0", num_ocurrencias, False, 1, fila_tabla)
	Call columnas_entremes("% SUE", "=IFERROR(RC[-1] / RC" & col_prim & ", 0) ", "0.00%", num_ocurrencias, False, 1, fila_tabla)
	Call columnas_entremes("Ajuste SUE", "=RC[-2] - RC[-19]", "$#,##0", num_ocurrencias, False, 1, fila_tabla)
	
	Call titulo_met_entremes("% SUE BF", fila_tabla, "Metodologia 2: Bornhuetter-Ferguson")


	''' METODOLOGIA 3: %SUE
	Call columnas_entremes("% SUE Nuevo", "=RC[-18]", "0.00%", num_ocurrencias, True, 2, fila_tabla)
	Call columnas_entremes("Ultimate", "=RC[-1] * RC" & col_prim & " ", "$#,##0", num_ocurrencias, False, 1, fila_tabla)
	Call columnas_entremes("Ajuste SUE", "=RC[-1] - RC[-23]", "$#,##0", num_ocurrencias, False, 1, fila_tabla)
	
	Call titulo_met_entremes("% SUE Nuevo", fila_tabla, "Metodologia 3: % Siniestralidad")
	
	
	''' AJUSTE PARCIAL
	Call columnas_entremes("% Ajuste", "=IFERROR(1 / (" & num_meses_periodo - mes_del_periodo & "), 100%)", "0.00%", num_ocurrencias + mes_del_periodo - 1, True, 2, fila_tabla)
	Call columnas_entremes("Ajuste parcial", "=IFERROR((RC[-34] - RC[-25]) * IF(RC[-1] = """", 100%, RC[-1]), 0)", "#,##0", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
	Call columnas_entremes("Ultimate Contable", "=IFERROR(RC[-26] + RC[-1], 0)", "#,##0", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
	Call columnas_entremes("Comentarios Aj.", "", "#,##0", num_ocurrencias + mes_del_periodo - 1, True, 1, fila_tabla)
	
	Call titulo_met_entremes("% Ajuste", fila_tabla, "Ajuste Parcial")
	
	
	''' SUE FINAL
	ws.Range(ws.Cells(fila_tabla + 1, 4), ws.Cells(fila_tabla + num_ocurrencias + mes_del_periodo - 1, 4)).FormulaR1C1 = "=IF(RC[1] = ""Mantener"", RC[8], IF(RC[1] = ""Completar diagonal"", RC[19], IF(RC[1] = ""Bornhuetter-Ferguson"", RC[25], IF(RC[1] = ""Sin IBNR"", RC[-1], RC[30]))))"
	
	ws.Cells(fila_tabla - 2, 1).Interior.ThemeColor = xlThemeColorAccent4
	ws.Cells(fila_tabla - 2, 1).Interior.TintAndShade = 0.799981688894314
	ws.Cells(fila_tabla - 2, 2).value = "Modifique las celdas de este color"
	
	
	''' SUPUESTO NUEVA OCURRENCIA Y F&S
	ws.Range(ws.Cells(fila_tabla, 1), ws.Cells(fila_tabla + num_ocurrencias + mes_del_periodo - 1, 1)).Copy
	sep_tabla = num_ocurrencias + mes_del_periodo + sep_triangulos
	fila_tabla = fila_tabla + sep_tabla
	
	If ws.Cells(5, 3) = "Frecuencia y Severidad" Then
		
		With ws.Cells(fila_tabla, 1)
			.PasteSpecial Paste : = xlPasteValues
			.PasteSpecial Paste : = xlPasteFormats
		End With
		
		Call columnas_entremes("Frecuencia Pagos", _
			"=IFERROR(SUMIFS(Aux_Totales!C" & col_auxtot("conteo_pago") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &""") / SUMIFS(Aux_Totales!C" & col_auxtot("expuestos") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &"""), 0) ", _
			"0.0000%", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
		Call columnas_entremes("Frecuencia Incurridos", _
			"=IFERROR(SUMIFS(Aux_Totales!C" & col_auxtot("conteo_incurrido") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &""") / SUMIFS(Aux_Totales!C" & col_auxtot("expuestos") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &"""), 0) ", _
			"0.0000%", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
		
		Call columnas_entremes("Frecuencia Ultimate", _
			"=IF(R6C3 = ""Severidad"", " & _
			"IFERROR(SUMIFS(Aux_Anterior!C" & col_auxant("Suma de frec_ultimate") & ", Aux_Anterior!C" & col_auxant("periodo_ocurrencia") & ", RC1, Aux_Anterior!C" & col_aperturas_ant & ", """& apertura &""", Aux_Anterior!C" & col_mes_corte_ant & ", " & mes_anterior() & "), 0), " & _
			"IFERROR(R[" & - sep_tabla & "]C / (RC[6] * RC[9]), 0))", _
			"0.0000%", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
		
		'NUEVA OCURRENCIA FRECUENCIA
		With ws.Range(ws.Cells(fila_tabla + num_ocurrencias, 4), ws.Cells(fila_tabla + num_ocurrencias + mes_del_periodo - 1, 4))
			.FormulaR1C1 = "=R" & fila_tabla + num_ocurrencias - 1 & "C / " & num_meses_periodo & ""
			.Font.Bold = True
			.Interior.ThemeColor = xlThemeColorAccent4
			.Interior.TintAndShade = 0.799981688894314
			.NumberFormat = "0.0000%"
		End With
		
		Call columnas_entremes("Comentarios Fr.", "", "", num_ocurrencias + mes_del_periodo, True, 1, fila_tabla)
		
		Call columnas_entremes("Periodo", "=RC1", "", num_ocurrencias + mes_del_periodo, False, 2, fila_tabla)
		
		Call columnas_entremes("Severidad Pagos", _
			"=IFERROR(R[" & - sep_tabla & "]C2 / SUMIFS(Aux_Totales!C" & col_auxtot("conteo_pago") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &"""), 0) ", _
			"$#,##0", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
		
		Call columnas_entremes("Severidad Incurridos", _
			"=IFERROR(R[" & - sep_tabla & "]C3 / SUMIFS(Aux_Totales!C" & col_auxtot("conteo_incurrido") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &"""), 0) ", _
			"$#,##0", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
		
		Call columnas_entremes("Severidad Ultimate", _
			"=IF(R6C3 = ""Frecuencia"", " & _
			"IFERROR(SUMIFS(Aux_Anterior!C" & col_auxant("Suma de seve_ultimate_" & atributo) & ", Aux_Anterior!C" & col_auxant("periodo_ocurrencia") & ", RC1, Aux_Anterior!C" & col_aperturas_ant & ", """& apertura &""", Aux_Anterior!C" & col_mes_corte_ant & ", " & mes_anterior() & "), 0), " & _
			"IFERROR(R[" & - sep_tabla & "]C[-6] / (RC[-6] * RC[3]), 0))", _
			"$#,##0", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
		
		'NUEVA OCURRENCIA SEVERIDAD
		With ws.Range(ws.Cells(fila_tabla + num_ocurrencias, 10), ws.Cells(fila_tabla + num_ocurrencias + mes_del_periodo - 1, 10))
			.FormulaR1C1 = "=R" & fila_tabla + num_ocurrencias - 1 & "C"
			.Font.Bold = True
			.Interior.ThemeColor = xlThemeColorAccent4
			.Interior.TintAndShade = 0.799981688894314
			.NumberFormat = "$#,##0"
		End With
		
		Call columnas_entremes("Comentarios Sv.", "", "#,##0", num_ocurrencias + mes_del_periodo, True, 1, fila_tabla)
		
		Call columnas_entremes("Expuestos", "=IFERROR(SUMIFS(Aux_Totales!C" & col_auxtot("expuestos") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &"""), 0) ", "#,##0", num_ocurrencias + mes_del_periodo, False, 2, fila_tabla)
		
		'NUEVA OCURRENCIA PLATA
		ws.Range(ws.Cells(fila_tabla - sep_tabla + num_ocurrencias, 4), ws.Cells(fila_tabla - sep_tabla + num_ocurrencias + mes_del_periodo - 1, 4)).FormulaR1C1 = "=IF(RC[1] = ""Mantener"", RC[8], IFERROR(R[" & num_ocurrencias + sep_tabla & "]C * R[" & num_ocurrencias + sep_tabla & "]C[6] * R[" & num_ocurrencias + sep_tabla & "]C[9],0))"
		
	Else
		
		With ws.Cells(fila_tabla, 1)
			.PasteSpecial Paste : = xlPasteValues
			.PasteSpecial Paste : = xlPasteFormats
		End With
		
		Call columnas_entremes("Pagos", _
			"=IF(R6C3 = ""Severidad"", " & _
			"IFERROR(SUMIFS(Aux_Totales!C" & col_auxtot("conteo_pago") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &""") / SUMIFS(Aux_Totales!C" & col_auxtot("expuestos") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &"""), 0), " & _
			"IFERROR(R[" & - sep_tabla & "]C / SUMIFS(Aux_Totales!C" & col_auxtot("conteo_pago") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &"""), 0))", _
			"", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
		Call columnas_entremes("Incurridos", _
			"=IF(R6C3 = ""Severidad"", " & _
			"IFERROR(SUMIFS(Aux_Totales!C" & col_auxtot("conteo_incurrido") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &""") / SUMIFS(Aux_Totales!C" & col_auxtot("expuestos") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &"""), 0), " & _
			"IFERROR(R[" & - sep_tabla & "]C / SUMIFS(Aux_Totales!C" & col_auxtot("conteo_incurrido") & ", Aux_Totales!C" & col_auxtot("periodo_ocurrencia") & ", RC1, Aux_Totales!C" & col_auxtot("apertura_reservas") & ", """& apertura &"""), 0))", _
			"", num_ocurrencias + mes_del_periodo, False, 1, fila_tabla)
		
		Call columnas_entremes("Ultimate", _
			"=IF( R6C3 = ""Severidad"", " & _
			"IFERROR(SUMIFS(Aux_Anterior!C" & col_auxant("Suma de frec_ultimate") & ", Aux_Anterior!C" & col_auxant("periodo_ocurrencia") & ", RC1, Aux_Anterior!C" & col_aperturas_ant & ", """& apertura &""", Aux_Anterior!C" & col_mes_corte_ant & ", " & mes_anterior() & "), 0), " & _
			"IFERROR(SUMIFS(Aux_Anterior!C" & col_auxant("Suma de seve_ultimate_" & atributo) & ", Aux_Anterior!C" & col_auxant("periodo_ocurrencia") & ", RC1, Aux_Anterior!C" & col_aperturas_ant & ", """& apertura &""", Aux_Anterior!C" & col_mes_corte_ant & ", " & mes_anterior() & "), 0))", _
			"", num_ocurrencias + mes_del_periodo, True, 1, fila_tabla)
		
		'NUEVA OCURRENCIA FREC/SEVE
		With ws.Range(ws.Cells(fila_tabla + num_ocurrencias, 4), ws.Cells(fila_tabla + num_ocurrencias + mes_del_periodo - 1, 4))
			.FormulaR1C1 = "=R" & fila_tabla + num_ocurrencias - 1 & "C * IF(R6C3 = ""Severidad"", 1 / " & num_meses_periodo & ", 1) "
			.Font.Bold = True
			.Interior.ThemeColor = xlThemeColorAccent4
			.Interior.TintAndShade = 0.799981688894314
		End With
		
		Call columnas_entremes("Comentarios", "", "#,##0", num_ocurrencias + mes_del_periodo, True, 1, fila_tabla)
		
		ws.Cells(fila_tabla, 2).FormulaR1C1 = "=IF(R6C3=""Frecuencia"", ""Severidad"", ""Frecuencia"") & "" Pagos"" "
		ws.Cells(fila_tabla, 3).FormulaR1C1 = "=IF(R6C3=""Frecuencia"", ""Severidad"", ""Frecuencia"") & "" Incurridos"" "
		ws.Cells(fila_tabla, 4).FormulaR1C1 = "=IF(R6C3=""Frecuencia"", ""Severidad"", ""Frecuencia"") & "" Ultimate"" "
		ws.Cells(fila_tabla, 5).FormulaR1C1 = "=""Comentarios"" & IF(R6C3=""Frecuencia"", "" Sv."", "" Fr."")"
		
		'NUEVA OCURRENCIA PLATA
		With ws.Range(ws.Cells(fila_tabla - sep_tabla + num_ocurrencias, 4), ws.Cells(fila_tabla - sep_tabla + num_ocurrencias + mes_del_periodo - 1, 4))
			.FormulaR1C1 = "=IF(RC[1] = ""Mantener"", RC[8], IFERROR(RC" & col_prim & " * RC" & col_prim + 1 & ", 0))"
			.Font.Bold = True
			.Interior.ThemeColor = xlThemeColorAccent4
			.Interior.TintAndShade = 0.799981688894314
		End With
		
		With ws.Range(ws.Cells(fila_tabla - sep_tabla + num_ocurrencias, 7), ws.Cells(fila_tabla - sep_tabla + num_ocurrencias + mes_del_periodo - 1, 7))
			.FormulaR1C1 = "=AVERAGE(R" & fila_tabla - sep_tabla + num_ocurrencias - 2 & "C:R" & fila_tabla - sep_tabla + num_ocurrencias - 1 & "C)"
			.Font.Bold = True
			.Interior.ThemeColor = xlThemeColorAccent4
			.Interior.TintAndShade = 0.799981688894314
			.NumberFormat = "0.00%"
		End With
		
	End If
	
	Application.ScreenUpdating = True
	Application.Calculation = xlCalculationAutomatic
	
End Sub
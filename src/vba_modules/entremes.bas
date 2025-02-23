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



Function crear_columna_entremes(nombre, formula, formato, modificable, num_filas) As Integer
	
	Set ws = ws_entremes()
	
	num_col = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column + 1
	ws.Cells(1, num_col).value = nombre
	
	With ws.Range(ws.Cells(2, num_col), ws.Cells(num_filas + 1, num_col))
		.Formula2R1C1 = formula
		.NumberFormat = formato
		
		If modificable Then
			.Interior.ThemeColor = xlThemeColorAccent4
			.Interior.TintAndShade = 0.799981688894314
		End If
		
	End With

	crear_columna_entremes = num_col

End Function



Sub preparar_Plantilla_Entremes(num_filas)

	' Application.ScreenUpdating = False
	Application.Calculation = xlCalculationManual
	
	Set ws = ws_entremes()

	col_expuestos = col_entremes("expuestos")
	col_prima_bruta_devengada = col_entremes("prima_bruta_devengada")
	col_prima_retenida_devengada = col_entremes("prima_retenida_devengada")

	col_frec_ultimate_anterior = col_entremes("frec_ultimate_anterior")
	col_seve_ultimate_bruto_anterior = col_entremes("seve_ultimate_bruto_anterior")
	col_seve_ultimate_retenido_anterior = col_entremes("seve_ultimate_retenido_anterior")
	col_ultimate_bruto_actuarial_anterior = col_entremes("plata_ultimate_bruto_anterior")
	col_ultimate_retenido_actuarial_anterior = col_entremes("plata_ultimate_retenido_anterior")

	col_ultimate_bruto_actuarial = crear_columna_entremes("plata_ultimate_bruto", "=IF(RC1 <> R[1]C1, RC" & col_prima_bruta_devengada & " * R[-1]C[2], RC" & col_ultimate_bruto_actuarial_anterior &")", "$#,##0", True, num_filas)
	col_ultimate_retenido_actuarial = crear_columna_entremes("plata_ultimate_retenido", "=IF(RC1 <> R[1]C1, RC" & col_prima_retenida_devengada & " * R[-1]C[2], RC" & col_ultimate_retenido_actuarial_anterior &")", "$#,##0", True, num_filas)
	col_pct_sue_bruto = crear_columna_entremes("pct_sue_bruto", "=IFERROR(RC" & col_ultimate_bruto_actuarial & " / RC" & col_prima_bruta_devengada & ", 0)", "0.00%", False, num_filas)
	col_pct_sue_retenido = crear_columna_entremes("pct_sue_retenido", "=IFERROR(RC" & col_ultimate_retenido_actuarial & " / RC" & col_prima_retenida_devengada & ", 0)", "0.00%", False, num_filas)

	col_ajuste_bruto = crear_columna_entremes("ajuste_bruto", "=RC" & col_ultimate_bruto_actuarial & " - RC" & col_ultimate_bruto_actuarial_anterior & "", "$#,##0", True, num_filas)

	''' METODOLOGIA 1: COMPLETAR DIAGONAL
	col_ultimate_bruto_metodologia_1_pago = crear_columna_entremes("ultimate_bruto_completar_diagonal_pago", "=0", "$#,##0", False, num_filas)
	col_ultimate_bruto_metodologia_1_incurrido = crear_columna_entremes("ultimate_bruto_completar_diagonal_incurrido", "=0", "$#,##0", False, num_filas)
	col_ultimate_retenido_metodologia_1_pago = crear_columna_entremes("ultimate_retenido_completar_diagonal_pago", "=0", "$#,##0", False, num_filas)
	col_ultimate_retenido_metodologia_1_incurrido = crear_columna_entremes("ultimate_retenido_completar_diagonal_incurrido", "=0", "$#,##0", False, num_filas)
	
	' ''' ALERTA
	' Call columnas_entremes("Alerta", "=IFERROR(IF(ABS(RC[14] / RC[3] - 1) > 0.05, ""Completar diagonal ajusta mas de 5%"", """"), """")", "#,##0", num_ocurrencias, False, 1, fila_tabla)
	' Call columnas_entremes("Comentarios", "", "", num_ocurrencias + mes_del_periodo, True, 1, fila_tabla)

	''' METODOLOGIA 2: BORNHUETTER-FERGUSON
	col_pct_sue_bf = crear_columna_entremes("pct_sue_bornhuetter_ferguson", "=0", "0.00%", True, num_filas)
	col_ultimate_bruto_metodologia_2_pago = crear_columna_entremes("ultimate_bruto_bornhuetter_ferguson_pago", "=0", "$#,##0", False, num_filas)
	col_ultimate_bruto_metodologia_2_incurrido = crear_columna_entremes("ultimate_bruto_bornhuetter_ferguson_incurrido", "=0", "$#,##0", False, num_filas)
	col_ultimate_retenido_metodologia_2_pago = crear_columna_entremes("ultimate_retenido_bornhuetter_ferguson_pago", "=0", "$#,##0", False, num_filas)
	col_ultimate_retenido_metodologia_2_incurrido = crear_columna_entremes("ultimate_retenido_bornhuetter_ferguson_incurrido", "=0", "$#,##0", False, num_filas)
	
	'''FRECUENCIA Y SEVERIDAD
	col_frec_ultimate = crear_columna_entremes("frec_ultimate", "=IF(RC1 <> R[1]C1, R[-1]C, RC" & col_frec_ultimate_anterior & ")", "0.00%", True, num_filas)
	col_seve_ultimate_bruto = crear_columna_entremes("seve_ultimate_bruto", "=IFERROR(RC" & col_ultimate_bruto_actuarial & " / (RC" & col_frec_ultimate & " * RC" & col_expuestos & "), 0)", "$#,##0", True, num_filas)
	col_seve_ultimate_retenido = crear_columna_entremes("seve_ultimate_retenido", "=IFERROR(RC" & col_ultimate_retenido_actuarial & " / (RC" & col_frec_ultimate & " * RC" & col_expuestos & "), 0)", "$#,##0", True, num_filas)

	''' AJUSTE PARCIAL
	col_pct_ajuste_gradual = crear_columna_entremes("pct_ajuste_gradual", "=0", "0.00%", True, num_filas)
	col_ultimate_bruto_contable = crear_columna_entremes("plata_ultimate_bruto_contable", "=0", "$#,##0", False, num_filas)
	col_ultimate_retenido_contable = crear_columna_entremes("plata_ultimate_retenido_contable", "=0", "$#,##0", False, num_filas)
	
	Application.ScreenUpdating = True
	Application.Calculation = xlCalculationAutomatic
	
End Sub
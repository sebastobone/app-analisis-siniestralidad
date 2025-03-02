Sub PrepararEntremes(num_filas)

	Application.ScreenUpdating = False
	Application.Calculation = xlCalculationManual
	
	Set ws = ws_entremes()

	col_pago_bruto = obtener_numero_columna(ws_entremes, "pago_bruto")
	col_pago_retenido = obtener_numero_columna(ws_entremes, "pago_retenido")
	col_incurrido_bruto = obtener_numero_columna(ws_entremes, "incurrido_bruto")
	col_incurrido_retenido = obtener_numero_columna(ws_entremes, "incurrido_retenido")

	col_expuestos = obtener_numero_columna(ws_entremes, "expuestos")
	col_prima_bruta_devengada = obtener_numero_columna(ws_entremes, "prima_bruta_devengada")
	col_prima_retenida_devengada = obtener_numero_columna(ws_entremes, "prima_retenida_devengada")

	col_frecuencia_ultimate_anterior = obtener_numero_columna(ws_entremes, "frecuencia_ultimate_anterior")
	col_severidad_ultimate_bruto_anterior = obtener_numero_columna(ws_entremes, "severidad_ultimate_bruto_anterior")
	col_severidad_ultimate_retenido_anterior = obtener_numero_columna(ws_entremes, "severidad_ultimate_retenido_anterior")
	col_ultimate_bruto_actuarial_anterior = obtener_numero_columna(ws_entremes, "plata_ultimate_bruto_anterior")
	col_ultimate_bruto_contable_anterior = obtener_numero_columna(ws_entremes, "plata_ultimate_contable_bruto_anterior")
	col_ultimate_retenido_actuarial_anterior = obtener_numero_columna(ws_entremes, "plata_ultimate_retenido_anterior")
	col_ultimate_retenido_contable_anterior = obtener_numero_columna(ws_entremes, "plata_ultimate_contable_retenido_anterior")

	col_factor_completitud_incurrido_retenido = obtener_numero_columna(ws_entremes, "factor_completitud_incurrido_retenido")

	inicio_columnas_calculadas = col_factor_completitud_incurrido_retenido

	col_ultimate_bruto_actuarial = crear_columna(ws, 1, inicio_columnas_calculadas + 1, "plata_ultimate_bruto", "=IF(RC1 <> R[1]C1, RC" & col_prima_bruta_devengada & " * R[-1]C[2], RC" & col_ultimate_bruto_actuarial_anterior &")", formato_plata(), True, num_filas, azul_oscuro(), blanco())
	col_ultimate_retenido_actuarial = crear_columna(ws, 1, inicio_columnas_calculadas + 2, "plata_ultimate_retenido", "=IF(RC1 <> R[1]C1, RC" & col_prima_retenida_devengada & " * R[-1]C[2], RC" & col_ultimate_retenido_actuarial_anterior &")", formato_plata(), True, num_filas, azul_claro(), blanco())
	col_pct_sue_bruto = crear_columna(ws, 1, inicio_columnas_calculadas + 3, "pct_sue_bruto", "=IFERROR(RC" & col_ultimate_bruto_actuarial & " / RC" & col_prima_bruta_devengada & ", 0)", formato_porcentaje(), False, num_filas, azul_oscuro(), blanco())
	col_pct_sue_retenido = crear_columna(ws, 1, inicio_columnas_calculadas + 4, "pct_sue_retenido", "=IFERROR(RC" & col_ultimate_retenido_actuarial & " / RC" & col_prima_retenida_devengada & ", 0)", formato_porcentaje(), False, num_filas, azul_claro(), blanco())

	col_ajuste_bruto = crear_columna(ws, 1, inicio_columnas_calculadas + 5, "ajuste_bruto", "=RC" & col_ultimate_bruto_actuarial & " - RC" & col_ultimate_bruto_actuarial_anterior & "", formato_plata(), False, num_filas, amarillo_oscuro(), blanco())
	col_ajuste_retenido = crear_columna(ws, 1, inicio_columnas_calculadas + 6, "ajuste_retenido", "=RC" & col_ultimate_retenido_actuarial & " - RC" & col_ultimate_retenido_actuarial_anterior & "", formato_plata(), False, num_filas, amarillo_claro(), blanco())

	col_alerta_bruto = crear_columna(ws, 1, inicio_columnas_calculadas + 7, "alerta_bruto", "=IFERROR(IF(ABS(MAX(RC[2], RC[3]) / RC" & col_ultimate_bruto_actuarial & " - 1) > 0.05, ""Completar diagonal ajusta mas de 5%"", """"), """")", "@", False, num_filas, amarillo_oscuro(), blanco())
	col_alerta_retenido = crear_columna(ws, 1, inicio_columnas_calculadas + 8, "alerta_retenido", "=IFERROR(IF(ABS(MAX(RC[3], RC[4]) / RC" & col_ultimate_retenido_actuarial & " - 1) > 0.05, ""Completar diagonal ajusta mas de 5%"", """"), """")", "@", False, num_filas, amarillo_claro(), blanco())

	''' METODOLOGIA 1: COMPLETAR DIAGONAL
	col_ultimate_bruto_metodologia_1_pago = crear_columna(ws, 1, inicio_columnas_calculadas + 9, "ultimate_bruto_completar_diagonal_pago", "=IFERROR(RC" & col_pago_bruto & " / RC" & obtener_numero_columna(ws_entremes, "factor_completitud_pago_bruto") & " / R[-1]C" & obtener_numero_columna(ws_entremes, "velocidad_pago_bruto_triangulo") & " , """") ", formato_plata(), False, num_filas, violeta_oscuro(), blanco())
	col_ultimate_bruto_metodologia_1_incurrido = crear_columna(ws, 1, inicio_columnas_calculadas + 10, "ultimate_bruto_completar_diagonal_incurrido", "=IFERROR(RC" & col_incurrido_bruto & " / RC" & obtener_numero_columna(ws_entremes, "factor_completitud_incurrido_bruto") & " / R[-1]C" & obtener_numero_columna(ws_entremes, "velocidad_incurrido_bruto_triangulo") & " , """") ", formato_plata(), False, num_filas, violeta_oscuro(), blanco())
	col_ultimate_retenido_metodologia_1_pago = crear_columna(ws, 1, inicio_columnas_calculadas + 11, "ultimate_retenido_completar_diagonal_pago", "=IFERROR(RC" & col_pago_retenido & " / RC" & obtener_numero_columna(ws_entremes, "factor_completitud_pago_retenido") & " / R[-1]C" & obtener_numero_columna(ws_entremes, "velocidad_pago_retenido_triangulo") & " , """") ", formato_plata(), False, num_filas, violeta_claro(), blanco())
	col_ultimate_retenido_metodologia_1_incurrido = crear_columna(ws, 1, inicio_columnas_calculadas + 12, "ultimate_retenido_completar_diagonal_incurrido", "=IFERROR(RC" & col_incurrido_retenido & " / RC" & obtener_numero_columna(ws_entremes, "factor_completitud_incurrido_retenido") & " / R[-1]C" & obtener_numero_columna(ws_entremes, "velocidad_incurrido_retenido_triangulo") & " , """") ", formato_plata(), False, num_filas, violeta_claro(), blanco())
	
	''' METODOLOGIA 2: BORNHUETTER-FERGUSON
	col_pct_sue_bf_bruto = crear_columna(ws, 1, inicio_columnas_calculadas + 13, "pct_sue_bornhuetter_ferguson_bruto", "=RC" & col_pct_sue_bruto & "", formato_porcentaje(), True, num_filas, naranja_oscuro(), blanco())
	col_ultimate_bruto_metodologia_2_pago = crear_columna(ws, 1, inicio_columnas_calculadas + 14, "ultimate_bruto_bornhuetter_ferguson_pago", "=RC" & col_pago_bruto & " + RC" & col_prima_bruta_devengada & " * RC" & col_pct_sue_bf_bruto & " * (1 - (RC" & obtener_numero_columna(ws_entremes, "factor_completitud_pago_bruto") & " * R[-1]C" & obtener_numero_columna(ws_entremes, "velocidad_pago_bruto_triangulo") & ")) ", formato_plata(), False, num_filas, naranja_oscuro(), blanco())
	col_ultimate_bruto_metodologia_2_incurrido = crear_columna(ws, 1, inicio_columnas_calculadas + 15, "ultimate_bruto_bornhuetter_ferguson_incurrido", "=RC" & col_incurrido_bruto & " + RC" & col_prima_bruta_devengada & " * RC" & col_pct_sue_bf_bruto & " * (1 - (RC" & obtener_numero_columna(ws_entremes, "factor_completitud_incurrido_bruto") & " * R[-1]C" & obtener_numero_columna(ws_entremes, "velocidad_incurrido_bruto_triangulo") & ")) ", formato_plata(), False, num_filas, naranja_oscuro(), blanco())
	col_pct_sue_bf_retenido = crear_columna(ws, 1, inicio_columnas_calculadas + 16, "pct_sue_bornhuetter_ferguson_retenido", "=RC" & col_pct_sue_retenido & "", formato_porcentaje(), True, num_filas, naranja_claro(), blanco())
	col_ultimate_retenido_metodologia_2_pago = crear_columna(ws, 1, inicio_columnas_calculadas + 17, "ultimate_retenido_bornhuetter_ferguson_pago", "=RC" & col_pago_retenido & " + RC" & col_prima_retenida_devengada & " * RC" & col_pct_sue_bf_retenido & " * (1 - (RC" & obtener_numero_columna(ws_entremes, "factor_completitud_pago_retenido") & " * R[-1]C" & obtener_numero_columna(ws_entremes, "velocidad_pago_retenido_triangulo") & ")) ", formato_plata(), False, num_filas, naranja_oscuro(), blanco())
	col_ultimate_retenido_metodologia_2_incurrido = crear_columna(ws, 1, inicio_columnas_calculadas + 18, "ultimate_retenido_bornhuetter_ferguson_incurrido", "=RC" & col_incurrido_retenido & " + RC" & col_prima_bruta_devengada & " * RC" & col_pct_sue_bf_retenido & " * (1 - (RC" & obtener_numero_columna(ws_entremes, "factor_completitud_incurrido_retenido") & " * R[-1]C" & obtener_numero_columna(ws_entremes, "velocidad_incurrido_retenido_triangulo") & ")) ", formato_plata(), False, num_filas, naranja_oscuro(), blanco())
	
	'''FRECUENCIA Y SEVERIDAD
	col_frecuencia_ultimate = crear_columna(ws, 1, inicio_columnas_calculadas + 19, "frecuencia_ultimate", "=IF(RC1 <> R[1]C1, R[-1]C, RC" & col_frecuencia_ultimate_anterior & ")", formato_porcentaje(), True, num_filas, cian_claro(), blanco())
	col_severidad_ultimate_bruto = crear_columna(ws, 1, inicio_columnas_calculadas + 20, "severidad_ultimate_bruto", "=IFERROR(RC" & col_ultimate_bruto_actuarial & " / (RC" & col_frecuencia_ultimate & " * RC" & col_expuestos & "), 0)", formato_plata(), True, num_filas, verde_oscuro(), blanco())
	col_severidad_ultimate_retenido = crear_columna(ws, 1, inicio_columnas_calculadas + 21, "severidad_ultimate_retenido", "=IFERROR(RC" & col_ultimate_retenido_actuarial & " / (RC" & col_frecuencia_ultimate & " * RC" & col_expuestos & "), 0)", formato_plata(), True, num_filas, verde_claro(), blanco())

	''' AJUSTE PARCIAL
	col_pct_ajuste_gradual_bruto = crear_columna(ws, 1, inicio_columnas_calculadas + 22, "pct_ajuste_gradual_bruto", "=1", formato_porcentaje(), True, num_filas, azul_oscuro(), blanco())
	col_pct_ajuste_gradual_retenido = crear_columna(ws, 1, inicio_columnas_calculadas + 23, "pct_ajuste_gradual_retenido", "=1", formato_porcentaje(), True, num_filas, azul_claro(), blanco())
	col_ultimate_bruto_contable = crear_columna(ws, 1, inicio_columnas_calculadas + 24, "plata_ultimate_contable_bruto", "=RC" & col_ultimate_bruto_contable_anterior & " + (RC" & col_ultimate_bruto_actuarial & " - RC" & col_ultimate_bruto_contable_anterior & ") * RC" & col_pct_ajuste_gradual_bruto & " ", formato_plata(), False, num_filas, azul_oscuro(), blanco())
	col_ultimate_retenido_contable = crear_columna(ws, 1, inicio_columnas_calculadas + 25, "plata_ultimate_contable_retenido", "=RC" & col_ultimate_retenido_contable_anterior & " + (RC" & col_ultimate_retenido_actuarial & " - RC" & col_ultimate_retenido_contable_anterior & ") * RC" & col_pct_ajuste_gradual_retenido & " ", formato_plata(), False, num_filas, azul_claro(), blanco())
	
	Application.ScreenUpdating = True
	Application.Calculation = xlCalculationAutomatic
	
End Sub


Sub VincularUltimatesEntremes()

	num_filas = ws_resumen.Cells(Rows.Count, 1).End(xlUp).Row

	Call crear_columna(ws_resumen, 1, obtener_numero_columna(ws_resumen, "frecuencia_ultimate"), "frecuencia_ultimate", "=Entremes!RC" & obtener_numero_columna(ws_entremes, "frecuencia_ultimate") & " ", formato_porcentaje(), False, num_filas, cian_claro(), blanco())
	Call crear_columna(ws_resumen, 1, obtener_numero_columna(ws_resumen, "severidad_ultimate_bruto"), "severidad_ultimate_bruto", "=Entremes!RC" & obtener_numero_columna(ws_entremes, "severidad_ultimate_bruto") & " ", formato_plata(), False, num_filas, verde_oscuro(), blanco())
	Call crear_columna(ws_resumen, 1, obtener_numero_columna(ws_resumen, "severidad_ultimate_retenido"), "severidad_ultimate_retenido", "=Entremes!RC" & obtener_numero_columna(ws_entremes, "severidad_ultimate_retenido") & " ", formato_plata(), False, num_filas, verde_claro(), blanco())
	Call crear_columna(ws_resumen, 1, obtener_numero_columna(ws_resumen, "plata_ultimate_bruto"), "plata_ultimate_bruto", "=Entremes!RC" & obtener_numero_columna(ws_entremes, "plata_ultimate_bruto") & " ", formato_plata(), False, num_filas, azul_oscuro(), blanco())
	Call crear_columna(ws_resumen, 1, obtener_numero_columna(ws_resumen, "plata_ultimate_contable_bruto"), "plata_ultimate_contable_bruto", "=Entremes!RC" & obtener_numero_columna(ws_entremes, "plata_ultimate_contable_bruto") & " ", formato_plata(), False, num_filas, azul_oscuro(), blanco())
	Call crear_columna(ws_resumen, 1, obtener_numero_columna(ws_resumen, "plata_ultimate_retenido"), "plata_ultimate_retenido", "=Entremes!RC" & obtener_numero_columna(ws_entremes, "plata_ultimate_retenido") & " ", formato_plata(), False, num_filas, azul_claro(), blanco())
	Call crear_columna(ws_resumen, 1, obtener_numero_columna(ws_resumen, "plata_ultimate_contable_retenido"), "plata_ultimate_contable_retenido", "=Entremes!RC" & obtener_numero_columna(ws_entremes, "plata_ultimate_contable_retenido") & " ", formato_plata(), False, num_filas, azul_claro(), blanco())

End Sub


Sub generar_Completar_Diagonal(num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas, apertura, atributo, mes_del_periodo)

	Application.ScreenUpdating = False
	Application.Calculation = xlCalculationManual
	
	Set ws = ws_completar_diagonal()

	Call color_columnas_triangulo(ws, fila_ini_plantillas, col_ocurrs_plantillas, num_alturas)
	Call estructura_factores(ws, num_ocurrencias, num_alturas, header_triangulos, sep_triangulos, fila_ini_plantillas, col_ocurrs_plantillas)

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
	
	With ws.Cells(fila_factor_compl, col_ocurrs_plantillas)
		.Interior.ThemeColor = xlThemeColorAccent1
        .Interior.TintAndShade = 0.799981688894314
		.value = "FACTOR COMPLETITUD DIAGONAL"
	End With
	
	fila_tabla = fila_factor_compl + sep_triangulos + 1
	
	ws.Activate
	With ws.Cells(fila_tabla, 1)
		.Interior.Color = azul_claro()
		.Font.Color = blanco()
		.Font.Bold = True
		.value = "ocurrencia"
	End With
	ws.Range(ws.Cells(fila_tabla + 1, 1), ws.Cells(fila_tabla + num_ocurrencias, 1)).value = ws.Range(ws.Cells(header_triangulos + fila_ini_plantillas, col_ocurrs_plantillas), ws.Cells(header_triangulos + fila_ini_plantillas + num_ocurrencias - 1, col_ocurrs_plantillas)).value
	
	col_pagos = crear_columna(ws, fila_tabla, 2, "pago", "=INDEX(R" & header_triangulos + fila_ini_plantillas & "C" & col_ocurrs_plantillas + 1 & ":R" & header_triangulos + fila_ini_plantillas + num_ocurrencias - 1 & "C" & col_ocurrs_plantillas + num_alturas & ", COUNTA(R" & fila_tabla + 1 & "C[-1]:RC[-1]), " & num_alturas & " - (COUNTA(R" & fila_tabla + 1 & "C[-1]:RC[-1]) - 1) * " & num_meses_periodo & ")", formato_plata(), False, num_ocurrencias, azul_claro(), blanco())
	col_incurridos = crear_columna(ws, fila_tabla, 3, "incurrido", "=INDEX(R" & header_triangulos + fila_ini_plantillas & "C" & col_ocurrs_plantillas + num_alturas + 1 & ":R" & header_triangulos + fila_ini_plantillas + num_ocurrencias - 1 & "C" & col_ocurrs_plantillas + num_alturas * 2 & ", COUNTA(R" & fila_tabla + 1 & "C[-1]:RC[-1]), " & num_alturas & " - (COUNTA(R" & fila_tabla + 1 & "C[-1]:RC[-1]) - 1) * " & num_meses_periodo & ")", formato_plata(), False, num_ocurrencias, azul_claro(), blanco())
	col_factor_completitud_pago = crear_columna(ws, fila_tabla, 4, "factor_completitud_pago", "=IFERROR(INDEX(R" & fila_tabla - 3 & "C" & col_ocurrs_plantillas + 1 & ":R" & fila_tabla - 3 & "C" & col_ocurrs_plantillas + num_alturas & ", 1, (COUNTA(R" & fila_tabla + num_ocurrencias & "C1:RC1) - 1) * " & num_meses_periodo & " + " & mes_del_periodo & "), 100%)", "0.00%", False, num_ocurrencias, azul_claro(), blanco())
	col_factor_completitud_incurrido = crear_columna(ws, fila_tabla, 5,"factor_completitud_incurrido", "=IFERROR(INDEX(R" & fila_tabla - 3 & "C" & col_ocurrs_plantillas + num_alturas + 1 & ":R" & fila_tabla - 3 & "C" & col_ocurrs_plantillas + num_alturas * 2 & ", 1, (COUNTA(R" & fila_tabla + num_ocurrencias & "C1:RC1) - 1) * " & num_meses_periodo & " + " & mes_del_periodo & "), 100%)", "0.00%", False, num_ocurrencias, azul_claro(), blanco())
	col_pagos_completos = crear_columna(ws, fila_tabla, 6, "pago_completo", "=RC" & col_pagos & " / RC" & col_factor_completitud_pago & "", formato_plata(), False, num_ocurrencias, azul_claro(), blanco())
	col_incurridos_completos = crear_columna(ws, fila_tabla, 7, "incurrido_completo", "=RC" & col_incurridos & " / RC" & col_factor_completitud_incurrido & "", formato_plata(), False, num_ocurrencias, azul_claro(), blanco())

	Application.ScreenUpdating = True
	Application.Calculation = xlCalculationAutomatic

End Sub
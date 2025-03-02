Function GraficarUltimate(ws, num_ocurrencias, nombre_cantidad)
	
	fila_tabla = WorksheetFunction.Match("ocurrencia", ws.Range("A:A"), 0)

	Set chartRange = ws.Range(ws.Cells(fila_tabla + 2, 2), ws.Cells(fila_tabla + num_ocurrencias, 4))
	Set chartObj = ws.ChartObjects.Add(Left:=ws.Cells(fila_tabla - 25, 1).Left, Top:=ws.Cells(fila_tabla - 25, 1).Top, Width:=500, Height:=300)

	With chartObj.Chart
		.ChartType = xlLine
		.SetSourceData Source:=chartRange
		.SeriesCollection(1).XValues = ws.Range(ws.Cells(fila_tabla + 2, 1), ws.Cells(fila_tabla + num_ocurrencias, 1))
		.HasLegend = False
		.Axes(xlValue).MajorGridLines.Delete
		.ChartArea.Format.Line.Visible = msoFalse
		.HasTitle = True
		.ChartTitle.Text = nombre_cantidad
	End With
	
End Function



Function grafica_analisis_factores()
	
	Dim grafFact As Shape

	current_sheet = ActiveSheet.Name
	fila_ratios = WorksheetFunction.Match("Ratios", Range("F:F"), 0)
	fila_exclusiones = WorksheetFunction.Match("Exclusiones", Range("F:F"), 0)
	fila_fact_sel = WorksheetFunction.Match("FACTORES SELECCIONADOS", Range("F:F"), 0)
	fila_evo = WorksheetFunction.Match("Evolucion", Range("F:F"), 0)

	Cells(fila_exclusiones - 3, 1).value = "Periodos"
	Cells(fila_exclusiones - 2, 1).value = "Altura"
	Cells(fila_exclusiones - 3, 2).value = num_ocurrencias - 1
	Cells(fila_exclusiones - 2, 2).value = 0

	fila_tabla = fila_evo + sep_triangulos + num_ocurrencias * 2 + 4
	
	Cells(fila_tabla, 1).Formula2R1C1 = "=+FILTER(R" & fila_ratios + 3 & "C6:R" & fila_ratios + 3 + num_ocurrencias - 1 & "C6,R" & fila_ratios + 3 & "C5:R" & fila_ratios + 3 + num_ocurrencias - 1 & "C5<=( R" & fila_exclusiones - 3 & "C2 + R" & fila_exclusiones - 2 & "C2 ),0)"
	Cells(fila_tabla, 2).Formula2R1C1 = "=+FILTER(FILTER(IF(R4C3 = ""Pago"", R" & fila_ratios + 3 & "C7:R" & fila_ratios + 3 + num_ocurrencias - 1 & "C" & 6 + num_alturas & ", R" & fila_ratios + 3 & "C" & 7 + num_alturas & ":R" & fila_ratios + 3 + num_ocurrencias - 1 & "C" & 6 + num_alturas * 2 & "), IF(R4C3 = ""Pago"", R" & fila_ratios + 2 & "C7:R" & fila_ratios + 2 & "C" & 6 + num_alturas & ", R" & fila_ratios + 2 & "C" & 7 + num_alturas & ":R" & fila_ratios + 2 & "C" & 6 + num_alturas * 2 & ") = R" & fila_exclusiones - 2 & "C2), R" & fila_ratios + 3 & "C5:R" & fila_ratios + 3 + num_ocurrencias - 1 & "C5<=( R" & fila_exclusiones - 3 & "C2 + R" & fila_exclusiones - 2 & "C2 ))"
	Cells(fila_tabla, 3).Formula2R1C1 = "=+FILTER(FILTER(IF(R4C3 = ""Pago"", R" & fila_exclusiones + 3 & "C7:R" & fila_exclusiones + 3 + num_ocurrencias - 1 & "C" & 6 + num_alturas & ", R" & fila_exclusiones + 3 & "C" & 7 + num_alturas & ":R" & fila_exclusiones + 3 + num_ocurrencias - 1 & "C" & 6 + num_alturas * 2 & "), IF(R4C3 = ""Pago"", R" & fila_exclusiones + 2 & "C7:R" & fila_exclusiones + 2 & "C" & 6 + num_alturas & ", R" & fila_exclusiones + 2 & "C" & 7 + num_alturas & ":R" & fila_exclusiones + 2 & "C" & 6 + num_alturas * 2 & ") = R" & fila_exclusiones - 2 & "C2), R" & fila_ratios + 3 & "C5:R" & fila_ratios + 3 + num_ocurrencias - 1 & "C5<=( R" & fila_exclusiones - 3 & "C2 + R" & fila_exclusiones - 2 & "C2 ))"
	Cells(fila_tabla, 4).Formula2R1C1 = "=+FILTER(R" & fila_tabla & "C1:R" & fila_tabla + num_ocurrencias - 1 & "C1, R" & fila_tabla & "C3:R" & fila_tabla + num_ocurrencias - 1 & "C3 = 1)"
	Cells(fila_tabla, 5).Formula2R1C1 = "=+IFERROR(ROUND(FILTER(R" & fila_tabla & "C2:R" & fila_tabla + num_ocurrencias - 1 & "C2, R" & fila_tabla & "C3:R" & fila_tabla + num_ocurrencias - 1 & "C3 = 1), 3), """")"
	Cells(fila_tabla, 6).Formula2R1C1 = "=+PERCENTILE.EXC(R" & fila_tabla & "C5:R" & fila_tabla + num_ocurrencias - 1 & "C5, 0.2)"
	Cells(fila_tabla, 7).Formula2R1C1 = "=+PERCENTILE.EXC(R" & fila_tabla & "C5:R" & fila_tabla + num_ocurrencias - 1 & "C5, 0.8)"
	Cells(fila_tabla, 8).Formula2R1C1 = "=+RC6"
	Cells(fila_tabla, 9).Formula2R1C1 = "=+PERCENTILE.EXC(R" & fila_tabla & "C5:R" & fila_tabla + num_ocurrencias - 1 & "C5, 0.4) - RC[-1]"
	Cells(fila_tabla, 10).Formula2R1C1 = "=+PERCENTILE.EXC(R" & fila_tabla & "C5:R" & fila_tabla + num_ocurrencias - 1 & "C5, 0.6) - PERCENTILE.EXC(R" & fila_tabla & "C5:R" & fila_tabla + num_ocurrencias - 1 & "C5, 0.4) "
	Cells(fila_tabla, 11).Formula2R1C1 = "=+RC7 - PERCENTILE.EXC(R" & fila_tabla & "C5:R" & fila_tabla + num_ocurrencias - 1 & "C5, 0.6)"
	Cells(fila_tabla, 12).Formula2R1C1 = "=+FILTER(IF(R4C3 = ""Pago"", R" & fila_fact_sel & "C7:R" & fila_fact_sel & "C" & 6 + num_alturas & ", R" & fila_fact_sel & "C" & 7 + num_alturas & ":R" & fila_fact_sel & "C" & 6 + num_alturas * 2 & "), IF(R4C3 = ""Pago"", R" & fila_ratios + 2 & "C7:R" & fila_ratios + 2 & "C" & 6 + num_alturas & ", R" & fila_ratios + 2 & "C" & 7 + num_alturas & ":R" & fila_ratios + 2 & "C" & 6 + num_alturas * 2 & ") = R" & fila_exclusiones - 2 & "C2)"

	Range(Cells(fila_tabla, 6), Cells(fila_tabla, 12)).Copy
	Range(Cells(fila_tabla, 6), Cells(fila_tabla + num_ocurrencias - 1, 12)).PasteSpecial Paste : = xlPasteAll

	Set grafFact = ActiveSheet.Shapes.AddChart2(227, xlLine)

	grafFact.Chart.SetSourceData Source : = Range(Cells(fila_tabla, 5), Cells(fila_tabla + num_ocurrencias - 2, 12))

	grafFact.Chart.SetElement(msoElementChartTitleAboveChart)
	grafFact.Chart.ChartTitle.Text = "Analisis Factores Evolucion"
	grafFact.Chart.FullSeriesCollection(1).ChartType = xlLineMarkers
	grafFact.Chart.FullSeriesCollection(1).format.Line.ForeColor.RGB = RGB(0, 51, 160)
	grafFact.Chart.FullSeriesCollection(2).ChartType = xlLine
	grafFact.Chart.FullSeriesCollection(2).format.Line.ForeColor.RGB = RGB(255, 0, 0)
	grafFact.Chart.FullSeriesCollection(3).ChartType = xlLine
	grafFact.Chart.FullSeriesCollection(3).format.Line.ForeColor.RGB = RGB(255, 0, 0)
	grafFact.Chart.FullSeriesCollection(4).ChartType = xlAreaStacked
	grafFact.Chart.FullSeriesCollection(4).format.Fill.Visible = msoFalse
	grafFact.Chart.FullSeriesCollection(5).ChartType = xlAreaStacked
	grafFact.Chart.FullSeriesCollection(5).format.Fill.ForeColor.RGB = RGB(255, 210, 146)
	grafFact.Chart.FullSeriesCollection(6).ChartType = xlAreaStacked
	grafFact.Chart.FullSeriesCollection(6).format.Fill.ForeColor.RGB = RGB(255, 187, 91)
	grafFact.Chart.FullSeriesCollection(7).ChartType = xlAreaStacked
	grafFact.Chart.FullSeriesCollection(7).format.Fill.ForeColor.RGB = RGB(237, 139, 0)
	grafFact.Chart.FullSeriesCollection(8).ChartType = xlLine
	grafFact.Chart.FullSeriesCollection(8).format.Line.ForeColor.RGB = RGB(0, 255, 0)

	grafFact.Chart.SeriesCollection(1).XValues = Range(Cells(fila_tabla, 4), Cells(fila_tabla, 4).End(xlDown))
	
	grafFact.Chart.FullSeriesCollection(1).AxisGroup = 1
	grafFact.Chart.Axes(xlValue).MinimumScale = WorksheetFunction.Min(Range(Cells(fila_tabla, 5), Cells(fila_tabla, 5).End(xlDown)))
	grafFact.Chart.Axes(xlValue).MaximumScale = WorksheetFunction.Max(Range(Cells(fila_tabla, 5), Cells(fila_tabla, 5).End(xlDown)))
	grafFact.Chart.HasLegend = False

	grafFact.Left = Cells(fila_exclusiones + 1, 1).Left
	grafFact.Top = Cells(fila_exclusiones + 1, 1).Top

	grafFact.Name = "Anl_Fact_" + current_sheet

End Function



Sub actualizar_lims_graf()
	
	Application.ScreenUpdating = False
	
	current_sheet = ActiveSheet.Name
	fila_evo = WorksheetFunction.Match("Evolucion", Range("F:F"), 0)
	fila_tabla = WorksheetFunction.Match("Periodo", Range("A:A"), 0) + sep_triangulos + num_ocurrencias - 1
	fila_exclusiones = WorksheetFunction.Match("Exclusiones", Range("F:F"), 0)

	naltura = WorksheetFunction.CountA(Range(Cells(fila_tabla, 4), Cells(fila_tabla, 4).End(xlDown)))

	ActiveSheet.ChartObjects("Anl_Fact_" + current_sheet).Chart.Axes(xlValue).MinimumScale = WorksheetFunction.Min(Range(Cells(fila_tabla, 5), Cells(fila_tabla, 5).End(xlDown)))
	ActiveSheet.ChartObjects("Anl_Fact_" + current_sheet).Chart.Axes(xlValue).MaximumScale = WorksheetFunction.Max(Range(Cells(fila_tabla, 5), Cells(fila_tabla, 5).End(xlDown)))
	ActiveSheet.ChartObjects("Anl_Fact_" + current_sheet).Chart.SetSourceData Source : = Range(Cells(fila_tabla, 5), Cells(fila_tabla + WorksheetFunction.Min(naltura, num_ocurrencias) - 1, 12))
	ActiveSheet.ChartObjects("Anl_Fact_" + current_sheet).Chart.SeriesCollection(1).XValues = Range(Cells(fila_tabla, 4), Cells(fila_tabla, 4).End(xlDown))

End Sub



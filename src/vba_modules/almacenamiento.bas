Function guardar_ultimate(ws_name, num_ocurrencias, mes_del_periodo, nombre_col_ultimate, apertura, atributo)

    Set ws = Worksheets(ws_name)

    fila_tabla = WorksheetFunction.Match("Ultimate", ws.Range("D:D"), 0)
    ws.Range(ws.Cells(fila_tabla + 1, 4), ws.Cells(fila_tabla + num_ocurrencias + mes_del_periodo - 1, 4)).Copy

    fila_obj = WorksheetFunction.Match(apertura, ws_auxtot.Range("A:A"), 0)
    ws_auxtot.Cells(fila_obj, col_auxtot(nombre_col_ultimate)).PasteSpecial Paste:=xlPasteValues

    col_plata_ultimate = col_auxtot("plata_ultimate_" & atributo)
    col_plata_ultimate_contable = col_auxtot("plata_ultimate_contable_" & atributo)

    If ws_name = "Plantilla_Frec" Or ws_name = "Plantilla_Seve" Then
        ws_auxtot.Range(ws_auxtot.Cells(fila_obj, col_plata_ultimate), ws_auxtot.Cells(fila_obj + num_ocurrencias + mes_del_periodo - 2, col_plata_ultimate)).Formula2R1C1 = "=RC" & col_auxtot("conteo_ultimate") & " * RC" & col_auxtot("seve_ultimate_" & atributo) & ""
    End If

    If ws_name = "Plantilla_Entremes" Then
        col_ult_cont_entremes = WorksheetFunction.Match("Ultimate Contable", ws.Range(" " & fila_tabla & ":" & fila_tabla & " "), 0)
        ws.Range(ws.Cells(fila_tabla + 1, col_ult_cont_entremes), ws.Cells(fila_tabla + num_ocurrencias + mes_del_periodo - 1, col_ult_cont_entremes)).Copy
        ws_auxtot.Cells(fila_obj, col_plata_ultimate_contable).PasteSpecial Paste:=xlPasteValues

        If ws.Cells(3, 3) = "Frecuencia y Severidad" Or ws.Cells(4, 3) = "Severidad" Then
            fila_tabla = WorksheetFunction.Match("Frecuencia Ultimate", ws.Range("D:D"), 0)
            ws.Range(ws.Cells(fila_tabla + 1, 4), ws.Cells(fila_tabla + num_ocurrencias + mes_del_periodo - 1, 4)).Copy
            ws_auxtot.Cells(fila_obj, col_auxtot("frec_ultimate")).PasteSpecial Paste:=xlPasteValues
            ws_auxtot.Range(ws_auxtot.Cells(fila_obj, col_auxtot("seve_ultimate_" & atributo)), ws_auxtot.Cells(fila_obj + num_ocurrencias + mes_del_periodo - 2, col_auxtot("seve_ultimate_" & atributo))).FormulaR1C1 = "=IFERROR(RC" & col_auxtot("plata_ultimate_" & atributo) & " / RC" & col_auxtot("conteo_ultimate") & ", 0)"
        Else
            fila_tabla = WorksheetFunction.Match("Severidad Ultimate", ws.Range("D:D"), 0)
            ws.Range(ws.Cells(fila_tabla + 1, 4), ws.Cells(fila_tabla + num_ocurrencias + mes_del_periodo - 1, 4)).Copy
            ws_auxtot.Cells(fila_obj, col_auxtot("seve_ultimate_" & atributo)).PasteSpecial Paste:=xlPasteValues
            ws_auxtot.Range(ws_auxtot.Cells(fila_obj, col_auxtot("frec_ultimate")), ws_auxtot.Cells(fila_obj + num_ocurrencias + mes_del_periodo - 2, col_auxtot("frec_ultimate"))).FormulaR1C1 = "=IFERROR(RC" & col_auxtot("plata_ultimate_" & atributo) & " / RC" & col_auxtot("seve_ultimate_" & atributo) & " / RC" & col_auxtot("expuestos") & ", 0)"
        End If
    
    Else
        ws_auxtot.Range(ws_auxtot.Cells(fila_obj, col_plata_ultimate), ws_auxtot.Cells(fila_obj + num_ocurrencias + mes_del_periodo - 2, col_plata_ultimate)).Copy
        ws_auxtot.Cells(fila_obj, col_plata_ultimate_contable).PasteSpecial Paste:=xlPasteValues
    End If

End Function
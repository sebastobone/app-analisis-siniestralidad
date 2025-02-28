Function guardar_vector(hoja_origen, hoja_destino, apertura, atributo, nombre_columna_origen, nombre_columna_destino, num_filas)

    Set ws_origen = Worksheets(hoja_origen)
    Set ws_destino = Worksheets(hoja_destino)

    fila_tabla = WorksheetFunction.Match("Ocurrencia", ws_origen.Range("A:A"), 0)
    columna_origen = WorksheetFunction.Match(nombre_columna_origen, ws_origen.Range(" " & fila_tabla & ":" & fila_tabla & " "), 0)
    
    fila_destino = WorksheetFunction.Match(apertura, ws_destino.Range("A:A"), 0)
    columna_destino = WorksheetFunction.Match(nombre_columna_destino, ws_destino.Range("1:1"), 0)
    ws_destino.Range(ws_destino.Cells(fila_destino, columna_destino), ws_destino.Cells(fila_destino + num_filas - 1, columna_destino)).value = ws_origen.Range(ws_origen.Cells(fila_tabla + 1, columna_origen), ws_origen.Cells(fila_tabla + num_filas, columna_origen)).value

    col_plata_ultimate = col_auxtot("plata_ultimate_" & atributo)
    col_plata_ultimate_contable = col_auxtot("plata_ultimate_contable_" & atributo)
    col_frec_ultimate = col_auxtot("frec_ultimate")
    col_seve_ultimate = col_auxtot("seve_ultimate_" & atributo)

    If hoja_origen = "Plantilla_Frec" Or hoja_origen = "Plantilla_Seve" Then
        ws_auxtot.Range(ws_auxtot.Cells(fila_obj, col_plata_ultimate), ws_auxtot.Cells(fila_obj + num_filas - 1, col_plata_ultimate)).Formula2R1C1 = "=RC" & col_auxtot("conteo_ultimate") & " * RC" & col_auxtot("seve_ultimate_" & atributo) & ""
    End If

    If hoja_origen = "Plantilla_Plata" Then
        ws_auxtot.Range(ws_auxtot.Cells(fila_obj, col_frec_ultimate), ws_auxtot.Cells(fila_obj + num_filas - 1, col_frec_ultimate)).ClearContents
        ws_auxtot.Range(ws_auxtot.Cells(fila_obj, col_seve_ultimate), ws_auxtot.Cells(fila_obj + num_filas - 1, col_seve_ultimate)).ClearContents
    End If

End Function
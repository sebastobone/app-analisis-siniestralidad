Function GuardarVector(HojaOrigen, HojaDestino, Apertura, Atributo, NombreColumnaOrigen, NombreColumnaDestino, NumFilas)

    Set wsOrigen = Worksheets(HojaOrigen)
    Set wsDestino = Worksheets(HojaDestino)

    FilaOrigen = WorksheetFunction.Match("ocurrencia", wsOrigen.Range("A:A"), 0)
    ColumnaOrigen = WorksheetFunction.Match(NombreColumnaOrigen, wsOrigen.Range(" " & FilaOrigen & ":" & FilaOrigen & " "), 0)
    
    FilaDestino = WorksheetFunction.Match(Apertura, wsDestino.Range("A:A"), 0)
    ColumnaDestino = WorksheetFunction.Match(NombreColumnaDestino, wsDestino.Range("1:1"), 0)
    wsDestino.Range(wsDestino.Cells(FilaDestino, ColumnaDestino), wsDestino.Cells(FilaDestino + NumFilas - 1, ColumnaDestino)).value = wsOrigen.Range(wsOrigen.Cells(FilaOrigen + 1, ColumnaOrigen), wsOrigen.Cells(FilaOrigen + NumFilas, ColumnaOrigen)).value

    ColPlataUltimate = obtener_numero_columna(ws_resumen, "plata_ultimate_" & Atributo)
    ColPlataUltimateContable = obtener_numero_columna(ws_resumen, "plata_ultimate_contable_" & Atributo)
    ColFrecuenciaUltimate = obtener_numero_columna(ws_resumen, "frecuencia_ultimate")
    ColSeveridadUltimate = obtener_numero_columna(ws_resumen, "severidad_ultimate_" & Atributo)

    If HojaOrigen = "Frecuencia" Or HojaOrigen = "Severidad" Then
        ws_resumen.Range(ws_resumen.Cells(FilaDestino, ColPlataUltimate), ws_resumen.Cells(FilaDestino + NumFilas - 1, ColPlataUltimate)).Formula2R1C1 = "=RC" & obtener_numero_columna(ws_resumen, "conteo_ultimate") & " * RC" & obtener_numero_columna(ws_resumen, "seve_ultimate_" & Atributo) & ""
    End If

    If HojaOrigen = "Plata" Then
        ws_resumen.Range(ws_resumen.Cells(FilaDestino, ColFrecuenciaUltimate), ws_resumen.Cells(FilaDestino + NumFilas - 1, ColFrecuenciaUltimate)).ClearContents
        ws_resumen.Range(ws_resumen.Cells(FilaDestino, ColSeveridadUltimate), ws_resumen.Cells(FilaDestino + NumFilas - 1, ColSeveridadUltimate)).ClearContents
    End If

End Function
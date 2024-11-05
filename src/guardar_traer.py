import sqlite3
import xlwings as xw
import constantes as ct


def index(objective: str, range: xw.Range) -> int:
    return range.value.index(objective) + 1


def objective_range(
    sheet: xw.Sheet,
    objective_string: str,
    row_or_column: str,
    offset_y: int,
    offset_x: int,
    height: int,
    width: int,
) -> xw.Range:
    return sheet.range(
        sheet.cells(
            index(objective_string, sheet.range(row_or_column)) + offset_y, offset_x
        ),
        sheet.cells(
            index(objective_string, sheet.range(row_or_column)) + offset_y + height - 1,
            offset_x + width - 1,
        ),
    )


def parameter_ranges(
    sheet: xw.Sheet, num_ocurrencias: int, num_alturas: int, mes_del_periodo: int
) -> dict[str, xw.Range]:
    ranges = {
        "MET_PAGO_INCURRIDO": sheet.range(sheet.cells(2, 3), sheet.cells(2, 4)),
        "EXCLUSIONES": objective_range(
            sheet,
            "Exclusiones",
            "F1:F1000",
            ct.HEADER_TRIANGULOS,
            ct.COL_OCURRS_PLANTILLAS + 1,
            num_ocurrencias,
            num_alturas,
        ),
        "VENTANAS": objective_range(sheet, "Periodo inicial", "B1:B1000", 1, 2, 16, 3),
        "FACTORES_SELECCIONADOS": objective_range(
            sheet,
            "FACTORES SELECCIONADOS",
            "F1:F1000",
            0,
            ct.COL_OCURRS_PLANTILLAS + 1,
            1,
            num_alturas,
        ),
        "ULTIMATE": objective_range(
            sheet,
            "Ultimate",
            "D1:D1000",
            1,
            4,
            num_ocurrencias + mes_del_periodo - 1,
            1,
        ),
        "METODOLOGIA": objective_range(
            sheet,
            "Metodologia",
            "E1:E1000",
            1,
            5,
            num_ocurrencias + mes_del_periodo - 1,
            1,
        ),
    }

    if sheet.name in ("Plantilla_Frec", "Plantilla_Seve", "Plantilla_Plata"):
        ranges.update(
            {
                "BASE": objective_range(
                    sheet,
                    "Base",
                    "F1:F1000",
                    ct.HEADER_TRIANGULOS,
                    ct.COL_OCURRS_PLANTILLAS + 1,
                    num_ocurrencias,
                    num_alturas,
                ),
                "INDICADOR": objective_range(
                    sheet, "Indicador", "F1:F1000", 1, 6, num_ocurrencias, 1
                ),
                "COMENTARIOS": objective_range(
                    sheet, "Comentarios", "G1:G1000", 1, 7, num_ocurrencias, 1
                ),
            }
        )

        if sheet.name == "Plantilla_Seve":
            ranges.update(
                {
                    "TIPO_INDEXACION": sheet.range(
                        sheet.cells(3, 3), sheet.cells(3, 4)
                    ),
                    "MEDIDA_INDEXACION": sheet.range(
                        sheet.cells(4, 3), sheet.cells(4, 4)
                    ),
                }
            )

            if sheet["C4"].value != "Ninguna":
                ranges.update(
                    {
                        "UNIDAD_INDEXACION": objective_range(
                            sheet,
                            "Unidad_Indexacion",
                            "F1:F1000",
                            ct.HEADER_TRIANGULOS,
                            ct.COL_OCURRS_PLANTILLAS + 1,
                            num_ocurrencias,
                            num_alturas,
                        )
                    }
                )

    elif sheet.name == "Plantilla_Entremes":
        ranges.update(
            {
                "FREC_SEV_ULTIMA_OCURRENCIA": sheet.range(
                    sheet.cells(3, 3), sheet.cells(3, 4)
                ),
                "VARIABLE_DESPEJADA": sheet.range(sheet.cells(4, 3), sheet.cells(4, 4)),
                "COMENTARIOS": objective_range(
                    sheet,
                    "Comentarios",
                    "J1:J1000",
                    1,
                    10,
                    num_ocurrencias + mes_del_periodo - 1,
                    1,
                ),
                "FACTOR_COMPLETITUD": objective_range(
                    sheet,
                    "Factor completitud",
                    "T1:T1000",
                    1,
                    20,
                    num_ocurrencias - 1,
                    1,
                ),
                "PCT_SUE_BF": objective_range(
                    sheet, "% SUE BF", "AA1:AA1000", 1, 27, num_ocurrencias - 1, 1
                ),
                "VELOCIDAD_BF": objective_range(
                    sheet, "Velocidad BF", "AB1:AB1000", 1, 28, num_ocurrencias - 1, 1
                ),
                "PCT_SUE_NUEVO": objective_range(
                    sheet, "% SUE Nuevo", "AG1:AG1000", 1, 33, num_ocurrencias - 1, 1
                ),
                "AJUSTE_PARCIAL": objective_range(
                    sheet,
                    "% Ajuste",
                    "AK1:AK1000",
                    1,
                    37,
                    num_ocurrencias + mes_del_periodo - 2,
                    1,
                ),
                "COMENTARIOS_AJUSTE": objective_range(
                    sheet,
                    "Comentarios Aj.",
                    "AN1:AN1000",
                    1,
                    40,
                    num_ocurrencias + mes_del_periodo - 2,
                    1,
                ),
            }
        )

        if sheet["C3"].value == "% Siniestralidad":
            ranges.update(
                {
                    "ULTIMATE_NUEVO_PERIODO": objective_range(
                        sheet,
                        "Ultimate",
                        "D1:D1000",
                        num_ocurrencias,
                        4,
                        mes_del_periodo,
                        1,
                    ),
                    "PCT_ULTIMATE_NUEVO_PERIODO": objective_range(
                        sheet,
                        "% SUE",
                        "G1:G1000",
                        num_ocurrencias,
                        6,
                        mes_del_periodo,
                        1,
                    ),
                    "ULTIMATE_FRECUENCIA_SEVERIDAD": objective_range(
                        sheet,
                        "Ultimate",
                        "D1:D1000",
                        num_ocurrencias + mes_del_periodo + ct.SEP_TRIANGULOS + 1,
                        4,
                        num_ocurrencias + mes_del_periodo - 1,
                        1,
                    ),
                    "COMENTARIOS_FRECUENCIA_SEVERIDAD": objective_range(
                        sheet,
                        "Ultimate",
                        "D1:D1000",
                        num_ocurrencias + mes_del_periodo + ct.SEP_TRIANGULOS + 1,
                        5,
                        num_ocurrencias + mes_del_periodo - 1,
                        1,
                    ),
                }
            )

        if sheet["C3"] == "Frecuencia y Severidad":
            ranges.update(
                {
                    "ULTIMATE_FRECUENCIA": objective_range(
                        sheet,
                        "Ultimate",
                        "D1:D1000",
                        num_ocurrencias + mes_del_periodo + ct.SEP_TRIANGULOS + 1,
                        4,
                        num_ocurrencias + mes_del_periodo - 1,
                        1,
                    ),
                    "COMENTARIOS_FRECUENCIA": objective_range(
                        sheet,
                        "Ultimate",
                        "D1:D1000",
                        num_ocurrencias + mes_del_periodo + ct.SEP_TRIANGULOS + 1,
                        5,
                        num_ocurrencias + mes_del_periodo - 1,
                        1,
                    ),
                    "ULTIMATE_SEVERIDAD": objective_range(
                        sheet,
                        "Ultimate",
                        "D1:D1000",
                        num_ocurrencias + mes_del_periodo + ct.SEP_TRIANGULOS + 1,
                        10,
                        num_ocurrencias + mes_del_periodo - 1,
                        1,
                    ),
                    "COMENTARIOS_SEVERIDAD": objective_range(
                        sheet,
                        "Ultimate",
                        "D1:D1000",
                        num_ocurrencias + mes_del_periodo + ct.SEP_TRIANGULOS + 1,
                        11,
                        num_ocurrencias + mes_del_periodo - 1,
                        1,
                    ),
                }
            )

    return ranges


def guardar(
    path_plantilla: str,
    sheet: xw.Sheet,
    apertura: str,
    atributo: str,
    num_ocurrencias: int,
    num_alturas: int,
    mes_del_periodo: int,
) -> None:
    conn = sqlite3.connect(f"{path_plantilla}/../data/db/formulas.db")
    cursor = conn.cursor()

    for range_name, range_values in parameter_ranges(
        sheet, num_ocurrencias, num_alturas, mes_del_periodo
    ).items():
        cursor.execute(
            """
            DELETE FROM matrices 
            WHERE apertura = ? 
            AND atributo = ?
            AND plantilla = ?
            AND tipo = ?
            """,
            (apertura, atributo, sheet.name, range_name),
        )

        for r, row in enumerate(range_values.formula, start=1):
            for c, formula in enumerate(row, start=1):
                cursor.execute(
                    """INSERT OR REPLACE INTO matrices 
                    (apertura, atributo, plantilla, tipo, fila, columna, formula) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (apertura, atributo, sheet.name, range_name, r, c, formula),
                )

    conn.commit()
    conn.close()


def traer_formulas(
    cursor: sqlite3.Cursor,
    apertura: str,
    atributo: str,
    plantilla: str,
    tipo: str,
    range_values: xw.Range,
):
    cursor.execute(
        """
        SELECT fila, columna, formula 
        FROM matrices 
        WHERE apertura = ? 
        AND atributo = ?
        AND plantilla = ?
        AND tipo = ?
        """,
        (apertura, atributo, plantilla, tipo),
    )

    formulas = [
        ["" for _ in range(range_values.shape[1])] for _ in range(range_values.shape[0])
    ]
    for row, col, formula in cursor.fetchall():
        formulas[row - 1][col - 1] = formula

    return formulas


def traer(
    path_plantilla: str,
    sheet: xw.Sheet,
    apertura: str,
    atributo: str,
    num_ocurrencias: int,
    num_alturas: int,
    mes_del_periodo: int,
) -> None:
    conn = sqlite3.connect(f"{path_plantilla}/../data/db/formulas.db")
    cursor = conn.cursor()

    for range_name, range_values in parameter_ranges(
        sheet, num_ocurrencias, num_alturas, mes_del_periodo
    ).items():
        range_values.formula = traer_formulas(
            cursor, apertura, atributo, sheet.name, range_name, range_values
        )

    conn.close()

# mypy: ignore-errors

import polars as pl
import pandas as pd
import xlwings as xw
import constantes as ct


def color_triangulo(ws: xw.Sheet, fila_triangulo: int, num_alturas: int):
    ws.cells(fila_triangulo, ct.COL_OCURRS_PLANTILLAS).color = (185, 207, 255)
    ws.range(
        ws.cells(fila_triangulo, ct.COL_OCURRS_PLANTILLAS + 1),
        ws.cells(fila_triangulo, ct.COL_OCURRS_PLANTILLAS + num_alturas),
    ).color = (
        191,
        248,
        255,
    )
    ws.range(
        ws.cells(fila_triangulo, ct.COL_OCURRS_PLANTILLAS + num_alturas + 1),
        ws.cells(fila_triangulo, ct.COL_OCURRS_AUXTOT + num_alturas * 2),
    ).color = (249, 250, 211)


def limpiar_plantilla(ws: xw.Sheet) -> None:
    for chart in ws.charts:
        chart.delete()
    ws.range(ws.cells(7, 1), ws.cells(1000, 1000)).clear()


def base_plantillas(
    apertura: str, atributo: str, periodicidades: list[list[str]], cantidades: list[str]
) -> pd.DataFrame:
    return (
        pl.scan_parquet(
            "Users/sebatoec/tests/xlwings/data/processed/base_triangulos.parquet"
        )
        .filter(pl.col("apertura_reservas") == apertura)
        .join(
            pl.LazyFrame(
                periodicidades, schema=["apertura_reservas", "periodicidad_ocurrencia"]
            ),
            on=["apertura_reservas", "periodicidad_ocurrencia"],
        )
        .drop(["diagonal", "periodo_desarrollo"])
        .unpivot(
            index=[
                "apertura_reservas",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "periodicidad_desarrollo",
                "index_desarrollo",
            ],
            variable_name="cantidad",
            value_name="valor",
        )
        .with_columns(
            atributo=pl.when(pl.col("cantidad").str.contains("retenido"))
            .then(pl.lit("retenido"))
            .otherwise(pl.lit("bruto")),
            cantidad=pl.col("cantidad").str.replace_many(
                {"_bruto": "", "_retenido": ""}
            ),
        )
        .filter((pl.col("atributo") == atributo) & pl.col("cantidad").is_in(cantidades))
        .collect()
        .to_pandas()
        .pivot(
            index="periodo_ocurrencia",
            columns=["cantidad", "index_desarrollo"],
            values="valor",
        )
    )


def triangulo(
    wb: xw.Book,
    ws: xw.Sheet,
    df,
    nombre: str,
    formula: str,
    formato: str,
    num_triangulo: int,
):
    NUM_OCURRENCIAS = ct.num_ocurrencias(ws)
    NUM_ALTURAS = ct.num_alturas(ws)

    fila_triangulo = (
        ct.FILA_INI_PLANTILLAS + (NUM_OCURRENCIAS + ct.SEP_TRIANGULOS) * num_triangulo
    )

    # s1 = time.time()
    color_triangulo(ws, fila_triangulo, NUM_ALTURAS)
    # wb.sheets["Modo"]["A2"].value = time.time() - s1

    celda_triangulo = ws.cells(fila_triangulo, ct.COL_OCURRS_PLANTILLAS)
    celda_triangulo.options(index=False).value = df
    celda_triangulo.value = nombre

    rango_triangulo = ws.range(
        ws.cells(fila_triangulo + 1, ct.COL_OCURRS_PLANTILLAS + 1),
        ws.cells(
            fila_triangulo + NUM_OCURRENCIAS,
            ct.COL_OCURRS_PLANTILLAS + NUM_ALTURAS * 2,
        ),
    )
    rango_triangulo.formula = formula
    rango_triangulo.number_format = formato

    # if nombre == "Ratios" and "Entremes" not in ws.name:
    #     celda_periodo = ws.cells(fila_triangulo, ct.COL_OCURRS_PLANTILLAS - 1)
    #     celda_periodo.font.bold = True
    #     celda_periodo.color = (
    #         255,
    #         235,
    #         205,
    #     )
    #     celda_periodo.value = "Periodo"

    #     ws.range(
    #         ws.cells(fila_triangulo + 1, ct.COL_OCURRS_PLANTILLAS - 1),
    #         ws.cells(fila_triangulo + 1, ct.COL_OCURRS_PLANTILLAS - 1),
    #     ).formula = f"=COUNTA(R{fila_triangulo + NUM_OCURRENCIAS}C[1]:RC[1]) - 1"

    #     # Mapa de color verde
    #     rng_color = ws.range(
    #         ws.cells(fila_triangulo + 1, ct.COL_OCURRS_PLANTILLAS + 1),
    #         ws.cells(fila_triangulo + NUM_OCURRENCIAS, ct.COL_OCURRS_PLANTILLAS + 1),
    #     )
    #     color_scale = rng_color.api.FormatConditions.AddColorScale(ColorScaleType=2)
    #     color_scale.SetFirstPriority()

    #     color_scale.ColorScaleCriteria(1).Type = xw.constants.ConditionValueLowestValue
    #     color_scale.ColorScaleCriteria(1).FormatColor.Color = 16776444  # Light green
    #     color_scale.ColorScaleCriteria(2).Type = xw.constants.ConditionValueHighestValue
    #     color_scale.ColorScaleCriteria(2).FormatColor.Color = 8109667  # Dark green

    #     # Texto rojo para factores excluidos
    #     ws.api.ReferenceStyle = xw.constants.ReferenceStyleR1C1
    #     formula_expr = f"=R[{NUM_OCURRENCIAS + ct.SEP_TRIANGULOS}]C=0"
    #     rng_color.api.FormatConditions.Add(
    #         Type=xw.constants.ConditionExpression, Formula1=formula_expr
    #     )
    #     ws.api.ReferenceStyle = xw.constants.ReferenceStyleA1

    #     rng_color.api.FormatConditions(2).SetFirstPriority()
    #     rng_color.api.FormatConditions(1).Font.Color = -16776961  # Red text
    #     rng_color.api.FormatConditions(1).StopIfTrue = False

    #     rng_color.api.Copy()
    #     for i in range(2, NUM_ALTURAS * 2):
    #         ws.cells(fila_triangulo + 1, ct.COL_OCURRS_PLANTILLAS + i).api.PasteSpecial(
    #             Paste=xw.constants.PasteFormats
    #         )

    # if nombre == "EXCLUSIONES":
    #     rng_exclusion = ws.range(
    #         ws.cells(celda_row + 3, celda_col + 1),
    #         ws.cells(celda_row + 2 + NUM_OCURRENCIAS, celda_col + 1).end("right"),
    #     )

    #     # Formato para factores excluidos
    #     exclusion_format = rng_exclusion.api.FormatConditions.Add(
    #         Type=xw.constants.ConditionCellValue,
    #         Operator=xw.constants.ConditionOperatorEqual,
    #         Formula1="=0",
    #     )
    #     exclusion_format.SetFirstPriority()
    #     exclusion_format.Font.Color = -16383844  # Dark blue text
    #     exclusion_format.Interior.Color = 13551615  # Light red fill
    #     exclusion_format.StopIfTrue = False


def factores_desarrollo(wb: xw.Book, ws: xw.Sheet, num_triangulos_ant: int):
    NUM_OCURRENCIAS = ct.num_ocurrencias(ws)
    NUM_ALTURAS = ct.num_alturas(ws)

    fila_factores = (
        ct.FILA_INI_PLANTILLAS
        + (NUM_OCURRENCIAS + ct.SEP_TRIANGULOS) * num_triangulos_ant
    )

    ws.cells(fila_factores, ct.COL_OCURRS_PLANTILLAS - 4).value = "Periodo inicial"
    ws.cells(fila_factores, ct.COL_OCURRS_PLANTILLAS - 3).value = "Periodo final"
    ws.cells(fila_factores, ct.COL_OCURRS_PLANTILLAS - 2).value = "Percentil"

    rango_titulos_ventanas = ws.range(
        ws.cells(fila_factores, ct.COL_OCURRS_PLANTILLAS - 4),
        ws.cells(fila_factores, ct.COL_OCURRS_PLANTILLAS - 2),
    )
    rango_titulos_ventanas.color = (185, 207, 255)
    rango_titulos_ventanas.font.bold = True

    color_triangulo(ws, fila_factores, NUM_ALTURAS)

    fila_valores = ct.fila_valores(ws)
    fila_ratios = (
        ws.range(
            ws.cells(1, ct.COL_OCURRS_PLANTILLAS),
            ws.cells(1000, ct.COL_OCURRS_PLANTILLAS),
        ).value.index("Ratios")
        + 1
    )
    fila_exclusiones = (
        ws.range(
            ws.cells(1, ct.COL_OCURRS_PLANTILLAS),
            ws.cells(1000, ct.COL_OCURRS_PLANTILLAS),
        ).value.index("Exclusiones")
        + 1
    )

    corr_ultima_altura = f"=+IF(R9C = {NUM_ALTURAS - 1}, 1, "
    array_ratios = f" R{fila_ratios}C : R{fila_ratios + NUM_OCURRENCIAS - 1}C "
    array_exclus = (
        f" R{fila_exclusiones}C : R{fila_exclusiones + NUM_OCURRENCIAS - 1}C "
    )
    array_valores_C1 = (
        f" R{fila_valores}C[1] : R{fila_valores + NUM_OCURRENCIAS - 1}C[1] "
    )
    array_valores_C0 = (
        f" R{fila_valores}C    : R{fila_valores + NUM_OCURRENCIAS - 1}C    "
    )

    # Vector con factores filtrados por exclusiones
    base_full = f"FILTER({array_ratios}, {array_exclus} > 0)"

    # Vector con factores filtrados por exclusiones y ventana temporal
    base_ventana = f"FILTER(INDEX({array_ratios}, MAX(COUNT({array_ratios}) - RC2 + 1, 1)) : INDEX({array_ratios}, MAX(COUNT({array_ratios}) - RC3 + 1, 1)), INDEX({array_exclus}, MAX(COUNT({array_exclus}) - RC2 + 1, 1)) : INDEX({array_exclus}, MAX(COUNT({array_exclus}) - RC3 + 1, 1)) > 0)"

    # En el promedio ponderado solamente varia la columna entre numerador y denominador
    ff1 = f"SUM(FILTER(INDEX({array_valores_C1}, MAX(COUNT({array_valores_C1}), 1)) : R{fila_valores}C[1], INDEX({array_exclus}, MAX(COUNT({array_exclus}), 1)) : R{fila_exclusiones}C > 0)) / "
    ff2 = f"SUM(FILTER(INDEX({array_valores_C0}, MAX(COUNT({array_valores_C1}), 1)) : R{fila_valores}C, INDEX({array_exclus}, MAX(COUNT({array_exclus}), 1)) : R{fila_exclusiones}C > 0)) ) "

    # En el promedio ponderado solamente varia la columna entre numerador y denominador
    fv1 = (
        "SUM(FILTER(INDEX("
        + array_valores_C1
        + ", MAX(COUNT("
        + array_valores_C1
        + ") - RC2 + 1, 1)) : INDEX("
        + array_valores_C1
        + ", MAX(COUNT("
        + array_valores_C1
        + ") - RC3 + 1, 1)), INDEX("
        + array_exclus
        + ", MAX(COUNT("
        + array_exclus
        + ") - RC2 + 1, 1)) : INDEX("
        + array_exclus
        + ", MAX(COUNT("
        + array_exclus
        + ") - RC3 + 1, 1)) > 0)) / "
    )
    fv2 = (
        "SUM(FILTER(INDEX("
        + array_valores_C0
        + ", MAX(COUNT("
        + array_valores_C1
        + ") - RC2 + 1, 1)) : INDEX("
        + array_valores_C0
        + ", MAX(COUNT("
        + array_valores_C1
        + ") - RC3 + 1, 1)), INDEX("
        + array_exclus
        + ", MAX(COUNT("
        + array_exclus
        + ") - RC2 + 1, 1)) : INDEX("
        + array_exclus
        + ", MAX(COUNT("
        + array_exclus
        + ") - RC3 + 1, 1)) > 0)) ) "
    )

    factores = pl.DataFrame(
        [
            [
                "PROMEDIO",
                "",
                "",
                "",
                corr_ultima_altura + "AVERAGE(" + base_full + "))",
            ],
            ["MEDIANA", "", "", "", corr_ultima_altura + "MEDIAN(" + base_full + "))"],
            ["PROMEDIO PONDERADO", "", "", "", corr_ultima_altura + ff1 + ff2],
            ["MINIMO", "", "", "", corr_ultima_altura + "MIN(" + base_full + "))"],
            ["MAXIMO", "", "", "", corr_ultima_altura + "MAX(" + base_full + "))"],
            [
                "PERCENTIL 1",
                "",
                "",
                0.3,
                corr_ultima_altura + "PERCENTILE.INC(" + base_full + ", RC4))",
            ],
            [
                "PERCENTIL 2",
                "",
                "",
                0.7,
                corr_ultima_altura + "PERCENTILE.INC(" + base_full + ", RC4))",
            ],
            [
                "PROMEDIO VENTANA",
                1,
                4,
                "",
                corr_ultima_altura + "AVERAGE(" + base_ventana + "))",
            ],
            [
                "MEDIANA VENTANA",
                1,
                4,
                "",
                corr_ultima_altura + "MEDIAN(" + base_ventana + "))",
            ],
            [
                "PROMEDIO PONDERADO VENTANA",
                1,
                4,
                "",
                corr_ultima_altura + "" + fv1 + fv2,
            ],
            [
                "MINIMO VENTANA",
                1,
                4,
                "",
                corr_ultima_altura + "MIN(" + base_ventana + "))",
            ],
            [
                "MAXIMO VENTANA",
                1,
                4,
                "",
                corr_ultima_altura + "MAX(" + base_ventana + "))",
            ],
            [
                "PERCENTIL 1 VENTANA",
                1,
                4,
                0.3,
                corr_ultima_altura + "PERCENTILE.INC(" + base_ventana + ",RC4))",
            ],
            [
                "PERCENTIL 2 VENTANA",
                1,
                4,
                0.7,
                corr_ultima_altura + "PERCENTILE.INC(" + base_ventana + ",RC4))",
            ],
            ["FACTORES SELECCIONADOS", "", "", "", "=+IFERROR(R[-5]C, 1)"],
            ["FACTORES ACUMULADOS", "", "", "", corr_ultima_altura + "R[-1]C*RC[1])"],
            ["DESARROLLO", "", "", "", corr_ultima_altura + "1/R[-1]C)"],
        ],
        schema=["Nombre", "Periodo inicial", "Periodo final", "Percentil", "Formula"],
    )

    ws.cells(fila_factores + 1, ct.COL_OCURRS_PLANTILLAS - 4).options(
        transpose=True
    ).value = factores["Periodo inicial"].to_list()
    ws.cells(fila_factores + 1, ct.COL_OCURRS_PLANTILLAS - 3).options(
        transpose=True
    ).value = factores["Periodo final"].to_list()
    ws.cells(fila_factores + 1, ct.COL_OCURRS_PLANTILLAS - 2).options(
        transpose=True
    ).value = factores["Percentil"].to_list()

    ws.cells(fila_factores + 1, ct.COL_OCURRS_PLANTILLAS).options(
        transpose=True
    ).value = factores["Nombre"].to_list()
    ws.range(
        ws.cells(fila_factores + 1, ct.COL_OCURRS_PLANTILLAS),
        ws.cells(fila_factores + factores.shape[0], ct.COL_OCURRS_PLANTILLAS),
    ).color = (185, 207, 255)

    for n_factor, formula in enumerate(factores["Formula"].to_list()):
        ws.cells(
            fila_factores + 1 + n_factor, ct.COL_OCURRS_PLANTILLAS + 1
        ).formula = formula

    # wb.sheets["WTF"]["A1"].formula = corr_ultima_altura + "AVERAGE(" + base_full + "))"
    wb.sheets["WTF"]["A1"].value = base_ventana

    # ws.range(
    #     ws.cells(fila_factores + 1, ct.COL_OCURRS_PLANTILLAS + 1),
    #     ws.cells(
    #         fila_factores + factores.shape[0],
    #         ct.COL_OCURRS_PLANTILLAS + NUM_ALTURAS * 2,
    #     ),
    # ).number_format = "#,##0.0000"
    # ws.range(
    #     ws.cells(fila_factores + factores.shape[0], ct.COL_OCURRS_PLANTILLAS + 1),
    #     ws.cells(
    #         fila_factores + factores.shape[0],
    #         ct.COL_OCURRS_PLANTILLAS + NUM_ALTURAS * 2,
    #     ),
    # ).number_format = "0.00%"
    # ws.range(
    #     ws.cells(
    #         fila_factores + factores.shape[0] - 2,
    #         ct.COL_OCURRS_PLANTILLAS + 1,
    #     ),
    #     ws.cells(
    #         fila_factores + factores.shape[0] - 2,
    #         ct.COL_OCURRS_PLANTILLAS + NUM_ALTURAS * 2,
    #     ),
    # ).font.bold = True

    # ws.Range(ws.Cells(celda_row + 1, celda_col + 1), ws.Cells(celda_row + UBound(formulas), celda_col + 1)).Copy
    # ws.Range(ws.Cells(celda_row + 1, celda_col + 1), ws.Cells(celda_row + UBound(formulas), celda_col + num_alturas * 2)).PasteSpecial Paste:=xlPasteAll

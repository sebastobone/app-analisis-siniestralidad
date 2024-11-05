# mypy: ignore-errors

import xlwings as xw
import base_plantillas
import tablas_resumen
import guardar_traer

import time


def main():
    wb = xw.Book.caller()
    modo = wb.sheets["Modo"]["A1"].value

    apertura = wb.sheets["Aperturas"]["A2"].value
    atributo = wb.sheets["Atributos"]["A2"].value
    periodicidades = (
        wb.sheets["Aperturas"].tables["periodicidades"].data_body_range.value
    )

    if modo == "PREPARAR_PLANTILLA":
        s = time.time()
        diagonales, expuestos, primas = tablas_resumen.tablas_resumen(periodicidades)

        wb.sheets["Aux_Totales"].range("A2:A10000").number_format = "@"
        wb.sheets["Aux_Totales"]["A1"].options(
            index=False
        ).value = diagonales.collect().to_pandas()

        wb.sheets["Aux_Expuestos"]["A1"].options(
            index=False
        ).value = expuestos.collect().to_pandas()
        wb.sheets["Aux_Primas"]["A1"].options(
            index=False
        ).value = primas.collect().to_pandas()

        wb.sheets["Modo"]["A2"].value = time.time() - s

    elif "GENERAR" in modo:
        s = time.time()

        xw.apps.active.api.ScreenUpdating = False
        xw.apps.active.api.Calculation = xw.constants.Calculation.xlCalculationManual

        plantilla = modo.replace("GENERAR_", "")
        atributo = atributo if plantilla != "Plantilla_Frec" else "Bruto"
        cantidades = (
            ["pago", "incurrido"]
            if plantilla != "Plantilla_Frec"
            else ["conteo_pago", "conteo_incurrido"]
        )

        df = base_plantillas.base_plantillas(
            apertura, atributo, periodicidades, cantidades
        ).to_pandas()

        # s = time.time()
        base_plantillas.limpiar_plantilla(wb.sheets[plantilla])
        # wb.sheets["Modo"]["A2"].value = time.time() - s

        wb.sheets[plantilla]["F7"].options(index=False).value = df

        macro = wb.macro("sep_triangulos")
        a = macro()
        print(a)
        wb.sheets[plantilla]["F28"].formula = a

        wtf = wb.macro("generar_plantilla_frecuencia")
        wtf()

        # base_plantillas.color_triangulo(wb.sheets[plantilla], 7, (df.shape[1] - 1) // 2)

        # base_plantillas.triangulo(
        #     wb,
        #     wb.sheets[plantilla],
        #     df,
        #     nombre="Frecuencia",
        #     formula=f"""=IF(R[{-ct.num_ocurrencias(wb.sheets[plantilla]) - ct.SEP_TRIANGULOS}]C = "", "", IFERROR(R[{-ct.num_ocurrencias(wb.sheets[plantilla]) - ct.SEP_TRIANGULOS}]C / SUMIFS(Aux_Totales!C{ct.COL_EXPUESTOS_AUXTOT}, Aux_Totales!C{ct.COL_OCURRS_AUXTOT}, RC6, Aux_Totales!C{ct.COL_APERTURAS_AUXTOT}, Aperturas!R2C1), 0))""",
        #     formato="0.0000%",
        #     num_triangulo=1,
        # )

        # base_plantillas.triangulo(
        #     wb,
        #     wb.sheets[plantilla],
        #     df,
        #     nombre="Ratios",
        #     formula=f"""=+IF(OR(R[{-ct.SEP_TRIANGULOS - ct.num_ocurrencias(wb.sheets[plantilla])}]C[1]="", R9C[1]<R9C), "", IFERROR(R[{-ct.SEP_TRIANGULOS - ct.num_ocurrencias(wb.sheets[plantilla])}]C[1]/R[{-ct.SEP_TRIANGULOS - ct.num_ocurrencias(wb.sheets[plantilla])}]C,""))""",
        #     formato="#,##0.0000",
        #     num_triangulo=2,
        # )

        # base_plantillas.triangulo(
        #     wb,
        #     wb.sheets[plantilla],
        #     df,
        #     nombre="Exclusiones",
        #     formula=f"""=+IF(OR(R[{(-ct.SEP_TRIANGULOS - ct.num_ocurrencias(wb.sheets[plantilla])) * 2}]C[1]="",R9C[1]<R9C),"",1)""",
        #     formato="#,##0",
        #     num_triangulo=3,
        # )

        # base_plantillas.factores_desarrollo(wb, wb.sheets[plantilla], 4)

        xw.apps.active.api.Calculation = xw.constants.Calculation.xlCalculationAutomatic
        xw.apps.active.api.ScreenUpdating = True

        wb.sheets["Modo"]["A2"].value = time.time() - s

        # s = time.time()
        # ct.num_ocurrencias(wb.sheets[plantilla])
        # wb.sheets["Modo"]["A2"].value = time.time() - s

    elif "GUARDAR" in modo:
        s = time.time()

        plantilla = modo.replace("GUARDAR_", "")
        atributo = atributo if plantilla != "Plantilla_Frec" else "Bruto"

        guardar_traer.guardar(
            wb.sheets[plantilla],
            apertura,
            atributo,
        )

        cols_ultimate = {
            "Plantilla_Frec": "Frec_Ultimate",
            "Plantilla_Seve": f"Seve_Ultimate_{atributo}",
            "Plantilla_Plata": f"Plata_Ultimate_{atributo}",
        }

        guardar_traer.guardar_ultimate(
            wb.sheets[plantilla],
            wb.sheets["Aux_Totales"],
            apertura,
            "Ultimate",
            cols_ultimate[plantilla],
        )

        wb.sheets["Modo"]["A2"].value = time.time() - s

    elif "TRAER" in modo:
        s = time.time()

        plantilla = modo.replace("TRAER_", "")
        atributo = atributo if plantilla != "Plantilla_Frec" else "Bruto"

        guardar_traer.traer(
            wb.sheets[plantilla],
            apertura,
            atributo,
        )

        wb.sheets["Modo"]["A2"].value = time.time() - s


if __name__ == "__main__":
    xw.Book("plantilla.xlsm").set_mock_caller()
    main()

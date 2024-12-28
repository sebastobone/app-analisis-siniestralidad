import os
import shutil
import time

import polars as pl
import xlwings as xw

import src.constantes as ct
from src import utils
from src.metodos_plantilla import base_plantillas, guardar_traer, tablas_resumen


def preparar_plantilla(
    wb: xw.Book, mes_corte: int, tipo_analisis: str, negocio: str
) -> xw.Book:
    s = time.time()

    wb.sheets["Main"]["A4"].value = "Mes corte"
    wb.sheets["Main"]["B4"].value = mes_corte
    wb.sheets["Main"]["A5"].value = "Mes anterior"
    wb.sheets["Main"]["B5"].value = (
        mes_corte - 1 if mes_corte % 100 != 1 else ((mes_corte // 100) - 1) * 100 + 12
    )

    if tipo_analisis == "triangulos":
        wb.sheets["Plantilla_Entremes"].visible = False
        wb.sheets["Plantilla_Frec"].visible = True
        wb.sheets["Plantilla_Seve"].visible = True
        wb.sheets["Plantilla_Plata"].visible = True
    elif tipo_analisis == "entremes":
        wb.sheets["Plantilla_Entremes"].visible = True
        wb.sheets["Plantilla_Frec"].visible = False
        wb.sheets["Plantilla_Seve"].visible = False
        wb.sheets["Plantilla_Plata"].visible = False

    for sheet in ["Aux_Totales", "Aux_Expuestos", "Aux_Primas", "Atipicos"]:
        wb.sheets[sheet].clear_contents()

    aperturas = tablas_resumen.aperturas(negocio)

    tablas_resumen.generar_tabla(wb.sheets["Main"], aperturas, "aperturas", (1, 4))
    tablas_resumen.generar_tabla(
        wb.sheets["Main"],
        aperturas.select("apertura_reservas").with_columns(
            periodicidad=pl.lit("Trimestral")
        ),
        "periodicidades",
        (1, 4 + len(aperturas.collect_schema().names()) + 1),
    )

    periodicidades = wb.sheets["Main"].tables["periodicidades"].data_body_range.value

    diagonales, expuestos, primas, atipicos = tablas_resumen.tablas_resumen(
        ct.path_plantilla(wb), periodicidades, tipo_analisis, aperturas.lazy()
    )

    wb.sheets["Aux_Totales"]["A1"].options(index=False).value = diagonales.to_pandas()

    wb.macro("formulas_aux_totales")(diagonales.shape[0])

    wb.sheets["Aux_Expuestos"]["A1"].options(index=False).value = expuestos.to_pandas()
    wb.sheets["Aux_Primas"]["A1"].options(index=False).value = primas.to_pandas()
    wb.sheets["Atipicos"]["A1"].options(index=False).value = atipicos.to_pandas()

    wb.sheets["Main"]["A1"].value = "PREPARAR_PLANTILLA"
    wb.sheets["Main"]["A2"].value = time.time() - s

    return wb


def generar_plantilla(
    wb: xw.Book, plantilla: str, apertura: str, atributo: str, mes_corte: int
) -> None:
    s = time.time()

    plantilla = f"Plantilla_{plantilla.capitalize()}"

    atributo = atributo if plantilla != "Plantilla_Frec" else "bruto"
    cantidades = (
        ["pago", "incurrido"]
        if plantilla != "Plantilla_Frec"
        else ["conteo_pago", "conteo_incurrido"]
    )

    periodicidades = (
        wb.sheets["Aperturas"].tables["periodicidades"].data_body_range.value
    )

    df = base_plantillas.base_plantillas(
        ct.path_plantilla(wb), apertura, atributo, periodicidades, cantidades
    )

    NUM_OCURRENCIAS = df.shape[0]
    NUM_ALTURAS = df.shape[1] // len(cantidades)
    MES_DEL_PERIODO = ct.mes_del_periodo(
        utils.yyyymm_to_date(mes_corte), NUM_OCURRENCIAS, NUM_ALTURAS
    )

    wb.sheets["Modo"]["B6"].value = MES_DEL_PERIODO

    wb.macro("limpiar_plantilla")(plantilla)

    wb.sheets[plantilla]["F7"].value = df

    wb.macro(f"generar_{plantilla}")(
        NUM_OCURRENCIAS,
        NUM_ALTURAS,
        ct.HEADER_TRIANGULOS,
        ct.SEP_TRIANGULOS,
        ct.FILA_INI_PLANTILLAS,
        ct.COL_OCURRS_PLANTILLAS,
        atributo,
        MES_DEL_PERIODO,
    )

    wb.sheets["Modo"]["A2"].value = time.time() - s


def guardar_traer_fn(
    wb: xw.Book, modo: str, plantilla: str, apertura: str, atributo: str, mes_corte: int
) -> None:
    s = time.time()

    plantilla = f"Plantilla_{plantilla.capitalize()}"
    atributo = atributo if plantilla != "Plantilla_Frec" else "bruto"

    NUM_OCURRENCIAS = ct.num_ocurrencias(wb.sheets[plantilla])
    NUM_ALTURAS = ct.num_alturas(wb.sheets[plantilla])

    if plantilla == "Plantilla_Entremes":
        MES_DEL_PERIODO = ct.mes_del_periodo(
            utils.yyyymm_to_date(mes_corte), NUM_OCURRENCIAS, NUM_ALTURAS
        )
    else:
        MES_DEL_PERIODO = 1

    if modo == "guardar":
        guardar_traer.guardar(
            wb.sheets[plantilla],
            apertura,
            atributo,
            NUM_OCURRENCIAS,
            NUM_ALTURAS,
            MES_DEL_PERIODO,
        )

        cols_ultimate = {
            "Plantilla_Frec": "frec_ultimate",
            "Plantilla_Seve": f"seve_ultimate_{atributo}",
            "Plantilla_Plata": f"plata_ultimate_{atributo}",
            "Plantilla_Entremes": f"plata_ultimate_{atributo}",
        }

        wb.macro("guardar_ultimate")(
            plantilla,
            NUM_OCURRENCIAS,
            MES_DEL_PERIODO,
            cols_ultimate[plantilla],
            atributo,
        )

        wb.sheets["Modo"]["A2"].value = time.time() - s

    elif modo == "traer":
        guardar_traer.traer(
            wb.sheets[plantilla],
            apertura,
            atributo,
            NUM_OCURRENCIAS,
            NUM_ALTURAS,
            MES_DEL_PERIODO,
        )

        wb.sheets["Modo"]["A2"].value = time.time() - s


def almacenar_analisis(wb: xw.Book, mes_corte: int) -> None:
    s = time.time()

    df_tips = ct.sheet_to_dataframe(wb, "Aux_Totales").with_columns(atipico=0)
    df_atips = ct.sheet_to_dataframe(wb, "Atipicos").with_columns(atipico=1)
    df_new = pl.concat([df_tips, df_atips]).with_columns(MES_CORTE=mes_corte)

    wb_res = xw.Book(f"{ct.path_plantilla(wb)}/resultados.xlsx")
    df_hist = ct.sheet_to_dataframe(wb_res, "BD", df_new.collect_schema())

    # Quitar versiones anteriores del analisis del mes actual para la apertura-reserva
    info_eliminar = df_new.select(["apertura_reservas", "ct.MES_CORTE"]).with_columns(
        eliminar=1
    )

    df_hist = (
        df_hist.join(
            info_eliminar, on=["apertura_reservas", "ct.MES_CORTE"], how="left"
        )
        .filter(pl.col("eliminar").is_null())
        .drop("eliminar")
    )

    df = pl.concat([df_hist, df_new])

    wb_res.sheets["BD"].clear_contents()
    # Por si falla, que no borre los nombres de las columnas
    wb_res.sheets["BD"]["A1"].value = df.collect_schema().names()
    wb_res.sheets["BD"]["A1"].options(index=False).value = df.collect().to_pandas()

    wb.sheets["Modo"]["A2"].value = time.time() - s


def traer_guardar_todo(
    wb: xw.Book, plantilla: str, mes_corte: int, traer: bool = False
) -> None:
    s = time.time()

    aperturas = pl.read_csv("data/processed/aperturas.csv", separator="\t").get_column(
        "apertura_reservas"
    )
    atributos = ["bruto", "retenido"] if plantilla != "Plantilla_Frec" else ["bruto"]

    for apertura in aperturas:
        for atributo in atributos:
            generar_plantilla(wb, plantilla, apertura, atributo, mes_corte)
            if traer:
                guardar_traer_fn(wb, "traer", plantilla, apertura, atributo, mes_corte)
            guardar_traer_fn(wb, "guardar", plantilla, apertura, atributo, mes_corte)

    wb.sheets["Modo"]["A2"].value = time.time() - s


def abrir_plantilla(plantilla_path: str) -> xw.Book:
    if not os.path.exists(plantilla_path):
        shutil.copyfile("src/plantilla.xlsm", plantilla_path)

    xw.Book(plantilla_path).set_mock_caller()
    wb = xw.Book.caller()

    wb.macro("eliminar_modulos")
    wb.macro("crear_modulos")

    return wb

import os
import shutil
import time
from typing import Literal

import polars as pl
import xlwings as xw

import src.constantes as ct
from src import utils
from src.metodos_plantilla import (
    base_plantillas,
    guardar_traer,
    tablas_resumen,
)


def preparar_plantilla(
    wb: xw.Book,
    mes_corte: int,
    tipo_analisis: Literal["triangulos", "entremes"],
    negocio: str,
) -> None:
    s = time.time()

    wb.macro("formatear_parametro")("Main", "Mes corte", 4, 1)
    wb.sheets["Main"].range((4, 2)).value = mes_corte

    wb.macro("formatear_parametro")("Main", "Mes anterior", 5, 1)
    wb.sheets["Main"].range((5, 2)).value = (
        mes_corte - 1 if mes_corte % 100 != 1 else ((mes_corte // 100) - 1) * 100 + 12
    )

    aperturas = tablas_resumen.aperturas(negocio)

    if tipo_analisis == "triangulos":
        wb.sheets["Plantilla_Entremes"].visible = False
        for plantilla in ["frec", "seve", "plata"]:
            plantilla_name = f"Plantilla_{plantilla.capitalize()}"
            wb.sheets[plantilla_name].visible = True

    elif tipo_analisis == "entremes":
        wb.sheets["Plantilla_Entremes"].visible = True
        for plantilla in ["frec", "seve", "plata"]:
            plantilla_name = f"Plantilla_{plantilla.capitalize()}"
            wb.sheets[plantilla_name].visible = False

    for sheet in ["Aux_Totales", "Aux_Expuestos", "Aux_Primas", "Atipicos"]:
        wb.sheets[sheet].clear_contents()

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
        periodicidades, tipo_analisis, aperturas.lazy()
    )

    wb.sheets["Aux_Totales"]["A1"].options(index=False).value = diagonales.to_pandas()

    wb.macro("formulas_aux_totales")(diagonales.shape[0])

    wb.sheets["Aux_Expuestos"]["A1"].options(index=False).value = expuestos.to_pandas()
    wb.sheets["Aux_Primas"]["A1"].options(index=False).value = primas.to_pandas()
    wb.sheets["Atipicos"]["A1"].options(index=False).value = atipicos.to_pandas()

    wb.sheets["Main"]["A1"].value = "PREPARAR_PLANTILLA"
    wb.sheets["Main"]["A2"].value = time.time() - s


def generar_plantilla(
    wb: xw.Book,
    plantilla: Literal["frec", "seve", "plata", "entremes"],
    mes_corte: int,
    negocio: str,
) -> None:
    s = time.time()

    plantilla_name = f"Plantilla_{plantilla.capitalize()}"

    aperturas = (
        tablas_resumen.aperturas(negocio).get_column("apertura_reservas").to_list()
    )

    wb.macro("limpiar_plantilla")(plantilla_name)
    wb.macro("generar_parametros")(plantilla_name, ",".join(aperturas), aperturas[0])

    apertura = str(wb.sheets[plantilla_name]["C2"].value)
    atributo = str(wb.sheets[plantilla_name]["C3"].value).lower()
    cantidades = (
        ["pago", "incurrido"]
        if plantilla_name != "Plantilla_Frec"
        else ["conteo_pago", "conteo_incurrido"]
    )

    periodicidades = wb.sheets["Main"].tables["periodicidades"].data_body_range.value

    df = base_plantillas.base_plantillas(apertura, atributo, periodicidades, cantidades)

    num_ocurrencias = df.shape[0]
    num_alturas = df.shape[1] // len(cantidades)
    mes_del_periodo = utils.mes_del_periodo(
        utils.yyyymm_to_date(mes_corte), num_ocurrencias, num_alturas
    )

    wb.macro("formatear_parametro")("Main", "Mes del periodo", 6, 1)
    wb.sheets["Main"].range((6, 2)).value = mes_del_periodo

    wb.sheets[plantilla_name].cells(
        ct.FILA_INI_PLANTILLAS, ct.COL_OCURRS_PLANTILLAS
    ).value = df

    wb.macro(f"generar_{plantilla_name}")(
        num_ocurrencias,
        num_alturas,
        ct.HEADER_TRIANGULOS,
        ct.SEP_TRIANGULOS,
        ct.FILA_INI_PLANTILLAS,
        ct.COL_OCURRS_PLANTILLAS,
        apertura,
        atributo,
        mes_del_periodo,
    )

    wb.sheets["Main"]["A1"].value = "GENERAR_PLANTILLA"
    wb.sheets["Main"]["A2"].value = time.time() - s


def guardar_traer_fn(
    wb: xw.Book,
    modo: str,
    plantilla: Literal["frec", "seve", "plata", "entremes"],
    mes_corte: int,
) -> None:
    s = time.time()

    plantilla_name = f"Plantilla_{plantilla.capitalize()}"

    apertura = str(wb.sheets[plantilla_name]["C2"].value)
    atributo = str(wb.sheets[plantilla_name]["C3"].value).lower()

    num_ocurrencias = utils.num_ocurrencias(wb.sheets[plantilla_name])
    num_alturas = utils.num_alturas(wb.sheets[plantilla_name])

    if plantilla_name == "Plantilla_Entremes":
        mes_del_periodo = utils.mes_del_periodo(
            utils.yyyymm_to_date(mes_corte), num_ocurrencias, num_alturas
        )
    else:
        mes_del_periodo = 1

    if modo == "guardar":
        guardar_traer.guardar(
            wb.sheets[plantilla_name],
            apertura,
            atributo,
            num_ocurrencias,
            num_alturas,
            mes_del_periodo,
        )

        cols_ultimate = {
            "Plantilla_Frec": "frec_ultimate",
            "Plantilla_Seve": f"seve_ultimate_{atributo}",
            "Plantilla_Plata": f"plata_ultimate_{atributo}",
            "Plantilla_Entremes": f"plata_ultimate_{atributo}",
        }

        wb.macro("guardar_ultimate")(
            plantilla_name,
            num_ocurrencias,
            mes_del_periodo,
            cols_ultimate[plantilla_name],
            apertura,
            atributo,
        )

        wb.sheets["Main"]["A1"].value = "GUARDAR_PLANTILLA"
        wb.sheets["Main"]["A2"].value = time.time() - s

    elif modo == "traer":
        guardar_traer.traer(
            wb.sheets[plantilla_name],
            apertura,
            atributo,
            num_ocurrencias,
            num_alturas,
            mes_del_periodo,
        )

        wb.sheets["Main"]["A1"].value = "TRAER_PLANTILLA"
        wb.sheets["Main"]["A2"].value = time.time() - s


def traer_guardar_todo(
    wb: xw.Book,
    plantilla: Literal["frec", "seve", "plata", "entremes"],
    mes_corte: int,
    negocio: str,
    traer: bool = False,
) -> None:
    s = time.time()

    plantilla_name = f"Plantilla_{plantilla.capitalize()}"
    aperturas = tablas_resumen.aperturas(negocio).get_column("apertura_reservas")
    atributos = (
        ["Bruto", "Retenido"] if plantilla_name != "Plantilla_Frec" else ["Bruto"]
    )

    for apertura in aperturas.to_list():
        for atributo in atributos:
            wb.sheets[plantilla_name]["C2"].value = apertura
            wb.sheets[plantilla_name]["C3"].value = atributo
            generar_plantilla(wb, plantilla, mes_corte, negocio)
            if traer:
                guardar_traer_fn(wb, "traer", plantilla, mes_corte)
            guardar_traer_fn(wb, "guardar", plantilla, mes_corte)

    wb.sheets["Main"]["A1"].value = "TRAER_GUARDAR_TODO" if traer else "GUARDAR_TODO"
    wb.sheets["Main"]["A2"].value = time.time() - s


def almacenar_analisis(wb: xw.Book, mes_corte: int) -> None:
    s = time.time()

    df_resultados_tipicos = (
        utils.sheet_to_dataframe(wb, "Aux_Totales")
        .with_columns(atipico=0, mes_corte=mes_corte)
        .collect()
    )
    df_resultados_atipicos = (
        utils.sheet_to_dataframe(wb, "Atipicos")
        .with_columns(atipico=1, mes_corte=mes_corte)
        .collect()
    )
    df_resultados = pl.concat([df_resultados_tipicos, df_resultados_atipicos])
    df_resultados.write_parquet(f"output/resultados_{wb.name}_{mes_corte}.parquet")

    wb.sheets["Main"]["A1"].value = "ALMACENAR_ANALISIS"
    wb.sheets["Main"]["A2"].value = time.time() - s


def abrir_plantilla(plantilla_path: str) -> xw.Book:
    if not os.path.exists(plantilla_path):
        shutil.copyfile("plantillas/plantilla.xlsm", plantilla_path)

    wb = xw.Book(plantilla_path, ignore_read_only_recommended=True)

    wb.macro("eliminar_modulos")()
    wb.macro("crear_modulos")()

    return wb

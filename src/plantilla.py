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


def generar_plantilla(
    wb: xw.Book,
    plantilla: Literal["frec", "seve", "plata", "entremes"],
    apertura: str,
    atributo: str,
    mes_corte: int,
) -> None:
    s = time.time()

    plantilla_name = f"Plantilla_{plantilla.capitalize()}"

    atributo = atributo if plantilla_name != "Plantilla_Frec" else "bruto"
    cantidades = (
        ["pago", "incurrido"]
        if plantilla_name != "Plantilla_Frec"
        else ["conteo_pago", "conteo_incurrido"]
    )

    periodicidades = wb.sheets["Main"].tables["periodicidades"].data_body_range.value

    df = base_plantillas.base_plantillas(
        ct.path_plantilla(wb), apertura, atributo, periodicidades, cantidades
    )

    num_ocurrencias = df.shape[0]
    num_alturas = df.shape[1] // len(cantidades)
    mes_del_periodo = ct.mes_del_periodo(
        utils.yyyymm_to_date(mes_corte), num_ocurrencias, num_alturas
    )

    wb.sheets["Main"]["A6"].value = "Mes del periodo"
    wb.sheets["Main"]["B6"].value = mes_del_periodo

    wb.macro("limpiar_plantilla")(plantilla_name)

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
    apertura: str,
    atributo: str,
    mes_corte: int,
) -> None:
    s = time.time()

    plantilla_name = f"Plantilla_{plantilla.capitalize()}"
    atributo = atributo if plantilla_name != "Plantilla_Frec" else "bruto"

    num_ocurrencias = ct.num_ocurrencias(wb.sheets[plantilla_name])
    num_alturas = ct.num_alturas(wb.sheets[plantilla_name])

    if plantilla_name == "Plantilla_Entremes":
        mes_del_periodo = ct.mes_del_periodo(
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

        wb.sheets["Main"]["A2"].value = time.time() - s


def traer_guardar_todo(
    wb: xw.Book,
    plantilla: Literal["frec", "seve", "plata", "entremes"],
    mes_corte: int,
    traer: bool = False,
) -> None:
    s = time.time()

    plantilla_name = f"Plantilla_{plantilla.capitalize()}"
    aperturas = pl.read_csv("data/processed/aperturas.csv", separator="\t").get_column(
        "apertura_reservas"
    )
    atributos = (
        ["bruto", "retenido"] if plantilla_name != "Plantilla_Frec" else ["bruto"]
    )

    for apertura in aperturas:
        for atributo in atributos:
            generar_plantilla(wb, plantilla, apertura, atributo, mes_corte)
            if traer:
                guardar_traer_fn(wb, "traer", plantilla, apertura, atributo, mes_corte)
            guardar_traer_fn(wb, "guardar", plantilla, apertura, atributo, mes_corte)

    wb.sheets["Main"]["A2"].value = time.time() - s


def almacenar_analisis(wb: xw.Book, mes_corte: int) -> None:
    s = time.time()
    
    df_resultados_tipicos = (
        ct.sheet_to_dataframe(wb, "Aux_Totales")
        .with_columns(atipico=0, mes_corte=mes_corte)
        .collect()
    )
    df_resultados_atipicos = (
        ct.sheet_to_dataframe(wb, "Atipicos")
        .with_columns(atipico=1, mes_corte=mes_corte)
        .collect()
    )
    df_resultados = pl.concat([df_resultados_tipicos, df_resultados_atipicos])
    df_resultados.write_parquet(f"output/resultados_{wb.name}_{mes_corte}.parquet")

    wb.sheets["Main"]["A2"].value = time.time() - s


def abrir_plantilla(plantilla_path: str) -> xw.Book:
    if not os.path.exists(plantilla_path):
        shutil.copyfile("src/plantilla.xlsm", plantilla_path)

    wb = xw.Book(plantilla_path, ignore_read_only_recommended=True)

    wb.macro("eliminar_modulos")()
    wb.macro("crear_modulos")()

    return wb

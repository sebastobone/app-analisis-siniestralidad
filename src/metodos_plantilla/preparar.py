import time
from typing import Literal

import polars as pl
import xlwings as xw

from src.logger_config import logger
from src.metodos_plantilla import tablas_resumen


def preparar_plantilla(
    wb: xw.Book,
    mes_corte: int,
    tipo_analisis: Literal["triangulos", "entremes"],
    negocio: str,
) -> None:
    s = time.time()

    generar_parametros_globales(wb, mes_corte)

    aperturas = tablas_resumen.obtener_tabla_aperturas(negocio)
    lista_aperturas = aperturas.get_column("apertura_reservas").to_list()

    generar_parametros_plantillas(wb, lista_aperturas)
    mostrar_plantillas_relevantes(wb, tipo_analisis)

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

    diagonales, atipicos = tablas_resumen.generar_tablas_resumen(
        periodicidades, tipo_analisis, aperturas.lazy()
    )

    generar_hojas_resumen(wb, diagonales, atipicos)

    logger.success("Plantilla preparada.")

    wb.sheets["Main"]["A1"].value = "PREPARAR_PLANTILLA"
    wb.sheets["Main"]["A2"].value = time.time() - s


def mostrar_plantillas_relevantes(wb: xw.Book, tipo_analisis: str):
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


def generar_parametros_globales(wb: xw.Book, mes_corte: int) -> None:
    wb.macro("formatear_parametro")("Main", "Mes corte", 4, 1)
    wb.sheets["Main"].range((4, 2)).value = mes_corte

    wb.macro("formatear_parametro")("Main", "Mes anterior", 5, 1)
    wb.sheets["Main"].range((5, 2)).value = (
        mes_corte - 1 if mes_corte % 100 != 1 else ((mes_corte // 100) - 1) * 100 + 12
    )


def generar_parametros_plantillas(wb: xw.Book, lista_aperturas: list[str]) -> None:
    for plantilla in ["frec", "seve", "plata", "entremes"]:
        plantilla_name = f"Plantilla_{plantilla.capitalize()}"
        wb.macro("generar_parametros")(
            plantilla_name, ",".join(lista_aperturas), lista_aperturas[0]
        )


def generar_hojas_resumen(
    wb: xw.Book, diagonales: pl.DataFrame, atipicos: pl.DataFrame
) -> None:
    for sheet in ["Aux_Totales", "Atipicos"]:
        wb.sheets[sheet].clear_contents()

    wb.sheets["Aux_Totales"]["A1"].options(index=False).value = diagonales.to_pandas()
    wb.sheets["Atipicos"]["A1"].options(index=False).value = atipicos.to_pandas()

    wb.macro("formatos_aux_totales")("Aux_Totales", diagonales.shape[0])
    wb.macro("formulas_aux_totales")(diagonales.shape[0])
    wb.macro("formatos_aux_totales")("Atipicos", atipicos.shape[0])

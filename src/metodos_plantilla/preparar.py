import time
from typing import Literal

import polars as pl
import xlwings as xw

from src import utils
from src.logger_config import logger
from src.metodos_plantilla import resultados, tablas_resumen


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
    resultados_anteriores = resultados.concatenar_archivos_resultados()

    if tipo_analisis == "entremes":
        verificar_resultados_anteriores_para_entremes(
            diagonales, resultados_anteriores, mes_corte
        )

    generar_hojas_resumen(wb, diagonales, resultados_anteriores, atipicos)

    logger.success("Plantilla preparada.")

    wb.sheets["Main"]["A1"].value = "PREPARAR_PLANTILLA"
    wb.sheets["Main"]["A2"].value = time.time() - s


def verificar_resultados_anteriores_para_entremes(
    diagonales: pl.DataFrame, resultados_anteriores: pl.DataFrame, mes_corte: int
) -> None:
    if resultados_anteriores.shape[0] == 0:
        logger.error(
            utils.limpiar_espacios_log(
                """
                No se encontraron resultados anteriores.
                Se necesitan para hacer el analisis de entremes.
                """
            )
        )
        raise ValueError

    mes_corte_anterior = utils.mes_anterior_corte(mes_corte)
    resultados_mes_anterior = resultados_anteriores.filter(
        pl.col("mes_corte") == mes_corte_anterior
    )

    if resultados_mes_anterior.shape[0] == 0:
        logger.error(
            utils.limpiar_espacios_log(
                f"""
                No se encontraron resultados anteriores
                para el mes {mes_corte_anterior}. Se necesitan
                para hacer el analisis de entremes.
                """
            )
        )
        raise ValueError

    aperturas_actuales = sorted(
        diagonales.get_column("apertura_reservas").unique().to_list()
    )
    aperturas_anteriores = sorted(
        resultados_mes_anterior.get_column("apertura_reservas").unique().to_list()
    )
    if aperturas_actuales != aperturas_anteriores:
        logger.error(
            utils.limpiar_espacios_log(
                f"""
                Las aperturas no coinciden con los analisis anteriores,
                los cuales se necesitan para el analisis de entremes. Si realizo
                un cambio a las aperturas con las que quiere hacer el analisis,
                modifique los resultados anteriores y vuelva a intentar.
                Aperturas actuales: {aperturas_actuales}.
                Aperturas anteriores: {aperturas_anteriores}.
                """
            )
        )
        raise ValueError


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
    wb.sheets["Main"].range((5, 2)).value = utils.mes_anterior_corte(mes_corte)


def generar_parametros_plantillas(wb: xw.Book, lista_aperturas: list[str]) -> None:
    for plantilla in ["frec", "seve", "plata", "entremes"]:
        plantilla_name = f"Plantilla_{plantilla.capitalize()}"
        wb.macro("generar_parametros")(
            plantilla_name, ",".join(lista_aperturas), lista_aperturas[0]
        )


def generar_hojas_resumen(
    wb: xw.Book,
    diagonales: pl.DataFrame,
    anterior: pl.DataFrame,
    atipicos: pl.DataFrame,
) -> None:
    for sheet in ["Aux_Totales", "Atipicos", "Aux_Anterior"]:
        wb.sheets[sheet].clear_contents()

    if anterior.shape[0] != 0:
        wb.sheets["Aux_Anterior"]["A1"].options(
            index=False
        ).value = anterior.to_pandas()
    wb.sheets["Aux_Totales"]["A1"].options(index=False).value = diagonales.to_pandas()
    wb.sheets["Atipicos"]["A1"].options(index=False).value = atipicos.to_pandas()

    wb.macro("formatos_aux_totales")("Aux_Totales", diagonales.shape[0])
    wb.macro("formulas_aux_totales")(diagonales.shape[0])
    wb.macro("formatos_aux_totales")("Atipicos", atipicos.shape[0])

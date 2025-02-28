import time
from typing import Literal

import polars as pl
import xlwings as xw

from src import constantes as ct
from src import utils
from src.logger_config import logger
from src.metodos_plantilla import resultados, tablas_resumen

from .completar_diagonal import factor_completitud as compl


def preparar_plantilla(
    wb: xw.Book,
    mes_corte: int,
    tipo_analisis: Literal["triangulos", "entremes"],
    negocio: str,
) -> None:
    s = time.time()

    generar_parametros_globales(wb, mes_corte)

    aperturas = utils.obtener_aperturas(negocio, "siniestros")

    mostrar_plantillas_relevantes(wb, tipo_analisis)

    diagonales, atipicos, entremes = tablas_resumen.generar_tablas_resumen(
        negocio, tipo_analisis, aperturas.lazy()
    )
    resultados_anteriores = resultados.concatenar_archivos_resultados()

    generar_hojas_resumen(wb, diagonales, resultados_anteriores, atipicos)

    if tipo_analisis == "entremes":
        verificar_resultados_anteriores_para_entremes(
            diagonales, resultados_anteriores, mes_corte
        )
        factores_completitud = compl.calcular_factores_completitud(
            aperturas.lazy(), mes_corte
        )
        generar_hoja_entremes(
            wb, entremes, resultados_anteriores, factores_completitud, mes_corte
        )

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
        wb.sheets["Completar_Diagonal"].visible = False
        wb.sheets["Vectores_Index"].visible = True
        for plantilla in ["frec", "seve", "plata"]:
            plantilla_name = f"Plantilla_{plantilla.capitalize()}"
            wb.sheets[plantilla_name].visible = True

    elif tipo_analisis == "entremes":
        wb.sheets["Plantilla_Entremes"].visible = True
        wb.sheets["Completar_Diagonal"].visible = True
        wb.sheets["Vectores_Index"].visible = False
        for plantilla in ["frec", "seve", "plata"]:
            plantilla_name = f"Plantilla_{plantilla.capitalize()}"
            wb.sheets[plantilla_name].visible = False


def generar_parametros_globales(wb: xw.Book, mes_corte: int) -> None:
    wb.macro("formatear_parametro")("Main", "Mes corte", 4, 1)
    wb.sheets["Main"].range((4, 2)).value = mes_corte

    wb.macro("formatear_parametro")("Main", "Mes anterior", 5, 1)
    wb.sheets["Main"].range((5, 2)).value = utils.mes_anterior_corte(mes_corte)


def mantener_formato_columnas(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.concat_str(pl.lit("'"), pl.col(column)).alias(column)
            for column in ["codigo_op", "codigo_ramo_op"]
        ]
    )


def generar_hojas_resumen(
    wb: xw.Book,
    diagonales: pl.DataFrame,
    resultados_anteriores: pl.DataFrame,
    atipicos: pl.DataFrame,
) -> None:
    for sheet in ["Aux_Totales", "Atipicos", "Aux_Anterior"]:
        wb.sheets[sheet].clear()

    if resultados_anteriores.shape[0] != 0:
        wb.sheets["Aux_Anterior"]["A1"].options(
            index=False
        ).value = resultados_anteriores.pipe(mantener_formato_columnas).to_pandas()
        wb.macro("formatear_tablas_resumen")(
            "Aux_Anterior", resultados_anteriores.shape[0]
        )

    wb.sheets["Aux_Totales"]["A1"].options(index=False).value = diagonales.pipe(
        mantener_formato_columnas
    ).to_pandas()
    wb.sheets["Atipicos"]["A1"].options(index=False).value = atipicos.pipe(
        mantener_formato_columnas
    ).to_pandas()

    wb.macro("formatear_tablas_resumen")("Aux_Totales", diagonales.shape[0])
    wb.macro("formatear_tablas_resumen")("Atipicos", atipicos.shape[0])


def generar_hoja_entremes(
    wb: xw.Book,
    tabla_entremes: pl.DataFrame,
    resultados_anteriores: pl.DataFrame,
    factores_completitud: pl.DataFrame,
    mes_corte: int,
) -> None:
    wb.sheets["Plantilla_Entremes"].clear()
    columnas_base = [
        "apertura_reservas",
        "periodicidad_ocurrencia",
        "periodo_ocurrencia",
    ]

    resultados_mes_anterior = (
        resultados_anteriores.filter(
            (pl.col("mes_corte") == utils.mes_anterior_corte(mes_corte))
            & (pl.col("atipico") == 0)
        )
        .select(columnas_base + ct.COLUMNAS_ULTIMATE)
        .rename({col: f"{col}_anterior" for col in ct.COLUMNAS_ULTIMATE})
    )

    tabla_entremes = (
        tabla_entremes.join(
            resultados_mes_anterior, on=columnas_base, how="left", validate="1:1"
        )
        .join(
            obtener_resultados_ultimo_triangulo(resultados_anteriores),
            on=columnas_base,
            how="left",
            validate="1:1",
        )
        .join(
            factores_completitud,
            on=["apertura_reservas", "periodicidad_ocurrencia", "periodo_ocurrencia"],
            how="left",
            validate="1:1",
        )
        .sort(["apertura_reservas", "periodo_ocurrencia"])
    )

    wb.sheets["Plantilla_Entremes"]["A1"].options(
        index=False
    ).value = tabla_entremes.to_pandas()
    wb.macro("preparar_Plantilla_Entremes")(tabla_entremes.shape[0])
    wb.macro("formatear_tabla_entremes")(tabla_entremes.shape[0])
    wb.macro("vincular_ultimates_entremes")(tabla_entremes.shape[0])


def obtener_resultados_ultimo_triangulo(
    resultados_anteriores: pl.DataFrame,
) -> pl.DataFrame:
    return (
        resultados_anteriores.with_columns(
            periodicidad=pl.col("periodicidad_ocurrencia")
            .replace(ct.PERIODICIDADES)
            .cast(pl.Int32)
        )
        .with_columns(
            mes_ultimo_triangulo=(
                pl.when(pl.col("mes_corte") % 100 < pl.col("periodicidad"))
                .then((pl.col("mes_corte") // 100 - 1) * 100 + 12)
                .otherwise(
                    (pl.col("mes_corte") // 100) * 100
                    + (pl.col("mes_corte") % 100).floordiv(pl.col("periodicidad"))
                    * pl.col("periodicidad")
                )
            ),
            velocidad_pago_bruto_triangulo=pl.col("pago_bruto")
            / pl.col("plata_ultimate_bruto"),
            velocidad_incurrido_bruto_triangulo=pl.col("incurrido_bruto")
            / pl.col("plata_ultimate_bruto"),
            velocidad_pago_retenido_triangulo=pl.col("pago_retenido")
            / pl.col("plata_ultimate_retenido"),
            velocidad_incurrido_retenido_triangulo=pl.col("incurrido_retenido")
            / pl.col("plata_ultimate_retenido"),
        )
        .filter(
            (pl.col("mes_corte") == pl.col("mes_ultimo_triangulo"))
            & (pl.col("atipico") == 0)
        )
        .select(
            [
                "apertura_reservas",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "velocidad_pago_bruto_triangulo",
                "velocidad_incurrido_bruto_triangulo",
                "velocidad_pago_retenido_triangulo",
                "velocidad_incurrido_retenido_triangulo",
            ]
        )
    )

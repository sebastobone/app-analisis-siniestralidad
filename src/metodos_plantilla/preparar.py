import time

import polars as pl
import xlwings as xw

from src import constantes as ct
from src import utils
from src.logger_config import logger
from src.metodos_plantilla import resultados, tablas_resumen
from src.models import Parametros

from .completar_diagonal import factor_completitud as compl


def preparar_plantilla(wb: xw.Book, p: Parametros) -> None:
    s = time.time()

    aperturas = utils.obtener_aperturas(p.negocio, "siniestros")

    limpiar_plantillas(wb)
    mostrar_plantillas_relevantes(wb, p.tipo_analisis)

    insumos = {
        "base_triangulos": pl.scan_parquet("data/processed/base_triangulos.parquet"),
        "base_ult_ocurr": pl.scan_parquet(
            "data/processed/base_ultima_ocurrencia.parquet"
        ),
        "base_atipicos": pl.scan_parquet("data/processed/base_atipicos.parquet"),
        "primas": pl.scan_parquet("data/processed/primas.parquet"),
        "expuestos": pl.scan_parquet("data/processed/expuestos.parquet"),
    }

    resumen, atipicos, entremes = tablas_resumen.generar_tablas_resumen(
        insumos, p.negocio, p.tipo_analisis, aperturas.lazy()
    )
    resultados_anteriores = resultados.concatenar_archivos_resultados()

    generar_hojas_resumen(wb, resumen, resultados_anteriores, atipicos)

    if p.tipo_analisis == "entremes":
        verificar_resultados_anteriores_para_entremes(
            resumen, resultados_anteriores, p.mes_corte
        )
        factores_completitud = compl.calcular_factores_completitud(
            aperturas.lazy(), p.mes_corte
        )
        generar_hoja_entremes(
            wb, entremes, resultados_anteriores, factores_completitud, p.mes_corte
        )

    logger.success("Plantilla preparada.")
    logger.info(f"Tiempo de preparacion: {round(time.time() - s, 2)} segundos.")


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


def limpiar_plantillas(wb: xw.Book):
    for hoja in ["Frecuencia", "Severidad", "Plata", "Entremes", "Completar_diagonal"]:
        wb.macro("LimpiarPlantilla")(hoja)


def mostrar_plantillas_relevantes(wb: xw.Book, tipo_analisis: str):
    if tipo_analisis == "triangulos":
        wb.sheets["Entremes"].visible = False
        wb.sheets["Completar_diagonal"].visible = False
        wb.sheets["Indexaciones"].visible = True
        for plantilla in ["frecuencia", "severidad", "plata"]:
            wb.sheets[plantilla].visible = True

    elif tipo_analisis == "entremes":
        wb.sheets["Entremes"].visible = True
        wb.sheets["Completar_diagonal"].visible = True
        wb.sheets["Indexaciones"].visible = False
        for plantilla in ["frecuencia", "severidad", "plata"]:
            wb.sheets[plantilla].visible = False


def generar_hojas_resumen(
    wb: xw.Book,
    resumen: pl.DataFrame,
    resultados_anteriores: pl.DataFrame,
    atipicos: pl.DataFrame,
) -> None:
    for sheet in ["Resumen", "Atipicos", "Historico"]:
        wb.sheets[sheet].clear()

    if resultados_anteriores.shape[0] != 0:
        wb.sheets["Historico"]["A1"].options(
            index=False
        ).value = resultados_anteriores.pipe(
            utils.mantener_formato_columnas
        ).to_pandas()
        wb.macro("FormatearTablaResumen")("Historico")

    wb.sheets["Resumen"]["A1"].options(index=False).value = resumen.pipe(
        utils.mantener_formato_columnas
    ).to_pandas()
    wb.sheets["Atipicos"]["A1"].options(index=False).value = atipicos.pipe(
        utils.mantener_formato_columnas
    ).to_pandas()

    wb.macro("FormatearTablaResumen")("Resumen")
    wb.macro("AgregarColumnasResumen")()
    wb.macro("FormatearTablaResumen")("Atipicos")


def generar_hoja_entremes(
    wb: xw.Book,
    tabla_entremes: pl.DataFrame,
    resultados_anteriores: pl.DataFrame,
    factores_completitud: pl.DataFrame,
    mes_corte: int,
) -> None:
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
            obtener_resultados_ultimo_triangulo(resultados_anteriores, mes_corte),
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

    wb.sheets["Entremes"]["A1"].options(index=False).value = tabla_entremes.to_pandas()
    wb.macro("FormatearTablaResumen")("Entremes")
    wb.macro("PrepararEntremes")()
    wb.macro("VincularUltimatesEntremes")()


def obtener_resultados_ultimo_triangulo(
    resultados_anteriores: pl.DataFrame,
    mes_corte: int,
) -> pl.DataFrame:
    return (
        resultados_anteriores.with_columns(
            periodicidad=pl.col("periodicidad_ocurrencia")
            .replace(ct.PERIODICIDADES)
            .cast(pl.Int32)
        )
        .filter(
            (pl.col("tipo_analisis") == "triangulos")
            & (pl.col("atipico") == 0)
            & (pl.col("mes_corte") < mes_corte)
        )
        .with_columns(
            velocidad_pago_bruto_triangulo=pl.col("pago_bruto")
            / pl.col("plata_ultimate_bruto"),
            velocidad_incurrido_bruto_triangulo=pl.col("incurrido_bruto")
            / pl.col("plata_ultimate_bruto"),
            velocidad_pago_retenido_triangulo=pl.col("pago_retenido")
            / pl.col("plata_ultimate_retenido"),
            velocidad_incurrido_retenido_triangulo=pl.col("incurrido_retenido")
            / pl.col("plata_ultimate_retenido"),
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


def verificar_plantilla_preparada(wb: xw.Book):
    resumen = utils.sheet_to_dataframe(wb, "Resumen")
    if resumen.is_empty():
        raise PlantillaNoPreparadaError(
            "La plantilla no ha sido preparada. Preparela y vuelva a intentar."
        )


class PlantillaNoPreparadaError(Exception):
    pass

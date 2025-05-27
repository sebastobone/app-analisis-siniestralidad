import time

import polars as pl
import xlwings as xw

from src import constantes as ct
from src import utils
from src.logger_config import logger
from src.metodos_plantilla import resultados, tablas_resumen
from src.models import Parametros, ReferenciasEntremes

from .completar_diagonal import factor_completitud as compl

COLUMNAS_BASE = ["apertura_reservas", "periodicidad_ocurrencia", "periodo_ocurrencia"]


def preparar_plantilla(
    wb: xw.Book, p: Parametros, referencias_entremes: ReferenciasEntremes
) -> None:
    s = time.time()

    aperturas = utils.obtener_aperturas(p.negocio, "siniestros")

    limpiar_plantillas(wb)
    mostrar_plantillas_relevantes(wb, p.tipo_analisis)

    insumos = {
        "base_triangulos": pl.scan_parquet("data/processed/base_triangulos.parquet"),
        "base_ult_ocurr": pl.scan_parquet(
            "data/processed/base_ultima_ocurrencia.parquet"
        ),
        "primas": pl.scan_parquet("data/processed/primas.parquet"),
        "expuestos": pl.scan_parquet("data/processed/expuestos.parquet"),
    }

    resumen, atipicos, entremes = tablas_resumen.generar_tablas_resumen(
        insumos, p.negocio, p.tipo_analisis, aperturas.lazy()
    )
    resultados_anteriores = resultados.concatenar_archivos_resultados()

    generar_hojas_resumen(wb, resumen, resultados_anteriores, atipicos)

    if p.tipo_analisis == "entremes":
        resultados_mes_anterior = obtener_resultados_mes_anterior(
            resultados_anteriores, p.mes_corte, referencias_entremes
        )
        comparar_aperturas_mes_anterior(resumen, resultados_mes_anterior)
        factores_completitud = compl.calcular_factores_completitud(
            aperturas.lazy(), p.mes_corte
        )
        entremes = complementar_tabla_entremes(
            entremes,
            resultados_anteriores,
            resultados_mes_anterior,
            factores_completitud,
            p.mes_corte,
        )
        generar_hoja_entremes(wb, entremes)

    logger.success("Plantilla preparada.")
    logger.info(f"Tiempo de preparacion: {round(time.time() - s, 2)} segundos.")


def verificar_existencia_analisis_anteriores(
    resultados_mes_anterior: pl.DataFrame,
) -> None:
    mes_corte_anterior = resultados_mes_anterior.get_column("mes_corte").unique().item()
    if resultados_mes_anterior.shape[0] == 0:
        raise ValueError(
            utils.limpiar_espacios_log(
                f"""
                No se encontraron resultados anteriores
                para el mes {mes_corte_anterior}. Se necesitan
                para hacer el analisis de entremes.
                """
            )
        )


def obtener_resultados_mes_anterior(
    resultados_anteriores: pl.DataFrame,
    mes_corte: int,
    referencias_entremes: ReferenciasEntremes,
) -> pl.DataFrame:
    mes_corte_anterior = utils.mes_anterior_corte(mes_corte)
    resultados_mes_anterior = resultados_anteriores.filter(
        pl.col("mes_corte") == mes_corte_anterior
    )

    resultado_actuarial = resultados_mes_anterior.filter(
        pl.col("tipo_analisis") == referencias_entremes.referencia_actuarial
    )
    resultado_contable = resultados_mes_anterior.filter(
        pl.col("tipo_analisis") == referencias_entremes.referencia_contable
    )

    if referencias_entremes.referencia_actuarial == "entremes":
        resultado_actuarial = agrupar_entremes_anterior(resultado_actuarial)

    if referencias_entremes.referencia_contable == "entremes":
        resultado_contable = agrupar_entremes_anterior(resultado_contable)

    return (
        resultado_actuarial.drop(
            [col for col in ct.COLUMNAS_ULTIMATE if "contable" in col]
        )
        .join(
            resultado_contable.drop(
                [col for col in ct.COLUMNAS_ULTIMATE if "contable" not in col]
            ),
            on=COLUMNAS_BASE + ["atipico"],
            how="outer",
            validate="1:1",
            coalesce=True,
        )
        .select(COLUMNAS_BASE + ["atipico"] + ct.COLUMNAS_ULTIMATE)
    )


def agrupar_entremes_anterior(entremes_anterior: pl.DataFrame) -> pl.DataFrame:
    periodicidades_inverso = {v: k for k, v in ct.PERIODICIDADES.items()}
    periodicidades_maximas = (
        entremes_anterior.with_columns(
            numero_meses_periodicididad=pl.col("periodicidad_ocurrencia")
            .replace(ct.PERIODICIDADES)
            .cast(pl.Int32),
        )
        .group_by("apertura_reservas")
        .agg(
            pl.max("numero_meses_periodicididad"),
            pl.max("periodo_ocurrencia").alias("ocurrencia_maxima"),
        )
        .with_columns(
            periodicidad_maxima=pl.col("numero_meses_periodicididad")
            .cast(pl.String)
            .replace(periodicidades_inverso)
        )
    )
    print(periodicidades_maximas)
    return (
        entremes_anterior.join(periodicidades_maximas, on="apertura_reservas")
        .with_columns(
            agrupar=(pl.col("periodicidad_ocurrencia") == "Mensual")
            & (
                pl.col("mes_corte").mod(100).mod(pl.col("numero_meses_periodicididad"))
                == 0
            )
            & ~(pl.col("periodicidad_maxima") == "Mensual")
        )
        .with_columns(
            periodicidad_ocurrencia=pl.when(pl.col("agrupar"))
            .then(pl.col("periodicidad_maxima"))
            .otherwise(pl.col("periodicidad_ocurrencia")),
            periodo_ocurrencia=pl.when(pl.col("agrupar"))
            .then(pl.col("ocurrencia_maxima"))
            .otherwise(pl.col("periodo_ocurrencia")),
        )
        .group_by(COLUMNAS_BASE + ["atipico"])
        .agg(
            [pl.mean(col) for col in ct.COLUMNAS_ULTIMATE[:3]]
            + [pl.sum(col) for col in ct.COLUMNAS_ULTIMATE[3:]]
        )
        .sort(COLUMNAS_BASE + ["atipico"])
    )


def comparar_aperturas_mes_anterior(
    diagonales: pl.DataFrame, resultados_mes_anterior: pl.DataFrame
) -> None:
    aperturas_actuales = sorted(
        diagonales.get_column("apertura_reservas").unique().to_list()
    )
    aperturas_anteriores = sorted(
        resultados_mes_anterior.get_column("apertura_reservas").unique().to_list()
    )
    if aperturas_actuales != aperturas_anteriores:
        raise ValueError(
            utils.limpiar_espacios_log(
                f"""
                Las aperturas no coinciden con el analisis anterior,
                el cual se necesita para el analisis de entremes. Si realizo
                un cambio a las aperturas con las que quiere hacer el analisis,
                modifique los resultados anteriores y vuelva a intentar.
                Aperturas actuales: {aperturas_actuales}.
                Aperturas anteriores: {aperturas_anteriores}.
                """
            )
        )


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


def complementar_tabla_entremes(
    tabla_entremes: pl.DataFrame,
    resultados_anteriores: pl.DataFrame,
    resultados_mes_anterior: pl.DataFrame,
    factores_completitud: pl.DataFrame,
    mes_corte: int,
) -> pl.DataFrame:
    resultados_mes_anterior = (
        resultados_mes_anterior.filter(pl.col("atipico") == 0)
        .select(COLUMNAS_BASE + ct.COLUMNAS_ULTIMATE)
        .rename({col: f"{col}_anterior" for col in ct.COLUMNAS_ULTIMATE})
    )

    return (
        tabla_entremes.join(
            resultados_mes_anterior, on=COLUMNAS_BASE, how="left", validate="1:1"
        )
        .join(
            obtener_resultados_ultimo_triangulo(resultados_anteriores, mes_corte),
            on=COLUMNAS_BASE,
            how="left",
            validate="1:1",
        )
        .join(factores_completitud, on=COLUMNAS_BASE, how="left", validate="1:1")
        .sort(["apertura_reservas", "periodo_ocurrencia"])
    )


def generar_hoja_entremes(
    wb: xw.Book,
    tabla_entremes: pl.DataFrame,
) -> None:
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
        .filter(pl.col("mes_corte") == pl.col("mes_corte").max())
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

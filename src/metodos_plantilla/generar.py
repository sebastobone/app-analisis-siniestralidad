import time

import polars as pl
import xlwings as xw

import src.constantes as ct
from src import utils
from src.logger_config import logger
from src.metodos_plantilla import actualizar, indexaciones, resultados
from src.models import ModosPlantilla, Parametros


def generar_plantillas(wb: xw.Book, p: Parametros, modos: ModosPlantilla):
    if modos.plantilla == "severidad":
        modos_frec = modos.model_copy(
            update={"plantilla": "frecuencia", "atributo": "bruto"}
        )
        try:
            actualizar.actualizar_plantilla(wb, p, modos_frec)
        except (
            actualizar.PlantillaNoGeneradaError,
            actualizar.PeriodicidadDiferenteError,
        ):
            generar_plantilla(wb, p, modos_frec)

        tipo_indexacion, _ = utils.obtener_parametros_indexacion(
            p.negocio, modos.apertura
        )
        if tipo_indexacion == "Por fecha de movimiento":
            indexaciones.traer_frecuencia_indexacion_movimiento(wb, p, modos_frec)

    generar_plantilla(wb, p, modos)


def generar_plantilla(wb: xw.Book, p: Parametros, modos: ModosPlantilla) -> None:
    s = time.time()

    hoja = wb.sheets[modos.plantilla.capitalize()]
    wb.macro("LimpiarPlantilla")(hoja.name)

    cantidades = (
        ["pago", "incurrido"]
        if hoja.name != "Frecuencia"
        else ["conteo_pago", "conteo_incurrido"]
    )

    aperturas = utils.obtener_aperturas(p.negocio, "siniestros")

    triangulo = crear_triangulo_base_plantilla(
        pl.scan_parquet("data/processed/base_triangulos.parquet"),
        modos.apertura,
        modos.atributo,
        aperturas,
        cantidades,
    )

    hoja.cells(ct.FILA_INI_PLANTILLAS + 1, ct.COL_OCURRS_PLANTILLAS).value = triangulo

    num_ocurrencias = triangulo.shape[0]
    num_alturas = triangulo.shape[1] // len(cantidades)
    mes_del_periodo = utils.mes_del_periodo(p.mes_corte, num_ocurrencias, num_alturas)
    periodicidad_apertura = (
        aperturas.filter(pl.col("apertura_reservas") == modos.apertura)
        .get_column("periodicidad_ocurrencia")
        .item(0)
    )
    num_meses_periodo = ct.PERIODICIDADES[periodicidad_apertura]

    mes_ultimos_resultados = obtener_mes_ultimos_resultados(modos.apertura)

    if modos.plantilla == "severidad":
        tipo_indexacion, medida_indexacion = utils.obtener_parametros_indexacion(
            p.negocio, modos.apertura
        )
        indexaciones.validar_medida_indexacion(
            wb.sheets["Indexaciones"], tipo_indexacion, medida_indexacion
        )
    else:
        tipo_indexacion, medida_indexacion = "Ninguna", "Ninguna"

    wb.macro(f"Generar{hoja.name}")(
        num_ocurrencias,
        num_alturas,
        ct.HEADER_TRIANGULOS,
        ct.SEP_TRIANGULOS,
        ct.FILA_INI_PLANTILLAS,
        ct.COL_OCURRS_PLANTILLAS,
        modos.apertura,
        modos.atributo,
        mes_del_periodo,
        num_meses_periodo,
        mes_ultimos_resultados,
        tipo_indexacion,
        medida_indexacion,
    )

    logger.success(
        f"""Plantilla {hoja.name} generada para {modos.apertura} - {modos.atributo}."""
    )
    logger.info(f"Tiempo de generacion: {round(time.time() - s, 2)} segundos.")


def crear_triangulo_base_plantilla(
    base_triangulos: pl.LazyFrame,
    apertura: str,
    atributo: str,
    aperturas: pl.DataFrame,
    cantidades: list[str],
) -> pl.DataFrame:
    return (
        base_triangulos.filter(
            (pl.col("apertura_reservas") == apertura) & (pl.col("atipico") == 0)
        )
        .join(
            aperturas.select(["apertura_reservas", "periodicidad_ocurrencia"]).lazy(),
            on=["apertura_reservas", "periodicidad_ocurrencia"],
        )
        .drop(["atipico", "diagonal", "periodo_desarrollo"])
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
        .pivot(
            on=["cantidad", "index_desarrollo"],
            index="periodo_ocurrencia",
            values="valor",
        )
    )


def obtener_mes_ultimos_resultados(apertura: str) -> int:
    resultados_anteriores = resultados.concatenar_archivos_resultados()
    if resultados_anteriores.shape[0] != 0:
        mes_ultimos_resultados = (
            resultados_anteriores.filter(
                (pl.col("apertura_reservas") == apertura)
                & (pl.col("tipo_analisis") == "entremes")
            )
            .get_column("mes_corte")
            .max()
        )
    else:
        mes_ultimos_resultados = 0

    if mes_ultimos_resultados is None:
        mes_ultimos_resultados = 0

    return mes_ultimos_resultados  # type: ignore

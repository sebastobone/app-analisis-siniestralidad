import time

import pandas as pd
import polars as pl
import xlwings as xw

import src.constantes as ct
from src import utils
from src.logger_config import logger
from src.metodos_plantilla import insumos as ins
from src.models import ModosPlantilla


def generar_plantilla(
    wb: xw.Book, negocio: str, modos: ModosPlantilla, mes_corte: int
) -> None:
    s = time.time()

    hoja_plantilla = modos.plantilla.capitalize()
    wb.macro("LimpiarPlantilla")(hoja_plantilla)

    cantidades = (
        ["pago", "incurrido"]
        if hoja_plantilla != "Frecuencia"
        else ["conteo_pago", "conteo_incurrido"]
    )

    aperturas = utils.obtener_aperturas(negocio, "siniestros")

    triangulo = crear_triangulo_base_plantilla(
        modos.apertura, modos.atributo, aperturas, cantidades
    )

    wb.sheets[hoja_plantilla].cells(
        ct.FILA_INI_PLANTILLAS, ct.COL_OCURRS_PLANTILLAS
    ).value = triangulo

    num_ocurrencias = triangulo.shape[0]
    num_alturas = triangulo.shape[1] // len(cantidades)
    mes_del_periodo = utils.mes_del_periodo(
        utils.yyyymm_to_date(mes_corte), num_ocurrencias, num_alturas
    )

    wb.macro(f"Generar{hoja_plantilla}")(
        num_ocurrencias,
        num_alturas,
        ct.HEADER_TRIANGULOS,
        ct.SEP_TRIANGULOS,
        ct.FILA_INI_PLANTILLAS,
        ct.COL_OCURRS_PLANTILLAS,
        modos.apertura,
        modos.atributo,
        mes_del_periodo,
    )

    logger.success(
        f"""{hoja_plantilla} generada para {modos.apertura} - {modos.atributo}."""
    )

    wb.sheets["Main"]["A1"].value = "GENERAR_PLANTILLA"
    wb.sheets["Main"]["A2"].value = time.time() - s


def crear_triangulo_base_plantilla(
    apertura: str,
    atributo: str,
    aperturas: pl.DataFrame,
    cantidades: list[str],
) -> pd.DataFrame:
    return (
        ins.df_triangulos()
        .filter(pl.col("apertura_reservas") == apertura)
        .join(
            aperturas.select(["apertura_reservas", "periodicidad_ocurrencia"]).lazy(),
            on=["apertura_reservas", "periodicidad_ocurrencia"],
        )
        .drop(["diagonal", "periodo_desarrollo"])
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
        .to_pandas()
        .pivot(
            index="periodo_ocurrencia",
            columns=["cantidad", "index_desarrollo"],
            values="valor",
        )
    )

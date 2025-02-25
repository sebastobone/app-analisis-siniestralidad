import time

import pandas as pd
import polars as pl
import xlwings as xw

import src.constantes as ct
from src import utils
from src.logger_config import logger
from src.metodos_plantilla import insumos as ins


def generar_plantilla(
    wb: xw.Book, plantilla: ct.LISTA_PLANTILLAS, mes_corte: int
) -> None:
    s = time.time()

    if plantilla != "completar_diagonal":
        plantilla_name = f"Plantilla_{plantilla.capitalize()}"
    else:
        plantilla_name = "Completar_diagonal"

    wb.macro("limpiar_plantilla")(plantilla_name)

    apertura = str(wb.sheets[plantilla_name]["C2"].value)
    atributo = str(wb.sheets[plantilla_name]["C3"].value).lower()
    cantidades = (
        ["pago", "incurrido"]
        if plantilla_name != "Plantilla_Frec"
        else ["conteo_pago", "conteo_incurrido"]
    )

    periodicidades = wb.sheets["Main"].tables["periodicidades"].data_body_range.value

    triangulo = crear_triangulo_base_plantilla(
        apertura, atributo, periodicidades, cantidades
    )

    wb.sheets[plantilla_name].cells(
        ct.FILA_INI_PLANTILLAS, ct.COL_OCURRS_PLANTILLAS
    ).value = triangulo

    num_ocurrencias = triangulo.shape[0]
    num_alturas = triangulo.shape[1] // len(cantidades)
    mes_del_periodo = utils.mes_del_periodo(
        utils.yyyymm_to_date(mes_corte), num_ocurrencias, num_alturas
    )

    wb.macro("formatear_parametro")("Main", "Mes del periodo", 6, 1)
    wb.sheets["Main"].range((6, 2)).value = mes_del_periodo

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

    logger.success(f"{plantilla_name} generada para {apertura} - {atributo}.")

    wb.sheets["Main"]["A1"].value = "GENERAR_PLANTILLA"
    wb.sheets["Main"]["A2"].value = time.time() - s


def crear_triangulo_base_plantilla(
    apertura: str,
    atributo: str,
    periodicidades: list[list[str]],
    cantidades: list[str],
) -> pd.DataFrame:
    return (
        ins.df_triangulos()
        .filter(pl.col("apertura_reservas") == apertura)
        .join(
            pl.LazyFrame(
                periodicidades,
                schema=["apertura_reservas", "periodicidad_ocurrencia"],
                orient="row",
            ),
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

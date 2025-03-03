import textwrap
from datetime import date
from math import ceil
from typing import Literal

import pandas as pd
import polars as pl
import xlwings as xw

from src import constantes as ct
from src.models import RangeDimension


def lowercase_columns(df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame | pl.DataFrame:
    return df.rename({column: column.lower() for column in df.collect_schema().names()})


def crear_columna_apertura_reservas(negocio: str) -> pl.Expr:
    return pl.concat_str(
        obtener_nombres_aperturas(negocio, "siniestros"),
        separator="_",
    ).alias("apertura_reservas")


def yyyymm_to_date(mes_yyyymm: int) -> date:
    return date(mes_yyyymm // 100, mes_yyyymm % 100, 1)


def date_to_yyyymm(mes_date: date, grain: str = "Mensual") -> int:
    return (
        mes_date.year * 100
        + ceil(mes_date.month / ct.PERIODICIDADES[grain]) * ct.PERIODICIDADES[grain]
    )


def date_to_yyyymm_pl(column: pl.Expr, grain: str = "Mensual") -> pl.Expr:
    return (
        column.dt.year() * 100
        + (column.dt.month() / pl.lit(ct.PERIODICIDADES[grain])).ceil()
        * pl.lit(ct.PERIODICIDADES[grain])
    ).cast(pl.Int32)


def min_cols_tera(tipo_query: str) -> list[str]:
    """Define las columnas minimas que tienen que salir de un query
    que consolida la informacion de primas, siniestros, o expuestos.
    """
    if tipo_query == "siniestros":
        return [
            "codigo_op",
            "codigo_ramo_op",
            "atipico",
            "fecha_siniestro",
            "fecha_registro",
            "conteo_pago",
            "conteo_incurrido",
            "conteo_desistido",
            "pago_bruto",
            "pago_retenido",
            "aviso_bruto",
            "aviso_retenido",
        ]
    elif tipo_query == "primas":
        return [
            "codigo_op",
            "codigo_ramo_op",
            "fecha_registro",
            "prima_bruta",
            "prima_bruta_devengada",
            "prima_retenida",
            "prima_retenida_devengada",
        ]

    return [
        "codigo_op",
        "codigo_ramo_op",
        "fecha_registro",
        "expuestos",
        "vigentes",
    ]


def obtener_dimensiones_triangulo(sheet: xw.Sheet) -> RangeDimension:
    numero_ocurrencias = sheet.range(
        sheet.cells(
            ct.FILA_INI_PLANTILLAS + ct.HEADER_TRIANGULOS, ct.COL_OCURRS_PLANTILLAS
        ),
        sheet.cells(
            ct.FILA_INI_PLANTILLAS + ct.HEADER_TRIANGULOS, ct.COL_OCURRS_PLANTILLAS
        ).end("down"),
    ).count
    numero_alturas = (
        sheet.range(
            sheet.cells(
                ct.FILA_INI_PLANTILLAS + ct.HEADER_TRIANGULOS - 1,
                ct.COL_OCURRS_PLANTILLAS + 1,
            ),
            sheet.cells(
                ct.FILA_INI_PLANTILLAS + ct.HEADER_TRIANGULOS - 1,
                ct.COL_OCURRS_PLANTILLAS + 1,
            ).end("right"),
        ).count
        // 2
    )
    return RangeDimension(height=numero_ocurrencias, width=numero_alturas)


def mes_del_periodo(mes_corte: date, num_ocurrencias: int, num_alturas: int) -> int:
    periodicidad = ceil(num_alturas / num_ocurrencias)

    if mes_corte.month <= periodicidad:
        mes_periodo = mes_corte.month
    else:
        mes_periodo = int(mes_corte.strftime("%Y%m")) - (
            mes_corte.year * 100
            + ((mes_corte.month - 1) // periodicidad) * periodicidad
        )
    return mes_periodo


def sheet_to_dataframe(
    wb: xw.Book, sheet_name: str, schema: pl.Schema | None = None
) -> pl.DataFrame:
    return pl.from_pandas(
        wb.sheets[sheet_name]
        .cells(1, 1)
        .options(pd.DataFrame, header=1, index=False, expand="table")
        .value,
        schema_overrides=schema,
    )


def path_plantilla(wb: xw.Book) -> str:
    return wb.fullname.replace(wb.name, "")


def limpiar_espacios_log(log: str) -> str:
    return textwrap.dedent(log).replace("\n", " ").replace("\t", " ")


def mes_anterior_corte(mes_corte: int) -> int:
    return (
        mes_corte - 1 if mes_corte % 100 != 1 else ((mes_corte // 100) - 1) * 100 + 12
    )


def obtener_aperturas(
    negocio: str, cantidad: Literal["siniestros", "primas", "expuestos"]
) -> pl.DataFrame:
    return pl.read_excel(
        f"data/segmentacion_{negocio}.xlsx",
        sheet_name=f"Aperturas_{cantidad.capitalize()}",
    )


def obtener_nombres_aperturas(
    negocio: str, cantidad: Literal["siniestros", "primas", "expuestos"]
) -> list[str]:
    aperturas = obtener_aperturas(negocio, cantidad)
    if cantidad == "siniestros":
        aperturas = aperturas.drop(["apertura_reservas", "periodicidad_ocurrencia"])
    return aperturas.collect_schema().names()

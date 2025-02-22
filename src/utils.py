import textwrap
from datetime import date
from math import ceil

import pandas as pd
import polars as pl
import xlwings as xw

from src import constantes as ct


def lowercase_columns(df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame | pl.DataFrame:
    return df.rename({column: column.lower() for column in df.collect_schema().names()})


def complementar_col_ramo_desc() -> pl.Expr:
    return pl.concat_str(
        pl.col("codigo_op"),
        pl.col("codigo_ramo_op"),
        pl.col("ramo_desc"),
        separator=" - ",
    )


def col_apertura_reservas(negocio: str) -> pl.Expr:
    return pl.concat_str(columnas_aperturas(negocio), separator="_").alias(
        "apertura_reservas"
    )


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


def columnas_aperturas(negocio: str) -> list[str]:
    base = ["codigo_op", "codigo_ramo_op"]
    if negocio == "autonomia":
        return base + ["apertura_canal_desc", "apertura_amparo_desc"]
    elif negocio == "soat":
        return base + ["apertura_canal_desc", "apertura_amparo_desc", "tipo_vehiculo"]
    elif negocio == "mock":
        return base + ["apertura_1", "apertura_2"]
    return []


def min_cols_tera(tipo_query: str) -> list[str]:
    """Define las columnas minimas que tienen que salir de un query
    que consolida la informacion de primas, siniestros, o expuestos.
    """
    if tipo_query == "siniestros":
        return [
            "codigo_op",
            "codigo_ramo_op",
            "ramo_desc",
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
            "ramo_desc",
            "fecha_registro",
            "prima_bruta",
            "prima_bruta_devengada",
            "prima_retenida",
            "prima_retenida_devengada",
        ]

    return [
        "codigo_op",
        "codigo_ramo_op",
        "ramo_desc",
        "fecha_registro",
        "expuestos",
        "vigentes",
    ]


def num_ocurrencias(sheet: xw.Sheet) -> int:
    return sheet.range(
        sheet.cells(
            ct.FILA_INI_PLANTILLAS + ct.HEADER_TRIANGULOS, ct.COL_OCURRS_PLANTILLAS
        ),
        sheet.cells(
            ct.FILA_INI_PLANTILLAS + ct.HEADER_TRIANGULOS, ct.COL_OCURRS_PLANTILLAS
        ).end("down"),
    ).count


def num_alturas(sheet: xw.Sheet) -> int:
    return (
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
) -> pl.LazyFrame:
    return pl.from_pandas(
        wb.sheets[sheet_name]
        .cells(1, 1)
        .options(pd.DataFrame, header=1, index=False, expand="table")
        .value,
        schema_overrides=schema,
    ).lazy()


def path_plantilla(wb: xw.Book) -> str:
    return wb.fullname.replace(wb.name, "")


def limpiar_espacios_log(log: str) -> str:
    return textwrap.dedent(log).replace("\n", " ").replace("\t", " ")


def mes_anterior_corte(mes_corte: int) -> int:
    return (
        mes_corte - 1 if mes_corte % 100 != 1 else ((mes_corte // 100) - 1) * 100 + 12
    )

import datetime as dt
import io
import textwrap
from math import ceil
from pathlib import Path
from typing import Any, Literal, overload

import polars as pl
import xlsxwriter
import xlwings as xw

from src import constantes as ct
from src.logger_config import logger
from src.models import RangeDimension


@overload
def lowercase_columns(df: pl.LazyFrame) -> pl.LazyFrame: ...


@overload
def lowercase_columns(df: pl.DataFrame) -> pl.DataFrame: ...


def lowercase_columns(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    return df.rename({column: column.lower() for column in df.collect_schema().names()})


def crear_columna_apertura_reservas(
    negocio: str, cantidad: Literal["siniestros", "primas", "expuestos"]
) -> pl.Expr:
    return pl.concat_str(
        obtener_nombres_aperturas(negocio, cantidad),
        separator="_",
    ).alias("apertura_reservas")


def yyyymm_to_date(mes_yyyymm: int) -> dt.date:
    return dt.date(mes_yyyymm // 100, mes_yyyymm % 100, 1)


def date_to_yyyymm(mes_date: dt.date, grain: str = "Mensual") -> int:
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


def mes_del_periodo(mes_corte: dt.date, num_ocurrencias: int, num_alturas: int) -> int:
    periodicidad = ceil(num_alturas / num_ocurrencias)

    if mes_corte.month <= periodicidad:
        mes_periodo = mes_corte.month
    else:
        mes_periodo = int(mes_corte.strftime("%Y%m")) - (
            mes_corte.year * 100
            + ((mes_corte.month - 1) // periodicidad) * periodicidad
        )
    return mes_periodo


def sheet_to_dataframe(wb: xw.Book, sheet_name: str) -> pl.DataFrame:
    df = wb.sheets[sheet_name].cells(1, 1).expand().options(pl.DataFrame).value
    return generalizar_tipos_columnas_resultados(df)


def generalizar_tipos_columnas_resultados(df: pl.DataFrame) -> pl.DataFrame:
    tipos_numericos = [
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.Float32,
        pl.Float64,
        pl.Decimal,
    ]
    cols_textuales = [
        "apertura_reservas",
        "codigo_op",
        "codigo_ramo_op",
        "periodicidad_ocurrencia",
        "tipo_analisis",
    ]
    cols_numericas = set(
        col
        for col, dtype in df.schema.items()
        if dtype in tipos_numericos and col not in cols_textuales
    ) | {"periodo_ocurrencia", "atipico"}

    cols_df = df.collect_schema().names()

    return df.with_columns(
        pl.col([col for col in cols_df if col in cols_textuales]).cast(pl.String),
        pl.col([col for col in cols_df if col in cols_numericas]).cast(pl.Float64),
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
    aperturas = pl.read_excel(
        f"data/segmentacion_{negocio}.xlsx",
        sheet_name=f"Aperturas_{cantidad.capitalize()}",
    )
    if cantidad == "siniestros":
        aperturas = aperturas.drop(
            ["tipo_indexacion_severidad", "medida_indexacion_severidad"]
        )

    return aperturas


def obtener_nombres_aperturas(
    negocio: str, cantidad: Literal["siniestros", "primas", "expuestos"]
) -> list[str]:
    aperturas = obtener_aperturas(negocio, cantidad)
    if cantidad == "siniestros":
        aperturas = aperturas.drop(["apertura_reservas", "periodicidad_ocurrencia"])
    return aperturas.collect_schema().names()


def obtener_parametros_indexacion(
    negocio: str, apertura: str
) -> tuple[ct.TIPOS_INDEXACION, str]:
    parametros_indexacion = (
        pl.read_excel(
            f"data/segmentacion_{negocio}.xlsx", sheet_name="Aperturas_Siniestros"
        )
        .select(
            [
                "apertura_reservas",
                "tipo_indexacion_severidad",
                "medida_indexacion_severidad",
            ]
        )
        .filter(pl.col("apertura_reservas") == apertura)
    )

    tipo_indexacion = parametros_indexacion.get_column(
        "tipo_indexacion_severidad"
    ).item(0)
    medida_indexacion = parametros_indexacion.get_column(
        "medida_indexacion_severidad"
    ).item(0)

    return tipo_indexacion, medida_indexacion


def mantener_formato_columnas(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.concat_str(pl.lit("'"), pl.col(column)).alias(column)
            for column in ["codigo_op", "codigo_ramo_op"]
        ]
    )


def validar_subconjunto(
    subconjunto: list[str],
    conjunto: list[str],
    mensaje_error: str,
    variables_mensaje: dict[str, str | list[str]] | None,
    severidad: Literal["error", "alerta"],
) -> None:
    if not set(subconjunto).issubset(set(conjunto)):
        faltantes = set(subconjunto) - set(conjunto)
        if variables_mensaje:
            log = mensaje_error.format(**variables_mensaje, faltantes=faltantes)
        else:
            log = mensaje_error.format(faltantes=faltantes)
        if severidad == "error":
            raise ValueError(limpiar_espacios_log(log))
        else:
            logger.warning(limpiar_espacios_log(log))


def vaciar_directorio(directorio_path: str) -> None:
    directorio = Path(directorio_path)
    for file in directorio.iterdir():
        if file.is_file() and file.name != ".gitkeep":
            file.unlink()


def validar_unicidad(
    df: pl.DataFrame,
    mensaje: str,
    variables_mensaje: dict[str, Any],
    severidad: Literal["error", "alerta"],
) -> None:
    if df.height != df.unique().height:
        if severidad == "error":
            raise ValueError(mensaje.format(**variables_mensaje))
        else:
            logger.warning(mensaje.format(**variables_mensaje))


def validar_no_nulos(
    df: pl.DataFrame, mensaje: str, variables_mensaje: dict[str, Any]
) -> None:
    nulos = df.filter(pl.any_horizontal(pl.all().is_null()))
    if not nulos.is_empty():
        raise ValueError(mensaje.format(**variables_mensaje, nulos=nulos))


def crear_excel(hojas: dict[str, pl.DataFrame]) -> io.BytesIO:
    excel_buffer = io.BytesIO()

    with xlsxwriter.Workbook(excel_buffer) as writer:
        for hoja in list(hojas.keys()):
            hojas[hoja].write_excel(writer, worksheet=hoja)

    excel_buffer.seek(0)
    return excel_buffer

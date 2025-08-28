import textwrap
from datetime import date
from math import ceil
from typing import Literal, overload

import numpy as np
import polars as pl
import xlwings as xw

from src import constantes as ct
from src.models import RangeDimension


@overload
def lowercase_columns(df: pl.LazyFrame) -> pl.LazyFrame: ...


@overload
def lowercase_columns(df: pl.DataFrame) -> pl.DataFrame: ...


def lowercase_columns(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
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


def columnas_minimas_salida_tera(
    negocio: str, tipo_query: Literal["siniestros", "primas", "expuestos"]
) -> list[str]:
    if tipo_query == "siniestros":
        columnas_descriptoras_adicionales = [
            "atipico",
            "fecha_siniestro",
            "fecha_registro",
        ]
    else:
        columnas_descriptoras_adicionales = ["fecha_registro"]

    return (
        obtener_nombres_aperturas(negocio, tipo_query)
        + columnas_descriptoras_adicionales
        + ct.COLUMNAS_VALORES_TERADATA[tipo_query]
    )


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


def generar_mock_siniestros(rango_meses: tuple[date, date]) -> pl.DataFrame:
    num_rows = 100000
    return pl.DataFrame(
        {
            "codigo_op": np.random.choice(["01"], size=num_rows),
            "codigo_ramo_op": np.random.choice(["001", "002"], size=num_rows),
            "apertura_1": np.random.choice(["A", "B"], size=num_rows),
            "apertura_2": np.random.choice(["D", "E"], size=num_rows),
            "atipico": np.random.choice([0, 1], size=num_rows, p=[0.95, 0.05]),
            "fecha_siniestro": np.random.choice(
                pl.date_range(
                    rango_meses[0], rango_meses[1], interval="1mo", eager=True
                ),
                size=num_rows,
            ),
            "fecha_registro": np.random.choice(
                pl.date_range(
                    rango_meses[0], rango_meses[1], interval="1mo", eager=True
                ),
                size=num_rows,
            ),
            "pago_bruto": np.random.random(size=num_rows) * 1e8,
            "pago_retenido": np.random.random(size=num_rows) * 1e7,
            "aviso_bruto": np.random.random(size=num_rows) * 1e7,
            "aviso_retenido": np.random.random(size=num_rows) * 1e6,
            "conteo_pago": np.random.randint(0, 100, size=num_rows),
            "conteo_incurrido": np.random.randint(0, 110, size=num_rows),
            "conteo_desistido": np.random.randint(0, 10, size=num_rows),
        }
    ).with_columns(crear_columna_apertura_reservas("demo"))


def generar_mock_primas(rango_meses: tuple[date, date]) -> pl.DataFrame:
    num_rows = 10000
    return pl.DataFrame(
        {
            "codigo_op": np.random.choice(["01"], size=num_rows),
            "codigo_ramo_op": np.random.choice(["001", "002"], size=num_rows),
            "apertura_1": np.random.choice(["A", "B"], size=num_rows),
            "apertura_2": np.random.choice(["D", "E"], size=num_rows),
            "fecha_registro": np.random.choice(
                pl.date_range(
                    rango_meses[0], rango_meses[1], interval="1mo", eager=True
                ),
                size=num_rows,
            ),
            "prima_bruta": np.random.random(size=num_rows) * 1e10,
            "prima_retenida": np.random.random(size=num_rows) * 1e9,
            "prima_bruta_devengada": np.random.random(size=num_rows) * 1e10,
            "prima_retenida_devengada": np.random.random(size=num_rows) * 1e9,
        }
    ).with_columns(crear_columna_apertura_reservas("demo"))


def generar_mock_expuestos(rango_meses: tuple[date, date]) -> pl.DataFrame:
    num_rows = 10000
    return (
        pl.DataFrame(
            {
                "codigo_op": np.random.choice(["01"], size=num_rows),
                "codigo_ramo_op": np.random.choice(["001", "002"], size=num_rows),
                "apertura_1": np.random.choice(["A", "B"], size=num_rows),
                "apertura_2": np.random.choice(["D", "E"], size=num_rows),
                "fecha_registro": np.random.choice(
                    pl.date_range(
                        rango_meses[0], rango_meses[1], interval="1mo", eager=True
                    ),
                    size=num_rows,
                ),
                "expuestos": np.random.random(size=num_rows) * 1e6,
                "vigentes": np.random.random(size=num_rows) * 1e6,
            }
        )
        .with_columns(crear_columna_apertura_reservas("demo"))
        .group_by(
            [
                "apertura_reservas",
                "codigo_op",
                "codigo_ramo_op",
                "apertura_1",
                "apertura_2",
                "fecha_registro",
            ]
        )
        .mean()
    )


def mantener_formato_columnas(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.concat_str(pl.lit("'"), pl.col(column)).alias(column)
            for column in ["codigo_op", "codigo_ramo_op"]
        ]
    )

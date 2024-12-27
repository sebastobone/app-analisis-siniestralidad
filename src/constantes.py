import xlwings as xw
import polars as pl
import pandas as pd
from math import ceil
from datetime import date


CREDENCIALES_TERADATA = {
    "host": "teradata.suranet.com",
    "user": "sebatoec",
    "password": "47^07Ghia0+b",
}

PERIODICIDADES = {"Mensual": 1, "Trimestral": 3, "Semestral": 6, "Anual": 12}

MONTH_MAP = pl.DataFrame(
    {
        "Nombre_Mes": [
            "ENE",
            "FEB",
            "MAR",
            "ABR",
            "MAY",
            "JUN",
            "JUL",
            "AGO",
            "SEP",
            "OCT",
            "NOV",
            "DIC",
        ],
        "Mes": list(range(1, 13)),
    }
)

NOMBRE_MES = {
    mes: nombre_mes
    for (mes, nombre_mes) in zip(
        MONTH_MAP.get_column("Mes"), MONTH_MAP.get_column("Nombre_Mes")
    )
}


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
    """
    Define las columnas minimas que tienen que salir de un query
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


HEADER_TRIANGULOS = 2
SEP_TRIANGULOS = 2
COL_OCURRS_PLANTILLAS = 6
FILA_INI_PLANTILLAS = 7


def num_ocurrencias(sheet: xw.Sheet) -> int:
    return sheet.range(
        sheet.cells(FILA_INI_PLANTILLAS + HEADER_TRIANGULOS, COL_OCURRS_PLANTILLAS),
        sheet.cells(FILA_INI_PLANTILLAS + HEADER_TRIANGULOS, COL_OCURRS_PLANTILLAS).end(
            "down"
        ),
    ).count


def num_alturas(sheet: xw.Sheet) -> int:
    return (
        sheet.range(
            sheet.cells(
                FILA_INI_PLANTILLAS + HEADER_TRIANGULOS - 1, COL_OCURRS_PLANTILLAS + 1
            ),
            sheet.cells(
                FILA_INI_PLANTILLAS + HEADER_TRIANGULOS - 1, COL_OCURRS_PLANTILLAS + 1
            ).end("right"),
        ).count
        // 2
    )


def mes_del_periodo(mes_corte: date, num_ocurrencias: int, num_alturas: int) -> int:
    periodicidad = ceil(num_alturas / num_ocurrencias)

    if mes_corte.month < periodicidad:
        mes_periodo = mes_corte.month
    else:
        mes_periodo = int(mes_corte.strftime("%Y%m")) - (
            mes_corte.year * 100 + (mes_corte.month // periodicidad) * periodicidad
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

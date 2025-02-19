from typing import Literal

import polars as pl

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
        MONTH_MAP.get_column("Mes"), MONTH_MAP.get_column("Nombre_Mes"), strict=False
    )
}


COLUMNAS_QTYS = [
    "pago_bruto",
    "pago_retenido",
    "incurrido_bruto",
    "incurrido_retenido",
    "conteo_pago",
    "conteo_incurrido",
    "conteo_desistido",
]


HEADER_TRIANGULOS = 2
SEP_TRIANGULOS = 2
COL_OCURRS_PLANTILLAS = 6
FILA_INI_PLANTILLAS = 2
FILA_INI_PARAMS = 4


LISTA_PLANTILLAS = Literal["frec", "seve", "plata", "entremes"]


COLORES_LOGS = {
    "INFO": "white",
    "SUCCESS": "lightgreen",
    "WARNING": "orange",
    "ERROR": "red",
    "CRITICAL": "red",
}

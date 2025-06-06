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

COLUMNAS_VALORES_TERADATA = {
    "siniestros": [
        "conteo_pago",
        "conteo_incurrido",
        "conteo_desistido",
        "pago_bruto",
        "pago_retenido",
        "aviso_bruto",
        "aviso_retenido",
    ],
    "primas": [
        "prima_bruta",
        "prima_bruta_devengada",
        "prima_retenida",
        "prima_retenida_devengada",
    ],
    "expuestos": ["expuestos", "vigentes"],
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

COLUMNAS_SINIESTROS_CUADRE = [
    "pago_bruto",
    "aviso_bruto",
    "pago_retenido",
    "aviso_retenido",
]

COLUMNAS_ULTIMATE = [
    "frecuencia_ultimate",
    "severidad_ultimate_bruto",
    "severidad_ultimate_retenido",
    "plata_ultimate_bruto",
    "plata_ultimate_contable_bruto",
    "plata_ultimate_retenido",
    "plata_ultimate_contable_retenido",
]


HEADER_TRIANGULOS = 2
SEP_TRIANGULOS = 2
COL_OCURRS_PLANTILLAS = 6
FILA_INI_PLANTILLAS = 2
FILA_INI_PARAMS = 4


LISTA_QUERIES_CUADRE = Literal["siniestros", "primas"]
LISTA_QUERIES = Literal["siniestros", "primas", "expuestos"]
LISTA_PLANTILLAS = Literal["frecuencia", "severidad", "plata", "completar_diagonal"]


COLORES_LOGS = {
    "INFO": "white",
    "SUCCESS": "lightgreen",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red",
}

AFOS_NECESARIOS = {
    "movilidad": ["Generales"],
    "autonomia": ["Generales", "Vida"],
    "salud": ["Vida"],
    "eps": ["EPS"],
    "arl": ["Vida"],
    "soat": ["Generales"],
    "empresariales": ["Generales"],
    "demo": ["AFO_Inexistente"],
}

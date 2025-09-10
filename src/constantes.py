from typing import Literal

import polars as pl
from pydantic import BaseModel

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


class DescriptoresSiniestros(BaseModel):
    codigo_op: type = pl.String
    codigo_ramo_op: type = pl.String
    atipico: type = pl.Int64
    fecha_siniestro: type = pl.Date
    fecha_registro: type = pl.Date


class ValoresSiniestros(BaseModel):
    conteo_pago: type = pl.Int64
    conteo_incurrido: type = pl.Int64
    conteo_desistido: type = pl.Int64
    pago_bruto: type = pl.Float64
    pago_retenido: type = pl.Float64
    aviso_bruto: type = pl.Float64
    aviso_retenido: type = pl.Float64


class DescriptoresPrimas(BaseModel):
    codigo_op: type = pl.String
    codigo_ramo_op: type = pl.String
    fecha_registro: type = pl.Date


class ValoresPrimas(BaseModel):
    prima_bruta: type = pl.Float64
    prima_bruta_devengada: type = pl.Float64
    prima_retenida: type = pl.Float64
    prima_retenida_devengada: type = pl.Float64


class DescriptoresExpuestos(BaseModel):
    codigo_op: type = pl.String
    codigo_ramo_op: type = pl.String
    fecha_registro: type = pl.Date


class ValoresExpuestos(BaseModel):
    expuestos: type = pl.Float64
    vigentes: type = pl.Float64


class Descriptores(BaseModel):
    siniestros: DescriptoresSiniestros = DescriptoresSiniestros()
    primas: DescriptoresPrimas = DescriptoresPrimas()
    expuestos: DescriptoresExpuestos = DescriptoresExpuestos()


class Valores(BaseModel):
    siniestros: ValoresSiniestros = ValoresSiniestros()
    primas: ValoresPrimas = ValoresPrimas()
    expuestos: ValoresExpuestos = ValoresExpuestos()


VALORES = Valores().model_dump()
DESCRIPTORES = Descriptores().model_dump()


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


CANTIDADES_CUADRE = Literal["siniestros", "primas"]
CANTIDADES = Literal["siniestros", "primas", "expuestos"]
LISTA_CANTIDADES: list[CANTIDADES] = ["siniestros", "primas", "expuestos"]
PLANTILLAS = Literal["frecuencia", "severidad", "plata", "completar_diagonal"]
TIPOS_INDEXACION = Literal[
    "Ninguna", "Por fecha de ocurrencia", "Por fecha de movimiento"
]


COLORES_LOGS = {
    "INFO": "white",
    "SUCCESS": "lightgreen",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red",
}

NUM_FILAS_DEMO = {"siniestros": 100000, "primas": 10000, "expuestos": 10000}

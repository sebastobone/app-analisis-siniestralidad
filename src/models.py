from datetime import date
from typing import Literal

from fastapi import UploadFile
from pydantic import BaseModel, field_serializer, field_validator
from sqlmodel import JSON, Column, Field, SQLModel, String

from src import constantes as ct


class Parametros(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    negocio: str
    mes_inicio: date
    mes_corte: date
    tipo_analisis: Literal["triangulos", "entremes"] = Field(sa_type=String)
    nombre_plantilla: str
    session_id: str | None = Field(index=True)

    @field_validator("nombre_plantilla", mode="after")
    @classmethod
    def evitar_nombrar_como_plantilla_base(cls, nombre: str) -> str:
        if nombre == "plantilla":
            raise ValueError(
                f"El nombre '{nombre}' no esta permitido, intente con otro."
            )
        return nombre

    @field_validator("mes_inicio", "mes_corte", mode="before")
    @classmethod
    def convertir_meses_inicio(cls, valor: str) -> date:
        # Todo lo que se manda por query parameters llega como string
        try:
            valor_int = int(valor)
        except ValueError as exc:
            raise ValueError(
                "Las fechas deben estar en formato YYYYMM (ej. 202002)"
            ) from exc

        anno, mes = validar_input_meses(valor_int)
        return date(anno, mes, 1)

    @field_serializer("mes_inicio", "mes_corte")
    def fechas_a_entero(self, valor: date) -> int:
        return valor.year * 100 + valor.month


def validar_input_meses(valor: int) -> tuple[int, int]:
    if valor > 999999 or valor < 100000:
        raise ValueError("Las fechas deben estar en formato YYYYMM (ej. 202002)")
    anno = valor // 100
    mes = valor % 100
    if anno < 1990 or anno > 2040:
        raise ValueError("El ano debe estar comprendido entre 1990 y 2040.")
    if mes < 1 or mes > 12:
        raise ValueError("El mes debe estar comprendido entre 1 y 12.")
    return anno, mes


class ModosPlantilla(BaseModel):
    apertura: str
    atributo: Literal["bruto", "retenido"]
    plantilla: Literal["frecuencia", "severidad", "plata", "completar_diagonal"]


class ReferenciasEntremes(BaseModel):
    referencia_actuarial: Literal["triangulos", "entremes"] = "entremes"
    referencia_contable: Literal["triangulos", "entremes"] = "entremes"


class Offset(BaseModel):
    y: int
    x: int


class RangeDimension(BaseModel):
    height: int
    width: int


class CredencialesTeradata(BaseModel):
    host: str = "teradata.suranet.com"
    user: str
    password: str


class ArchivosCantidades:
    def __init__(
        self,
        siniestros: list[UploadFile] | None = None,
        primas: list[UploadFile] | None = None,
        expuestos: list[UploadFile] | None = None,
    ):
        self.siniestros = siniestros
        self.primas = primas
        self.expuestos = expuestos


class MetadataCantidades(SQLModel, table=True):
    ruta: str = Field(primary_key=True)
    nombre_original: str
    origen: Literal["extraccion", "carga_manual", "cuadre_contable", "demo"] = Field(
        sa_type=String
    )
    cantidad: ct.CANTIDADES = Field(sa_type=String)
    numero_filas: int | None = None
    rutas_padres: list[str] | None = Field(sa_column=Column(JSON))


class SeleccionadosCuadre(BaseModel):
    siniestros: list[str]
    primas: list[str]

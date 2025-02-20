from typing import Literal

from pydantic import BaseModel
from sqlmodel import Field, SQLModel, String


class Parametros(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    negocio: str
    mes_inicio: int = Field(ge=199001, le=204001)
    mes_corte: int = Field(ge=199001, le=204001)
    tipo_analisis: Literal["triangulos", "entremes"] = Field(sa_type=String)
    aproximar_reaseguro: bool = False
    nombre_plantilla: str
    cuadre_contable_sinis: bool = False
    add_fraude_soat: bool = False
    cuadre_contable_primas: bool = False
    session_id: str | None = Field(index=True)


class ModosPlantilla(BaseModel):
    plantilla: Literal["frec", "seve", "plata", "entremes"]
    modo: Literal["generar", "guardar", "traer", "guardar_todo", "traer_guardar_todo"]


class Offset(BaseModel):
    y: int
    x: int


class RangeDimension(BaseModel):
    height: int
    width: int


class EstructuraApertura(BaseModel):
    apertura: str
    atributo: str
    dimensiones_triangulo: RangeDimension
    mes_del_periodo: int

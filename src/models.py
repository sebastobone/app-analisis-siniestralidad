from typing import Literal

from pydantic import BaseModel, field_validator
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

    @field_validator("nombre_plantilla", mode="after")
    @classmethod
    def evitar_nombrar_como_plantilla_base(cls, nombre: str) -> str:
        if nombre == "plantilla":
            raise ValueError(
                f"El nombre '{nombre}' no esta permitido, intente con otro."
            )
        return nombre


class ModosPlantilla(BaseModel):
    apertura: str
    atributo: Literal["bruto", "retenido"]
    plantilla: Literal["frecuencia", "severidad", "plata", "completar_diagonal"]


class Offset(BaseModel):
    y: int
    x: int


class RangeDimension(BaseModel):
    height: int
    width: int

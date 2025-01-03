from typing import Literal

from sqlmodel import Field, SQLModel, String


class Parametros(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    negocio: str
    mes_inicio: int
    mes_corte: int
    tipo_analisis: Literal["triangulos", "entremes"] = Field(sa_type=String)
    aproximar_reaseguro: bool
    nombre_plantilla: str
    cuadre_contable_sinis: bool
    add_fraude_soat: bool
    cuadre_contable_primas: bool
    session_id: str | None = Field(index=True)

from typing import Any, Literal

import polars as pl

from src.logger_config import logger
from src.utils import limpiar_espacios_log


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

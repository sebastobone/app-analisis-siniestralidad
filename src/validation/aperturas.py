from typing import Literal

import polars as pl

from src import constantes as ct
from src import utils
from src.logger_config import logger


def _validar_hojas(hojas: dict[str, pl.DataFrame]) -> None:
    hojas_necesarias = {
        "Aperturas_Siniestros",
        "Aperturas_Primas",
        "Aperturas_Expuestos",
    }
    hojas_encontradas = set(hojas.keys())
    if not hojas_necesarias.issubset(hojas_encontradas):
        faltantes = hojas_necesarias - hojas_encontradas
        raise ValueError(
            f"Faltan las siguientes hojas en el archivo de segmentacion: {faltantes}"
        )


def _validar_columnas(
    columnas_encontradas: set[str],
    columnas_necesarias: set[str],
    nombre_hoja: str,
    mensaje_error: str,
) -> None:
    if not columnas_necesarias.issubset(columnas_encontradas):
        faltantes = columnas_necesarias - columnas_encontradas
        raise ValueError(
            utils.limpiar_espacios_log(
                mensaje_error.format(nombre_hoja=nombre_hoja, faltantes=faltantes)
            )
        )


def _validar_unicidad(df: pl.DataFrame, columna: str, nombre_hoja: str) -> None:
    col = df.get_column(columna)
    if col.len() != col.unique().len():
        raise ValueError(
            utils.limpiar_espacios_log(
                f"""
                La columna "{columna}" en la hoja "{nombre_hoja}"
                contiene valores duplicados.
                """
            )
        )


def _validar_valores_permitidos(
    df: pl.DataFrame, columna: str, permitidos: set[str], nombre_hoja: str
) -> None:
    valores = set(df.get_column(columna).unique().to_list())
    if not valores.issubset(permitidos):
        raise ValueError(
            utils.limpiar_espacios_log(
                f"""
                La columna "{columna}" en la hoja "{nombre_hoja}"
                contiene valores invalidos. Los valores permitidos son:
                {", ".join(permitidos)}.
                """
            )
        )


def _validar_medidas_indexacion(df: pl.DataFrame) -> None:
    medidas_indexacion = (
        df.select(["tipo_indexacion_severidad", "medida_indexacion_severidad"])
        .unique()
        .with_columns(
            pl.when(
                (pl.col("tipo_indexacion_severidad") != "Ninguna")
                & (pl.col("medida_indexacion_severidad") == "Ninguna")
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("es_invalida")
        )
    )
    if medidas_indexacion.filter(pl.col("es_invalida")).height > 0:
        raise ValueError(
            utils.limpiar_espacios_log(
                """
                La columna "medida_indexacion_severidad" en la hoja 
                "Aperturas_Siniestros" contiene valores invalidos. Si 
                "tipo_indexacion_severidad" es diferente de "Ninguna",
                entonces "medida_indexacion_severidad" no puede ser "Ninguna".
                """
            )
        )


def _validar_cruces_aperturas(
    hojas: dict[str, pl.DataFrame],
    nombre_hoja: Literal["Aperturas_Primas", "Aperturas_Expuestos"],
) -> None:
    aperturas_siniestros = hojas["Aperturas_Siniestros"].drop(
        [
            "apertura_reservas",
            "periodicidad_ocurrencia",
            "tipo_indexacion_severidad",
            "medida_indexacion_severidad",
        ]
    )
    aperturas_primas = hojas["Aperturas_Primas"]

    cruce = aperturas_siniestros.join(
        aperturas_primas, on=aperturas_primas.collect_schema().names(), how="full"
    )
    num_nulls = cruce.null_count().max_horizontal().max()
    nulos = cruce.filter(pl.any_horizontal(pl.all().is_null()))
    if isinstance(num_nulls, int) and num_nulls > 0:
        raise ValueError(
            f"""
            No tiene las mismas aperturas en las hojas "Aperturas_Siniestros"
            y "{nombre_hoja}". Revise los valores que no cruzan: {nulos}
            """
        )


def validar_aperturas(hojas: dict[str, pl.DataFrame]) -> None:
    _validar_hojas(hojas)

    mensaje_columnas_faltantes = """
        Faltan las siguientes columnas en la hoja "{nombre_hoja}"
        del archivo de segmentacion: {faltantes}
    """

    # Validar siniestros
    columnas_necesarias_siniestros = {
        "apertura_reservas",
        "codigo_op",
        "codigo_ramo_op",
        "periodicidad_ocurrencia",
        "tipo_indexacion_severidad",
        "medida_indexacion_severidad",
    }
    columnas_encontradas_siniestros = set(
        hojas["Aperturas_Siniestros"].collect_schema().names()
    )

    _validar_columnas(
        columnas_encontradas_siniestros,
        columnas_necesarias_siniestros,
        "Aperturas_Siniestros",
        mensaje_columnas_faltantes,
    )
    _validar_unicidad(
        hojas["Aperturas_Siniestros"], "apertura_reservas", "Aperturas_Siniestros"
    )
    _validar_valores_permitidos(
        hojas["Aperturas_Siniestros"],
        "periodicidad_ocurrencia",
        set(ct.PERIODICIDADES.keys()),
        "Aperturas_Siniestros",
    )
    _validar_valores_permitidos(
        hojas["Aperturas_Siniestros"],
        "tipo_indexacion_severidad",
        {"Ninguna", "Por fecha de ocurrencia", "Por fecha de movimiento"},
        "Aperturas_Siniestros",
    )
    _validar_medidas_indexacion(hojas["Aperturas_Siniestros"])

    mensaje_variables_sobrantes = """
        Los variables de apertura de la hoja "{nombre_hoja}"
        deben estar contenidas en las variables de apertura de la hoja
        "Aperturas_Siniestros". Sobrantes: {faltantes}
    """

    # Validar primas y expuestos
    for nombre_hoja in ["Aperturas_Primas", "Aperturas_Expuestos"]:
        columnas_encontradas = set(hojas[nombre_hoja].collect_schema().names())

        _validar_columnas(
            columnas_encontradas,
            {"codigo_op", "codigo_ramo_op"},
            nombre_hoja,
            mensaje_columnas_faltantes,
        )
        _validar_columnas(
            columnas_encontradas_siniestros,
            columnas_encontradas,
            nombre_hoja,
            mensaje_variables_sobrantes,
        )
        _validar_cruces_aperturas(hojas, nombre_hoja)

    logger.info(
        "Validacion de aperturas en el archivo de segmentacion completada exitosamente."
    )

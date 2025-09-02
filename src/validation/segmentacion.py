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
            "periodicidad_ocurrencia",
            "tipo_indexacion_severidad",
            "medida_indexacion_severidad",
        ]
    )
    aperturas = hojas[nombre_hoja]

    cruce = aperturas_siniestros.join(
        aperturas, on=aperturas.collect_schema().names(), how="full"
    )
    utils.validar_no_nulos(
        cruce,
        """
        No tiene las mismas aperturas en las hojas "Aperturas_Siniestros"
        y {nombre_hoja}. Revise los valores que no cruzan: {nulos}
        """,
        {"nombre_hoja": nombre_hoja},
    )


def validar_archivo_segmentacion(hojas: dict[str, pl.DataFrame]) -> None:
    _validar_hojas(hojas)

    mensaje_columnas_faltantes = """
        Faltan las siguientes columnas en la hoja "{nombre_hoja}"
        del archivo de segmentacion: {faltantes}
    """

    mensaje_valores_no_permitidos = """
        La columna {columna} en la hoja {nombre_hoja} contiene valores invalidos.
        Se encontro {faltantes}, pero los valores permitidos son {permitidos}.
    """

    mensaje_aperturas_duplicadas = """
        La hoja {nombre_hoja} contiene aperturas duplicadas.
    """

    # Validar siniestros
    columnas_necesarias_siniestros = [
        "codigo_op",
        "codigo_ramo_op",
        "periodicidad_ocurrencia",
        "tipo_indexacion_severidad",
        "medida_indexacion_severidad",
    ]
    columnas_encontradas_siniestros = (
        hojas["Aperturas_Siniestros"].collect_schema().names()
    )

    utils.validar_subconjunto(
        columnas_necesarias_siniestros,
        columnas_encontradas_siniestros,
        mensaje_columnas_faltantes,
        {"nombre_hoja": "Aperturas_Siniestros"},
        "error",
    )
    utils.validar_unicidad(
        hojas["Aperturas_Siniestros"].drop(
            [
                "periodicidad_ocurrencia",
                "tipo_indexacion_severidad",
                "medida_indexacion_severidad",
            ]
        ),
        mensaje_aperturas_duplicadas,
        {"nombre_hoja": "Aperturas_Siniestros"},
        "error",
    )
    utils.validar_subconjunto(
        hojas["Aperturas_Siniestros"]
        .get_column("periodicidad_ocurrencia")
        .unique()
        .to_list(),
        list(ct.PERIODICIDADES.keys()),
        mensaje_valores_no_permitidos,
        {
            "columna": "periodicidad_ocurrencia",
            "nombre_hoja": "Aperturas_Siniestros",
            "permitidos": list(ct.PERIODICIDADES.keys()),
        },
        "error",
    )
    utils.validar_subconjunto(
        hojas["Aperturas_Siniestros"]
        .get_column("tipo_indexacion_severidad")
        .unique()
        .to_list(),
        ["Ninguna", "Por fecha de ocurrencia", "Por fecha de movimiento"],
        mensaje_valores_no_permitidos,
        {
            "columna": "tipo_indexacion_severidad",
            "nombre_hoja": "Aperturas_Siniestros",
            "permitidos": [
                "Ninguna",
                "Por fecha de ocurrencia",
                "Por fecha de movimiento",
            ],
        },
        "error",
    )
    _validar_medidas_indexacion(hojas["Aperturas_Siniestros"])

    mensaje_variables_sobrantes = """
        Los variables de apertura de la hoja "{nombre_hoja}"
        deben estar contenidas en las variables de apertura de la hoja
        "Aperturas_Siniestros". Sobrantes: {faltantes}
    """

    # Validar primas y expuestos
    for nombre_hoja in ["Aperturas_Primas", "Aperturas_Expuestos"]:
        columnas_encontradas = hojas[nombre_hoja].collect_schema().names()

        utils.validar_subconjunto(
            ["codigo_op", "codigo_ramo_op"],
            columnas_encontradas,
            mensaje_columnas_faltantes,
            {"nombre_hoja": nombre_hoja},
            "error",
        )
        utils.validar_subconjunto(
            columnas_encontradas,
            columnas_encontradas_siniestros,
            mensaje_variables_sobrantes,
            {"nombre_hoja": nombre_hoja},
            "error",
        )
        utils.validar_unicidad(
            hojas[nombre_hoja],
            mensaje_aperturas_duplicadas,
            {"nombre_hoja": nombre_hoja},
            "error",
        )
        _validar_cruces_aperturas(hojas, nombre_hoja)

    logger.info(
        "Validacion de aperturas en el archivo de segmentacion completada exitosamente."
    )

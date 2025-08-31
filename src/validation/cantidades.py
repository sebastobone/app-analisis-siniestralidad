import polars as pl

from src import constantes as ct
from src import utils
from src.logger_config import logger

MENSAJE_COLUMNAS_FALTANTES = """
    En el archivo {nombre_archivo} faltan las siguientes columnas: {faltantes}
"""

MENSAJE_APERTURAS_FALTANTES = """
    ¡Error! Las siguientes aperturas se encontraron en el archivo {nombre_archivo},
    pero no estan definidas: {faltantes}. Agregue estas aperturas al archivo de
    segmentacion.
"""

MENSAJE_APERTURAS_SOBRANTES = """
    ¡Alerta! Las siguientes aperturas se definieron, pero no se encuentran
    en el archivo {nombre_archivo}. Puede optar por quitarlas del archivo de
    segmentacion, o simplemente ignorarlas: {sobrantes}
"""


def validar_archivo(
    negocio: str, df: pl.DataFrame, filename: str, cantidad: ct.LISTA_CANTIDADES
) -> None:
    utils.validar_subconjunto(
        list(ct.Descriptores().model_dump()[cantidad].keys())
        + list(ct.Valores().model_dump()[cantidad].keys()),
        df.collect_schema().names(),
        MENSAJE_COLUMNAS_FALTANTES,
        {"nombre_archivo": filename},
        "error",
    )
    utils.validar_subconjunto(
        utils.obtener_nombres_aperturas(negocio, cantidad),
        df.collect_schema().names(),
        MENSAJE_COLUMNAS_FALTANTES,
        {"nombre_archivo": filename},
        "error",
    )
    _validar_descriptores_no_nulos(df, negocio, cantidad, filename)

    aperturas_encontradas = (
        df.select(utils.crear_columna_apertura_reservas(negocio, cantidad))
        .get_column("apertura_reservas")
        .unique()
        .to_list()
    )
    aperturas_definidas = (
        utils.obtener_aperturas(negocio, cantidad)
        .select(utils.crear_columna_apertura_reservas(negocio, cantidad))
        .get_column("apertura_reservas")
        .unique()
        .to_list()
    )

    utils.validar_subconjunto(
        aperturas_encontradas,
        aperturas_definidas,
        MENSAJE_APERTURAS_FALTANTES,
        {"nombre_archivo": filename},
        "error",
    )
    utils.validar_subconjunto(
        aperturas_definidas,
        aperturas_encontradas,
        MENSAJE_APERTURAS_SOBRANTES,
        {"nombre_archivo": filename},
        "alerta",
    )

    logger.info(f"Archivo {filename} validado exitosamente.")


def _validar_descriptores_no_nulos(
    df: pl.DataFrame, negocio: str, cantidad: ct.LISTA_CANTIDADES, filename: str
) -> None:
    columnas_descriptoras = set(ct.Valores().model_dump()[cantidad].keys())
    columnas_aperturas = set(utils.obtener_nombres_aperturas(negocio, cantidad))
    df = df.select(columnas_aperturas.union(columnas_descriptoras)).unique()

    nulos = df.filter(pl.any_horizontal(pl.all().is_null()))
    if not nulos.is_empty():
        raise ValueError(
            f"""
            Tiene valores nulos en el archivo {filename}: {nulos}.
            Corrija estos valores y vuelva a intentar.
            """
        )

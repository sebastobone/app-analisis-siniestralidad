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
    negocio: str, df: pl.DataFrame, filename: str, cantidad: ct.CANTIDADES
) -> None:
    utils.validar_subconjunto(
        list(ct.DESCRIPTORES[cantidad].keys()) + list(ct.VALORES[cantidad].keys()),
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
    df: pl.DataFrame, negocio: str, cantidad: ct.CANTIDADES, filename: str
) -> None:
    columnas_descriptoras = set(ct.VALORES[cantidad].keys())
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


def organizar_archivo(
    df: pl.DataFrame,
    negocio: str,
    mes_inicio: int,
    cantidad: ct.CANTIDADES,
    nombre_archivo: str,
) -> pl.DataFrame:
    df = (
        df.pipe(asignar_tipos_columnas, cantidad, nombre_archivo)
        .pipe(mensualizar, cantidad)
        .pipe(colapsar_primera_ocurrencia, cantidad, mes_inicio)
        .pipe(agrupar_por_columnas_relevantes, negocio, cantidad)
    )

    if cantidad == "siniestros":
        df = df.select(
            utils.crear_columna_apertura_reservas(negocio, cantidad), pl.all()
        )

    return df


def asignar_tipos_columnas(
    df: pl.DataFrame,
    cantidad: ct.CANTIDADES,
    nombre_archivo: str,
) -> pl.DataFrame:
    mensaje_error = f"""
        Ocurrio un error al transformar los tipos de datos del archivo
        {nombre_archivo}:
    """
    try:
        df = df.cast(ct.DESCRIPTORES[cantidad]).cast(ct.VALORES[cantidad])
    except pl.exceptions.InvalidOperationError as e:
        raise ValueError(utils.limpiar_espacios_log(f"{mensaje_error} {str(e)}")) from e
    return df


def mensualizar(df: pl.DataFrame, cantidad: ct.CANTIDADES) -> pl.DataFrame:
    columnas_descriptoras = set(ct.DESCRIPTORES[cantidad].keys())
    columnas_fechas = columnas_descriptoras.intersection(
        {"fecha_siniestro", "fecha_registro"}
    )

    return df.with_columns(
        [pl.col(col).dt.month_start().alias(col) for col in columnas_fechas]
    )


def colapsar_primera_ocurrencia(
    df: pl.DataFrame, cantidad: ct.CANTIDADES, mes_inicio: int
) -> pl.DataFrame:
    """
    Para que las diagonales del triangulo se puedan comparar
    con la cifra contable
    """
    columnas_descriptoras = set(ct.DESCRIPTORES[cantidad].keys())
    columnas_fechas = columnas_descriptoras.intersection(
        {"fecha_siniestro", "fecha_registro"}
    )
    fecha_inicio = utils.yyyymm_to_date(mes_inicio)
    return df.with_columns(
        [
            pl.col(col).clip(lower_bound=fecha_inicio).alias(col)
            for col in columnas_fechas
        ]
    )


def agrupar_por_columnas_relevantes(
    df: pl.DataFrame, negocio: str, cantidad: ct.CANTIDADES
) -> pl.DataFrame:
    columnas_descriptoras = list(ct.DESCRIPTORES[cantidad].keys())
    columnas_aperturas = utils.obtener_nombres_aperturas(negocio, cantidad)
    columnas_valores = list(ct.VALORES[cantidad].keys())

    # Deja una lista con elementos unicos conservando el orden
    columnas_group = list(dict.fromkeys(columnas_aperturas + columnas_descriptoras))
    return (
        df.group_by(columnas_group)
        .agg([pl.sum(col) for col in columnas_valores])
        .sort(columnas_group)
    )

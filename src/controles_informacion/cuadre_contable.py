import polars as pl

from src import constantes as ct
from src import utils
from src.logger_config import logger


def columnas_cantidades_cuadre(file: ct.CANTIDADES_CUADRE) -> list[str]:
    return (
        ct.COLUMNAS_SINIESTROS_CUADRE
        if file == "siniestros"
        else list(ct.Valores().model_dump()[file].keys())
    )


async def realizar_cuadre_contable(
    negocio: str,
    file: ct.CANTIDADES_CUADRE,
    base: pl.DataFrame,
    dif_sap_vs_tera: pl.DataFrame,
    meses_a_cuadrar: pl.DataFrame,
) -> pl.DataFrame:
    diferencias = (
        obtener_aperturas_para_asignar_diferencia(negocio, file)
        .pipe(calcular_pesos_aperturas, base, file, negocio)
        .pipe(repartir_diferencias, dif_sap_vs_tera, meses_a_cuadrar, file)
        .pipe(agregar_columnas_faltantes, file, negocio)
        .select(base.collect_schema().names())
    )

    base_cuadrada = agregar_diferencias(base, diferencias)
    logger.success("Cuadre contable realizado exitosamente.")
    return base_cuadrada


def obtener_aperturas_para_asignar_diferencia(
    negocio: str, file: ct.CANTIDADES_CUADRE
) -> pl.DataFrame:
    return pl.read_excel(
        f"data/segmentacion_{negocio}.xlsx",
        sheet_name=f"Cuadre_{file.capitalize()}",
    )


def calcular_pesos_aperturas(
    aperturas: pl.DataFrame,
    base: pl.DataFrame,
    file: ct.CANTIDADES_CUADRE,
    negocio: str,
) -> pl.DataFrame:
    columnas_aperturas = utils.obtener_nombres_aperturas(negocio, file)
    columnas_cantidades = columnas_cantidades_cuadre(file)
    return (
        base.join(aperturas, on=columnas_aperturas)
        .group_by(columnas_aperturas)
        .agg([pl.sum(col) for col in columnas_cantidades])
        .with_columns(
            [
                (pl.col(col) / pl.col(col).sum().over(["codigo_op", "codigo_ramo_op"]))
                .fill_nan(0)
                .alias(f"peso_{col}")
                for col in columnas_cantidades
            ]
        )
        .drop(columnas_cantidades)
    )


def repartir_diferencias(
    pesos_aperturas_diferencia: pl.DataFrame,
    dif_sap_vs_tera: pl.DataFrame,
    meses_a_cuadrar: pl.DataFrame,
    file: ct.CANTIDADES_CUADRE,
) -> pl.DataFrame:
    columnas_cantidades = columnas_cantidades_cuadre(file)
    return (
        dif_sap_vs_tera.lazy()
        .drop(columnas_cantidades)
        .rename({f"diferencia_{col}": col for col in columnas_cantidades})
        .select(["codigo_op", "codigo_ramo_op", "fecha_registro"] + columnas_cantidades)
        .join(pesos_aperturas_diferencia.lazy(), on=["codigo_op", "codigo_ramo_op"])
        .join(meses_a_cuadrar.lazy(), on="fecha_registro")
        .with_columns(
            [
                pl.col(col) * pl.col(f"peso_{col}") * pl.col(f"cuadrar_{col}")
                for col in columnas_cantidades
            ]
        )
        .collect()
    )


def agregar_columnas_faltantes(
    diferencias: pl.DataFrame, file: ct.CANTIDADES_CUADRE, negocio: str
) -> pl.DataFrame:
    if file == "siniestros":
        diferencias = diferencias.with_columns(
            apertura_reservas=utils.crear_columna_apertura_reservas(negocio, file),
            conteo_pago=0,
            conteo_incurrido=0,
            conteo_desistido=0,
            atipico=0,
            fecha_siniestro=pl.col("fecha_registro"),
        )
    return diferencias


def agregar_diferencias(df: pl.DataFrame, diferencias: pl.DataFrame) -> pl.DataFrame:
    columns = df.collect_schema().names()
    return (
        pl.concat([df, diferencias], how="vertical_relaxed")
        .group_by(columns[: columns.index("fecha_registro") + 1])
        .sum()
        .sort(columns[: columns.index("fecha_registro") + 1])
    )

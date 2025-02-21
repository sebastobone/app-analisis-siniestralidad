import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pandas as pd
import polars as pl
import teradatasql as td
from tqdm import tqdm

from src import utils
from src.configuracion import configuracion
from src.logger_config import logger
from src.models import Parametros


async def correr_query(
    file_path: str, save_path: str, save_format: str, p: Parametros
) -> None:
    tipo_query = determinar_tipo_query(file_path)
    segmentaciones = await obtener_segmentaciones(
        f"data/segmentacion_{p.negocio}.xlsx", tipo_query
    )

    particiones_fechas = crear_particiones_fechas(p.mes_inicio, p.mes_corte)

    queries = reemplazar_parametros_queries(open(file_path).read(), p)
    await verificar_numero_segmentaciones(file_path, queries, segmentaciones)

    logger.info(f"Ejecutando query {file_path}...")
    df = await ejecutar_queries(queries.split(";"), particiones_fechas, segmentaciones)
    logger.debug(df)

    if tipo_query != "otro":
        await verificar_resultado_siniestros_primas_expuestos(
            tipo_query, df, p.negocio, p.mes_inicio, p.mes_corte
        )
        df = df.select(
            utils.col_apertura_reservas(p.negocio),
            pl.all(),
        )

    await guardar_resultado(df, save_path, save_format, tipo_query)


def determinar_tipo_query(file: str) -> str:
    nombre_query = file.split("/")[-1]
    cantidad = nombre_query.replace(".sql", "")
    if cantidad in ["primas", "expuestos", "siniestros"]:
        return cantidad
    return "otro"


async def obtener_segmentaciones(
    path_archivo_segm: str, tipo_query: str
) -> list[pl.DataFrame]:
    try:
        hojas_segm = [
            str(hoja)
            for hoja in pd.ExcelFile(path_archivo_segm).sheet_names
            if str(hoja).startswith("add")
        ]
    except FileNotFoundError:
        logger.error(f"No se encuentra el archivo {path_archivo_segm}.")
        raise

    if hojas_segm:
        await verificar_nombre_hojas_segmentacion(hojas_segm)

    hojas_query = [hoja for hoja in hojas_segm if tipo_query[0] in hoja.split("_")[1]]

    return [
        pl.read_excel(path_archivo_segm, sheet_name=hoja_query)
        for hoja_query in hojas_query
    ]


def reemplazar_parametros_queries(queries: str, p: Parametros) -> str:
    return queries.format(
        mes_primera_ocurrencia=p.mes_inicio,
        mes_corte=p.mes_corte,
        fecha_primera_ocurrencia=utils.yyyymm_to_date(p.mes_inicio),
        fecha_mes_corte=utils.yyyymm_to_date(p.mes_corte),
        aproximar_reaseguro=1 if p.aproximar_reaseguro else 0,
    )


async def ejecutar_queries(
    queries: list[str],
    fechas_chunks: list[tuple[date, date]],
    segm: list[pl.DataFrame],
) -> pl.DataFrame:
    con, cur = conectar_teradata()

    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor()

    add_num = 0
    for n_query, query in tqdm(enumerate(queries)):
        logger.info(f"Ejecutando query {n_query + 1} de {len(queries)}...")
        try:
            if "?" not in query:
                await loop.run_in_executor(
                    executor, ejecutar_query_de_procesamiento, cur, query, fechas_chunks
                )
            else:
                add = await verificar_tabla_a_cargar(query, segm[add_num])
                await loop.run_in_executor(executor, cur.executemany, query, add.rows())
                add_num += 1

        except td.OperationalError:
            logger.error(utils.limpiar_espacios_log(f"Error en {query[:100]}"))
            raise

    resultado = pl.read_database(queries[-1], con)
    return pl.DataFrame(utils.lowercase_columns(resultado))


def ejecutar_query_de_procesamiento(
    cur: td.TeradataCursor, query: str, particiones_fechas: list[tuple[date, date]]
) -> None:
    if "{chunk_ini}" in query:
        ejecutar_query_particionado_en_fechas(cur, query, particiones_fechas)
    else:
        cur.execute(query)  # type: ignore


def ejecutar_query_particionado_en_fechas(
    cur: td.TeradataCursor, query: str, particiones_fechas: list[tuple[date, date]]
) -> None:
    for chunk_ini, chunk_fin in tqdm(particiones_fechas):
        cur.execute(
            query.format(
                chunk_ini=chunk_ini.strftime(format="%Y%m"),
                chunk_fin=chunk_fin.strftime(format="%Y%m"),
            )
        )  # type: ignore


async def verificar_tabla_a_cargar(query: str, add: pl.DataFrame) -> pl.DataFrame:
    await verificar_numero_columnas_segmentacion(query, add)
    add = await verificar_registros_duplicados(add)
    await verificar_valores_nulos(add)
    return add


def conectar_teradata() -> tuple[td.TeradataConnection, td.TeradataCursor]:
    creds = {
        "host": configuracion.teradata_host,
        "user": configuracion.teradata_user,
        "password": configuracion.teradata_password,
    }

    try:
        con = td.connect(json.dumps(creds))  # type: ignore
    except td.OperationalError as exc:
        if "Hostname lookup failed" in str(exc):
            logger.error(
                utils.limpiar_espacios_log(
                    """
                    Error al conectar con Teradata. Revise que se
                    encuentre conectado a la VPN en caso de no estar
                    conectado a la red "+SURA".
                    """
                )
            )
        else:
            logger.error(
                utils.limpiar_espacios_log(
                    """
                    Error al conectar con Teradata. Revise sus
                    credenciales.
                    """
                )
            )
        raise

    return con, con.cursor()  # type: ignore


def crear_particiones_fechas(
    mes_inicio: int, mes_corte: int
) -> list[tuple[date, date]]:
    inicios_mes = pl.date_range(
        utils.yyyymm_to_date(mes_inicio),
        utils.yyyymm_to_date(mes_corte),
        interval="1mo",
        eager=True,
    )
    fines_mes = inicios_mes.dt.month_end()
    return list(zip(inicios_mes, fines_mes, strict=False))


async def guardar_resultado(
    df: pl.DataFrame, save_path: str, save_format: str, tipo_query: str
) -> None:
    # Para poder visualizarlo facil, en caso de ser necesario
    if tipo_query != "otro" and save_format != "csv":
        df.write_csv(f"{save_path}.csv", separator="\t")

    if save_format == "parquet":
        df.write_parquet(f"{save_path}.parquet")
    elif save_format in ("csv", "txt"):
        df.write_csv(f"{save_path}.{save_format}", separator="\t")

    logger.success(f"Datos almacenados en {save_path}.{save_format}.")


async def verificar_nombre_hojas_segmentacion(segm_sheets: list[str]) -> None:
    for sheet in segm_sheets:
        partes = sheet.split("_")
        if (len(partes) < 3) or not any(char in partes[1] for char in "spe"):
            logger.error(
                """
                El nombre de las hojas con tablas a cargar debe seguir el formato
                "add_[indicador de queries que la utilizan]_[nombre de la tabla]".
                El indicador se escribe de la siguiente forma:
                    siniestros -> s
                    primas -> p
                    expuestos -> e
                Ejemplo: "add_spe_Canales" o "add_p_Sucursales".
                Corregir el nombre de la hoja.
                """
            )
            raise ValueError


async def verificar_numero_segmentaciones(
    file_path: str, queries: str, adds: list[pl.DataFrame]
) -> None:
    num_adds_necesarios = queries.count("?);")
    if num_adds_necesarios != len(adds):
        logger.error(
            f"""
            Necesita {num_adds_necesarios} tablas adicionales para 
            ejecutar el query {file_path},
            pero en el Excel "segmentacion.xlsx" hay {len(adds)} hojas
            de este tipo. Por favor, revise las hojas que tiene o revise que el 
            nombre de las hojas siga el formato
            "add_[indicador de queries que la utilizan]_[nombre de la tabla]".
            Hojas actuales: {adds}
            """
        )
        raise ValueError


async def verificar_numero_columnas_segmentacion(query: str, add: pl.DataFrame) -> None:
    num_cols = query.count("?")
    num_cols_add = len(add.collect_schema().names())
    if num_cols != num_cols_add:
        logger.error(
            f"""
            Error en {query}:
            La tabla creada en Teradata recibe {num_cols} columnas, pero la
            tabla que esta intentando ingresar tiene {num_cols_add} columnas:
            {add}
            Revise que el orden de las tablas en el Excel (de izquierda a derecha)
            sea el mismo que el del query.
            """
        )
        raise ValueError


async def verificar_registros_duplicados(add: pl.DataFrame) -> pl.DataFrame:
    if len(add) != len(add.unique()):
        logger.warning(
            f"""
            Alerta -> tiene registros duplicados en la siguiente tabla: {add}
            El proceso los elimina y va a continuar, pero se recomienda
            revisar la tabla en el Excel.
            """
        )
    return add.unique()


async def verificar_valores_nulos(add: pl.DataFrame) -> None:
    num_nulls = add.null_count().max_horizontal().max()
    if isinstance(num_nulls, int) and num_nulls > 0:
        logger.error(
            f"""
            Error -> tiene valores nulos en la siguiente tabla: {add}
            Corrija estos valores antes de ejecutar el proceso.
            """.replace("\n", " ")
        )
        raise ValueError


async def verificar_formato_fecha(col: pl.Series) -> None:
    if col.dtype != pl.Date:
        logger.error(f"""La columna {col.name} debe estar en formato fecha.""")
        raise ValueError


async def verificar_fechas_dentro_de_rangos(
    col: pl.Series, lim_inf: date, lim_sup: date
) -> None:
    if col.dt.min() < lim_inf or col.dt.max() > lim_sup:  # type: ignore
        logger.error(
            utils.limpiar_espacios_log(
                f"""La columna {col.name} debe estar entre {lim_inf} y {lim_sup},
                pero la informacion generada esta entre {col.dt.min()} y {col.dt.max()}.
                Revise el query.
                """
            )
        )
        raise ValueError


async def verificar_resultado_siniestros_primas_expuestos(
    tipo_query: str, df: pl.DataFrame, negocio: str, mes_inicio: int, mes_corte: int
) -> None:
    cols = df.collect_schema().names()

    for column in utils.min_cols_tera(tipo_query):
        if column not in cols:
            logger.error(f"""¡Falta la columna {column}!""")
            raise ValueError

    await verificar_formato_fecha(df.get_column("fecha_registro"))
    await verificar_fechas_dentro_de_rangos(
        df.get_column("fecha_registro"),
        utils.yyyymm_to_date(mes_inicio),
        utils.yyyymm_to_date(mes_corte),
    )

    if tipo_query == "siniestros":
        await verificar_formato_fecha(df.get_column("fecha_siniestro"))
        await verificar_fechas_dentro_de_rangos(
            df.get_column("fecha_siniestro"),
            utils.yyyymm_to_date(mes_inicio),
            utils.yyyymm_to_date(mes_corte),
        )

    segmentaciones_faltantes = df.filter(
        pl.any_horizontal(
            [
                (pl.col(col).is_null() | pl.col(col).eq("-1"))
                for col in utils.columnas_aperturas(negocio)
            ]
        )
    )
    if not segmentaciones_faltantes.is_empty():
        logger.error(
            f"""
            ¡Alerta! Revise las segmentaciones faltantes. {segmentaciones_faltantes}
            """
        )
        raise ValueError

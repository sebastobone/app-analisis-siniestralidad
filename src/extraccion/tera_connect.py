import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Literal

import pandas as pd
import polars as pl
import teradatasql as td

from src import constantes as ct
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

        df.write_csv(f"data/raw/{tipo_query}_teradata.csv", separator="\t")
        df = await eliminar_columnas_extra(df, p.negocio, tipo_query)

        if tipo_query == "siniestros":
            df = df.select(utils.crear_columna_apertura_reservas(p.negocio), pl.all())
            aperturas_generadas = sorted(
                df.get_column("apertura_reservas").unique().to_list()
            )
            aperturas_esperadas = sorted(
                utils.obtener_aperturas(p.negocio, "siniestros")
                .get_column("apertura_reservas")
                .unique()
                .to_list()
            )
            await verificar_aperturas_faltantes(
                aperturas_generadas, aperturas_esperadas
            )
            await verificar_aperturas_sobrantes(
                aperturas_generadas, aperturas_esperadas
            )

    await guardar_resultado(df, save_path, save_format, tipo_query)


def determinar_tipo_query(
    file: str,
) -> Literal["siniestros", "primas", "expuestos", "otro"]:
    nombre_query = file.split("/")[-1]
    cantidad = nombre_query.replace(".sql", "")
    return cantidad if cantidad in ["siniestros", "primas", "expuestos"] else "otro"  # type: ignore


async def obtener_segmentaciones(
    path_archivo_segm: str, tipo_query: str
) -> list[pl.DataFrame]:
    hojas_segm = [
        str(hoja)
        for hoja in pd.ExcelFile(path_archivo_segm).sheet_names
        if str(hoja).startswith("add")
    ]

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
    for n_query, query in enumerate(queries):
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

        except td.OperationalError as exc:
            raise ValueError(f"Error en query: {query[:100]}: \n{str(exc)}") from exc

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
    for chunk_ini, chunk_fin in particiones_fechas:
        logger.info(f"Ejecutando query para el rango {chunk_ini} - {chunk_fin}...")
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
    con = td.connect(**creds)  # type: ignore
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
            raise ValueError(
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


async def verificar_numero_segmentaciones(
    file_path: str, queries: str, adds: list[pl.DataFrame]
) -> None:
    num_adds_necesarios = queries.count("?);")
    if num_adds_necesarios != len(adds):
        raise ValueError(
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


async def verificar_numero_columnas_segmentacion(query: str, add: pl.DataFrame) -> None:
    num_cols = query.count("?")
    num_cols_add = len(add.collect_schema().names())
    if num_cols != num_cols_add:
        raise ValueError(
            f"""
            Error en {query}:
            La tabla creada en Teradata recibe {num_cols} columnas, pero la
            tabla que esta intentando ingresar tiene {num_cols_add} columnas:
            {add}
            Revise que el orden de las tablas en el Excel (de izquierda a derecha)
            sea el mismo que el del query.
            """
        )


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
        raise ValueError(
            f"""
            Error -> tiene valores nulos en la siguiente tabla: {add}
            Corrija estos valores antes de ejecutar el proceso.
            """
        )


async def verificar_formato_fecha(col: pl.Series) -> None:
    if col.dtype != pl.Date:
        raise ValueError(f"La columna {col.name} debe estar en formato fecha.")


async def verificar_fechas_dentro_de_rangos(
    col: pl.Series, lim_inf: date, lim_sup: date
) -> None:
    if col.dt.min() < lim_inf or col.dt.max() > lim_sup:  # type: ignore
        raise ValueError(
            utils.limpiar_espacios_log(
                f"""La columna {col.name} debe estar entre {lim_inf} y {lim_sup},
                pero la informacion generada esta entre {col.dt.min()} y {col.dt.max()}.
                Revise el query.
                """
            )
        )


async def verificar_resultado_siniestros_primas_expuestos(
    tipo_query: Literal["siniestros", "primas", "expuestos"],
    df: pl.DataFrame,
    negocio: str,
    mes_inicio: int,
    mes_corte: int,
) -> None:
    cols = df.collect_schema().names()

    for column in utils.columnas_minimas_salida_tera(negocio, tipo_query):
        if column not in cols:
            raise ValueError(f"¡Falta la columna {column}!")

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
                for col in utils.obtener_nombres_aperturas(negocio, tipo_query)
            ]
        )
    )
    if not segmentaciones_faltantes.is_empty():
        raise ValueError(
            f"""
            ¡Alerta! Revise las segmentaciones faltantes. {segmentaciones_faltantes}
            """
        )


async def eliminar_columnas_extra(
    df: pl.DataFrame,
    negocio: str,
    tipo_query: Literal["siniestros", "primas", "expuestos"],
) -> pl.DataFrame:
    columnas_necesarias = utils.columnas_minimas_salida_tera(negocio, tipo_query)
    columnas_valores = ct.COLUMNAS_VALORES_TERADATA[tipo_query]
    columnas_descriptoras = [
        col for col in columnas_necesarias if col not in columnas_valores
    ]
    return (
        df.select(columnas_necesarias)
        .group_by(columnas_descriptoras)
        .agg([pl.sum(col) for col in columnas_valores])
        .sort(columnas_descriptoras)
    )


async def verificar_aperturas_faltantes(
    aperturas_generadas: list[str], aperturas_esperadas: list[str]
) -> None:
    faltantes = []
    for apertura_generada in aperturas_generadas:
        if apertura_generada not in aperturas_esperadas:
            faltantes.append(apertura_generada)

    if len(faltantes) > 0:
        raise ValueError(
            utils.limpiar_espacios_log(
                f"""
                ¡Error! Las siguientes aperturas se generaron, pero no se
                esperaban: {faltantes}. Agregue estas aperturas al archivo de
                segmentacion.
                """
            )
        )


async def verificar_aperturas_sobrantes(
    aperturas_generadas: list[str], aperturas_esperadas: list[str]
) -> None:
    sobrantes = []
    for apertura_esperada in aperturas_esperadas:
        if apertura_esperada not in aperturas_generadas:
            sobrantes.append(apertura_esperada)

    if len(sobrantes) > 0:
        logger.warning(
            f"¡Alerta! No se generaron las siguientes aperturas: {sobrantes}"
        )

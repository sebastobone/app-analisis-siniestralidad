import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import polars as pl
import teradatasql as td

from src import constantes as ct
from src import utils
from src.logger_config import logger
from src.models import CredencialesTeradata, Parametros
from src.validation import cantidades

MENSAJE_APERTURAS_FALTANTES = """
    ¡Error! Las siguientes aperturas se encuentran en el archivo generado,
    pero no estan definidas: {faltantes}. Agregue estas aperturas al archivo de
    segmentacion.
"""

MENSAJE_APERTURAS_SOBRANTES = """
    ¡Alerta! Las siguientes aperturas se definieron, pero no se encuentran
    en el archivo generado. Puede optar por quitarlas del archivo de
    segmentacion, o simplemente ignorarlas: {faltantes}
"""


async def correr_query(
    file_path: str, p: Parametros, credenciales: CredencialesTeradata
) -> None:
    tipo_query = determinar_tipo_query(file_path)
    segmentaciones = await obtener_segmentaciones(
        f"data/segmentacion_{p.negocio}.xlsx", tipo_query
    )

    particiones_fechas = crear_particiones_fechas(p.mes_inicio, p.mes_corte)

    queries = reemplazar_parametros_queries(open(file_path).read(), p)
    await verificar_numero_segmentaciones(file_path, queries, segmentaciones)

    logger.info(f"Ejecutando query {file_path}...")
    df = await ejecutar_queries(
        queries.split(";"), particiones_fechas, segmentaciones, credenciales
    )
    logger.debug(df)

    cantidades.validar_archivo(p.negocio, df, tipo_query, tipo_query)

    df = utils.agrupar_por_columnas_relevantes(df, p.negocio, tipo_query)

    if tipo_query == "siniestros":
        df = df.select(
            utils.crear_columna_apertura_reservas(p.negocio, "siniestros"), pl.all()
        )

    await guardar_resultado(df, tipo_query)


def determinar_tipo_query(
    file: str,
) -> ct.LISTA_CANTIDADES:
    return file.split("/")[-1].replace(".sql", "")  # type: ignore


async def obtener_segmentaciones(
    path_archivo_segm: str, tipo_query: str
) -> list[pl.DataFrame]:
    hojas_segm = [
        str(hoja)
        for hoja in list(pl.read_excel(path_archivo_segm, sheet_id=0).keys())
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
    )


async def ejecutar_queries(
    queries: list[str],
    fechas_chunks: list[tuple[date, date]],
    segm: list[pl.DataFrame],
    credenciales: CredencialesTeradata,
) -> pl.DataFrame:
    con, cur = conectar_teradata(credenciales)

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


def conectar_teradata(
    credenciales: CredencialesTeradata,
) -> tuple[td.TeradataConnection, td.TeradataCursor]:
    con = td.connect(**credenciales.model_dump())  # type: ignore
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


async def guardar_resultado(df: pl.DataFrame, tipo_query: ct.LISTA_CANTIDADES) -> None:
    # En csv para poder visualizarlo facil, en caso de ser necesario
    for sufijo in ["_teradata", ""]:
        df.write_csv(f"data/raw/{tipo_query}{sufijo}.csv", separator="\t")
        df.write_parquet(f"data/raw/{tipo_query}{sufijo}.parquet")

    logger.success(f"Datos almacenados en data/raw/{tipo_query}.csv.")


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

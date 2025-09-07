import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import polars as pl
import teradatasql as td

from src import constantes as ct
from src import utils
from src.dependencias import SessionDep
from src.informacion.almacenamiento import guardar_archivo
from src.logger_config import logger
from src.models import CredencialesTeradata, MetadataCantidades, Parametros
from src.procesamiento.autonomia import adds as autonomia
from src.validation import adds, cantidades, segmentacion

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

ERROR_TIPOS_DATOS = """
    Ocurrio un error al transformar los tipos de datos del archivo
    {nombre_archivo}:
"""


async def correr_query(
    p: Parametros,
    cantidad: ct.CANTIDADES,
    credenciales: CredencialesTeradata,
    session: SessionDep,
) -> None:
    segmentacion.validar_archivo_segmentacion(
        pl.read_excel(f"data/segmentacion_{p.negocio}.xlsx", sheet_id=0)
    )

    if p.negocio == "autonomia":
        await _preparar_auxiliares_autonomia(p, cantidad)

    segmentaciones = await _obtener_segmentaciones(p.negocio, cantidad)

    particiones_fechas = _crear_particiones_fechas(p.mes_inicio, p.mes_corte)

    file_path = f"data/queries/{p.negocio}/{cantidad}.sql"
    queries = _reemplazar_parametros_queries(open(file_path).read(), p).split(";")
    await adds.validar_numero_segmentaciones(
        file_path, p.negocio, queries, segmentaciones
    )

    logger.info(f"Ejecutando query {file_path}...")
    df = await _ejecutar_queries(
        queries, particiones_fechas, segmentaciones, credenciales
    )

    df.pipe(cantidades.validar_archivo, p.negocio, cantidad, cantidad).pipe(
        cantidades.organizar_archivo,
        p.negocio,
        (p.mes_inicio, p.mes_corte),
        cantidad,
        cantidad,
    ).pipe(
        guardar_archivo,
        session,
        MetadataCantidades(
            ruta=f"data/raw/{cantidad}.parquet",
            nombre_original=f"{cantidad}.parquet",
            origen="extraccion",
            cantidad=cantidad,
        ),
    )

    logger.success(f"Query de {cantidad} ejecutado exitosamente.")


async def _preparar_auxiliares_autonomia(
    p: Parametros, cantidad: ct.CANTIDADES
) -> None:
    if cantidad == "siniestros":
        await autonomia.sap_sinis_ced(p.mes_corte)
    elif cantidad == "primas":
        await autonomia.sap_primas_ced(p.mes_corte)


async def _obtener_segmentaciones(
    negocio: str, cantidad: ct.CANTIDADES
) -> list[pl.DataFrame]:
    hojas_segm = [
        str(hoja)
        for hoja in list(
            pl.read_excel(f"data/segmentacion_{negocio}.xlsx", sheet_id=0).keys()
        )
        if str(hoja).startswith("add")
    ]

    if hojas_segm:
        await adds.validar_nombre_hojas_segmentacion(hojas_segm)

    hojas_query = [hoja for hoja in hojas_segm if cantidad[0] in hoja.split("_")[1]]

    return [
        pl.read_excel(f"data/segmentacion_{negocio}.xlsx", sheet_name=hoja_query)
        for hoja_query in hojas_query
    ]


def _reemplazar_parametros_queries(queries: str, p: Parametros) -> str:
    return queries.format(
        mes_primera_ocurrencia=p.mes_inicio,
        mes_corte=p.mes_corte,
        fecha_primera_ocurrencia=p.mes_inicio,
        fecha_mes_corte=p.mes_corte,
    )


async def _ejecutar_queries(
    queries: list[str],
    fechas_chunks: list[tuple[date, date]],
    segm: list[pl.DataFrame],
    credenciales: CredencialesTeradata,
) -> pl.DataFrame:
    con, cur = _conectar_teradata(credenciales)

    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor()

    add_num = 0
    for n_query, query in enumerate(queries):
        logger.info(f"Ejecutando query {n_query + 1} de {len(queries)}...")
        try:
            if "?" not in query:
                await loop.run_in_executor(
                    executor,
                    _ejecutar_query_de_procesamiento,
                    cur,
                    query,
                    fechas_chunks,
                )
            else:
                add = await adds.validar_tabla_a_cargar(query, segm[add_num])
                await loop.run_in_executor(executor, cur.executemany, query, add.rows())
                add_num += 1

        except td.OperationalError as exc:
            raise ValueError(f"Error en query: {query[:100]}: \n{str(exc)}") from exc

    resultado = pl.read_database(queries[-1], con)
    return pl.DataFrame(utils.lowercase_columns(resultado))


def _ejecutar_query_de_procesamiento(
    cur: td.TeradataCursor, query: str, particiones_fechas: list[tuple[date, date]]
) -> None:
    if "{chunk_ini}" in query:
        _ejecutar_query_particionado_en_fechas(cur, query, particiones_fechas)
    else:
        cur.execute(query)  # type: ignore


def _ejecutar_query_particionado_en_fechas(
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


def _conectar_teradata(
    credenciales: CredencialesTeradata,
) -> tuple[td.TeradataConnection, td.TeradataCursor]:
    con = td.connect(**credenciales.model_dump())  # type: ignore
    return con, con.cursor()  # type: ignore


def _crear_particiones_fechas(
    mes_inicio: date, mes_corte: date
) -> list[tuple[date, date]]:
    inicios_mes = pl.date_range(mes_inicio, mes_corte, interval="1mo", eager=True)
    fines_mes = inicios_mes.dt.month_end()
    return list(zip(inicios_mes, fines_mes, strict=False))

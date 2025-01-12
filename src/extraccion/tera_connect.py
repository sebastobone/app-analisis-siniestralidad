import json
import textwrap
from datetime import date

import pandas as pd
import polars as pl
import teradatasql as td
from tqdm import tqdm

from src import utils
from src.configuracion import configuracion
from src.logger_config import logger
from src.models import Parametros


def tipo_query(file: str) -> str:
    nombre_query = file.split("/")[-1]
    for qty in ["siniestros", "primas", "expuestos"]:
        if qty == nombre_query.replace(".sql", ""):
            return qty
    return "otro"


def preparar_queries(
    queries: str,
    mes_inicio: int,
    mes_corte: int,
    aproximar_reaseguro: bool,
) -> str:
    return queries.format(
        mes_primera_ocurrencia=mes_inicio,
        mes_corte=mes_corte,
        fecha_primera_ocurrencia=utils.yyyymm_to_date(mes_inicio),
        fecha_mes_corte=utils.yyyymm_to_date(mes_corte),
        aproximar_reaseguro=1 if aproximar_reaseguro else 0,
    )


def cargar_segmentaciones(
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
        check_adds_segmentacion(hojas_segm)

    hojas_query = [hoja for hoja in hojas_segm if tipo_query[0] in hoja.split("_")[1]]

    return [
        pl.read_excel(path_archivo_segm, sheet_name=hoja_query)
        for hoja_query in hojas_query
    ]


def fechas_chunks(mes_inicio: int, mes_corte: int) -> list[tuple[date, date]]:
    """Limites para correr queries pesados por partes"""
    return list(
        zip(
            pl.date_range(
                utils.yyyymm_to_date(mes_inicio),
                utils.yyyymm_to_date(mes_corte),
                interval="1mo",
                eager=True,
            ),
            pl.date_range(
                utils.yyyymm_to_date(mes_inicio),
                utils.yyyymm_to_date(mes_corte),
                interval="1mo",
                eager=True,
            ).dt.month_end(),
            strict=False,
        )
    )


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
                textwrap.dedent(
                    """
                    Error al conectar con Teradata. Revise que se
                    encuentre conectado a la VPN en caso de no estar
                    conectado a la red "+SURA".
                    """
                ).replace("\n", " ")
            )
        else:
            logger.error(
                textwrap.dedent(
                    """
                    Error al conectar con Teradata. Revise sus
                    credenciales.
                    """
                ).replace("\n", " ")
            )
        raise

    cur = con.cursor()  # type: ignore
    logger.success("Conexion con Teradata exitosa.")

    return con, cur


def ejecutar_queries(
    queries: list[str],
    fechas_chunks: list[tuple[date, date]],
    segm: list[pl.DataFrame],
) -> pl.DataFrame:
    _, cur = conectar_teradata()

    add_num = 0
    for n_query, query in tqdm(enumerate(queries)):
        logger.info(f"Ejecutando query {n_query + 1} de {len(queries)}...")
        try:
            if "?" not in query:
                if "{chunk_ini}" in query:
                    for chunk_ini, chunk_fin in tqdm(fechas_chunks):
                        cur.execute(
                            query.format(
                                chunk_ini=chunk_ini.strftime(format="%Y%m"),
                                chunk_fin=chunk_fin.strftime(format="%Y%m"),
                            )
                        )  # type: ignore
                else:
                    cur.execute(query)  # type: ignore
            else:
                check_numero_columnas_add(query, segm[add_num])
                add = check_duplicados(segm[add_num])
                check_nulls(add)
                cur.executemany(query, add.rows())  # type: ignore
                add_num += 1

        except td.OperationalError:
            logger.error(f"Error en {query[:100]}")
            raise

    return pl.DataFrame(
        utils.lowercase_columns(
            pl.read_database("{fn teradata_try_fastexport}" + queries[-1], cur)
        )
    )


def guardar_resultados(
    df: pl.DataFrame,
    save_path: str,
    save_format: str,
    tipo_query: str,
) -> None:
    # Para poder visualizarlo facil, en caso de ser necesario
    if tipo_query != "otro" and save_format != "csv":
        df.write_csv(f"{save_path}.csv", separator="\t")

    if save_format == "parquet":
        df.write_parquet(f"{save_path}.parquet")
    elif save_format in ("csv", "txt"):
        df.write_csv(f"{save_path}.{save_format}", separator="\t")

    logger.success(f"Datos almacenados en {save_path}.{save_format}.")


def check_adds_segmentacion(segm_sheets: list[str]) -> None:
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


def check_suficiencia_adds(
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


def check_numero_columnas_add(query: str, add: pl.DataFrame) -> None:
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


def check_duplicados(add: pl.DataFrame) -> pl.DataFrame:
    if len(add) != len(add.unique()):
        logger.warning(
            f"""
            Alerta -> tiene registros duplicados en la siguiente tabla: {add}
            El proceso los elimina y va a continuar, pero se recomienda
            revisar la tabla en el Excel.
            """
        )
    return add.unique()


def check_nulls(add: pl.DataFrame) -> None:
    num_nulls = add.null_count().max_horizontal().max()
    if isinstance(num_nulls, int) and num_nulls > 0:
        logger.error(
            f"""
            Error -> tiene valores nulos en la siguiente tabla: {add}
            Corrija estos valores antes de ejecutar el proceso.
            """.replace("\n", " ")
        )
        raise ValueError


def asegurar_formato_fecha(col: pl.Series) -> None:
    if col.dtype != pl.Date:
        logger.error(f"""La columna {col.name} debe estar en formato fecha.""")
        raise ValueError


def impedir_fecha_por_fuera(col: pl.Series, lim_inf: date, lim_sup: date) -> None:
    if col.dt.min() < lim_inf or col.dt.max() > lim_sup:  # type: ignore
        logger.error(
            f"""La columna {col.name} debe estar entre {lim_inf} y {lim_sup},
                pero la informacion generada esta entre {col.dt.min()} y {col.dt.max()}.
                Revise el query.
                """.replace("\n", " ")
        )
        raise ValueError


def check_final_info(
    tipo_query: str, df: pl.DataFrame, negocio: str, mes_inicio: int, mes_corte: int
) -> None:
    """Esta funcion se usa cuando se ejecuta un query de siniestros,
    primas, o expuestos que consolida la informacion necesaria para
    pasar a las transformaciones de la plantilla, sin necesidad de
    hacer procesamiento extra. Valida la consistencia de la informacion.
    """

    cols = df.collect_schema().names()

    for column in utils.min_cols_tera(tipo_query):
        if column not in cols:
            logger.error(f"""¡Falta la columna {column}!""")
            raise ValueError

    asegurar_formato_fecha(df.get_column("fecha_registro"))
    impedir_fecha_por_fuera(
        df.get_column("fecha_registro"),
        utils.yyyymm_to_date(mes_inicio),
        utils.yyyymm_to_date(mes_corte),
    )

    if tipo_query == "siniestros":
        asegurar_formato_fecha(df.get_column("fecha_siniestro"))
        impedir_fecha_por_fuera(
            df.get_column("fecha_siniestro"),
            utils.yyyymm_to_date(mes_inicio),
            utils.yyyymm_to_date(mes_corte),
        )

    # Segmentaciones faltantes
    df_faltante = df.filter(
        pl.any_horizontal(
            [
                (pl.col(col).is_null() | pl.col(col).eq("-1"))
                for col in utils.columnas_aperturas(negocio)
            ]
        )
    )
    if not df_faltante.is_empty():
        logger.error(f"""¡Alerta! Revise las segmentaciones faltantes. {df_faltante}""")
        raise ValueError


def correr_query(
    file_path: str,
    save_path: str,
    save_format: str,
    p: Parametros,
) -> None:
    tipo = tipo_query(file_path)
    segm = cargar_segmentaciones(f"data/segmentacion_{p.negocio}.xlsx", tipo)

    fchunks = fechas_chunks(p.mes_inicio, p.mes_corte)

    queries = preparar_queries(
        open(file_path).read(), p.mes_inicio, p.mes_corte, p.aproximar_reaseguro
    )
    check_suficiencia_adds(file_path, queries, segm)

    df = ejecutar_queries(queries.split(";"), fchunks, segm)
    logger.debug(df)

    if tipo != "otro":
        check_final_info(tipo, df, p.negocio, p.mes_inicio, p.mes_corte)
        df = df.select(
            utils.col_apertura_reservas(p.negocio),
            pl.all(),
        )

    guardar_resultados(df, save_path, save_format, tipo)

import teradatasql as td
import polars as pl
from tqdm import tqdm
import json
import pandas as pd
import constantes as ct
from datetime import date


def sini_primas_exp(file: str) -> str:
    query_name = file.split("/")[-1]
    for qty in ["siniestros", "primas", "expuestos"]:
        if qty in query_name:
            return qty
    return "otro"


def check_adds_segmentacion(segm_sheets: list[str]) -> None:
    for segm_sheet in segm_sheets:
        if (len(segm_sheet.split("_")) < 3) or (
            "s" not in segm_sheet.split("_")[1]
            and "p" not in segm_sheet.split("_")[1]
            and "e" not in segm_sheet.split("_")[1]
        ):
            raise Exception(
                """
                El nombre de las hojas con tablas a cargar debe seguir el
                formato "add_[indicador de queries que la utilizan]_[nombre de la tabla]".
                El indicador se escribe de la siguiente forma:
                    siniestros -> s
                    primas -> p
                    expuestos -> e
                De forma que, por ejemplo, un formato valido seria "add_spe_Canales"
                o "add_p_Sucursales". Debe corregir el nombre de la hoja.
                """
            )


def check_suficiencia_adds(file: str, queries: str, segm_sheets: list[str]) -> None:
    query_type = sini_primas_exp(file)
    segm_sheets_file = [
        segm_sheet
        for segm_sheet in segm_sheets
        if query_type[:1] in segm_sheet.split("_")[1]
    ]
    num_adds = queries.count("?);")
    if num_adds != len(segm_sheets_file):
        raise Exception(
            f"""
            Necesita {num_adds} tablas adicionales para ejecutar el query {file},
            pero en el Excel "segmentacion.xlsx" hay {len(segm_sheets_file)} hojas
            de este tipo. Por favor, revise las hojas que tiene o revise que el 
            nombre de las hojas siga el formato
            "add_[indicador de queries que la utilizan]_[nombre de la tabla]".
            Hojas actuales: {segm_sheets_file}
            """
        )


def check_numero_columnas_add(file: str, query: str, add: pl.DataFrame) -> None:
    num_cols = query.count("?")
    num_cols_add = len(add.collect_schema().names())
    if num_cols != num_cols_add:
        raise Exception(
            f"""
            Error en {file} -> {query}:
            La tabla creada en Teradata recibe {num_cols} columnas, pero la
            tabla que esta intentando ingresar tiene {num_cols_add} columnas:
            {add}
            Revise que el orden de las tablas en el Excel (de izquierda a derecha)
            sea el mismo que el del query.
            """
        )


def check_duplicados(add: pl.DataFrame) -> pl.DataFrame:
    if len(add) != len(add.unique()):
        print(
            f"""
            Alerta -> tiene registros duplicados en la siguiente tabla: {add}
            El proceso los elimina y va a continuar, pero se recomienda
            revisar la tabla en el Excel.
            """
        )
    return add.unique()


def check_nulls(add: pl.DataFrame) -> None:
    if add.null_count().max_horizontal().max() > 0:
        raise Exception(
            f"""
            Error -> tiene valores nulos en la siguiente tabla: {add}
            Corrija estos valores antes de ejecutar el proceso.
            """
        )


def checks_final_info(file: str, df: pl.DataFrame) -> None:
    """
    Esta funcion se usa cuando se ejecuta un query de siniestros,
    primas, o expuestos que consolida la informacion necesaria para
    pasar a las transformaciones de la plantilla, sin necesidad de
    hacer procesamiento extra.
    """
    for column in ct.cols_tera(file):
        if column not in df.collect_schema().names():
            raise Exception(f"""¡Falta la columna {column}!""")

    if (
        "fecha_siniestro" in df.collect_schema().names()
        and df.get_column("fecha_siniestro").dtype != pl.Date
    ):
        raise Exception("""La columna fecha_siniestro debe estar en formato fecha.""")

    if df.get_column("fecha_registro").dtype != pl.Date:
        raise Exception("""La columna fecha_registro debe estar en formato fecha.""")

    df = df.select(
        pl.concat_str(
            [
                pl.col("codigo_op"),
                pl.col("codigo_ramo_op"),
                pl.col("apertura_canal_desc"),
                pl.col("apertura_amparo_desc"),
            ],
            separator="_",
        ).alias("apertura_reservas"),
        pl.all(),
    )
    df.write_csv(file.replace("queries", "raw").replace(".sql", ".csv"), separator="\t")

    return df


def read_query(file: str) -> None:
    tipo_query = sini_primas_exp(file)

    if ct.NEGOCIO == "autonomia" and "siniestros" in file:
        extra_processing = True
    else:
        extra_processing = False

    if "siniestros" in file or "primas" in file or "expuestos" in file:
        query_negocio = True
    else:
        query_negocio = False

    # Tablas de segmentacion
    segm_sheets = [
        str(sheet)
        for sheet in pd.ExcelFile(f"data/segmentacion_{ct.NEGOCIO}.xlsx").sheet_names
        if str(sheet)[:3] == "add"
    ]
    if len(segm_sheets) > 0:
        check_adds_segmentacion(segm_sheets)

    segm = [
        pl.read_excel(
            f"data/segmentacion_{ct.NEGOCIO}.xlsx", sheet_name=str(segm_sheet)
        )
        for segm_sheet in segm_sheets
        if tipo_query[:1] in segm_sheet.split("_")[1]
    ]

    queries = open(file).read()

    check_suficiencia_adds(file, queries, segm_sheets)

    con = td.connect(json.dumps(ct.CREDENCIALES_TERADATA))
    cur = con.cursor()

    # Limites para correr queries pesados por partes
    chunk_lim_1 = date(
        ct.MES_PRIMERA_OCURRENCIA // 100, ct.MES_PRIMERA_OCURRENCIA % 100, 1
    )
    chunk_lim_2 = date(ct.MES_CORTE // 100, ct.MES_CORTE % 100, 1)

    fechas_chunks = pl.date_range(chunk_lim_1, chunk_lim_2, interval="1mo", eager=True)
    fechas_chunks = list(zip(fechas_chunks, fechas_chunks.dt.month_end()))

    add_num = 0
    for n_query, query in enumerate(tqdm(queries.split(sep=";"))):
        if "?" not in query:
            if "{chunk_ini}" not in query:
                query = query.format(
                    mes_primera_ocurrencia=ct.MES_PRIMERA_OCURRENCIA,
                    mes_corte=ct.MES_CORTE,
                    fecha_primera_ocurrencia=ct.INI_DATE,
                    fecha_mes_corte=ct.END_DATE,
                    dia_reaseguro=ct.DIA_CARGA_REASEGURO,
                )
            elif "{chunk_ini}" in query:
                for fecha_chunk in tqdm(fechas_chunks):
                    query_chunk = query.format(
                        chunk_ini=fecha_chunk[0].strftime(format="%Y%m"),
                        chunk_fin=fecha_chunk[1].strftime(format="%Y%m"),
                    )
                    cur.execute(query_chunk)

            if n_query != len(queries.split(sep=";")) - 1:
                cur.execute(query)
            else:
                df = pl.read_database(query, con)
                for column in df.collect_schema().names():
                    df = df.rename({column: column.lower()})

                if tipo_query != "otro":
                    checks_final_info(tipo_query, df)

                df.write_parquet(
                    file.replace("queries", "raw").replace(".sql", ".parquet")
                )
                print(f"""
                      Query {file} completado. Datos almacenados en 
                      {file.replace("queries", "raw").replace(".sql", ".parquet")}.
                      """)
        else:
            check_numero_columnas_add(file, query, segm[add_num])
            add = check_duplicados(segm[add_num])
            check_nulls(add)
            cur.executemany(query, add.rows())
            add_num += 1

    if not extra_processing and query_negocio:
        # Segmentaciones faltantes
        df_faltante = df.filter(
            pl.any_horizontal(
                [
                    (pl.col(col).is_null() | pl.col(col).eq("-1"))
                    for col in ct.APERT_COLS
                ]
            )
        )
        if len(df_faltante) != 0:
            print(df_faltante)
            raise Exception("""¡Alerta! Revise las segmentaciones faltantes.""")

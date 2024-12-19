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
        if qty == query_name.replace(".sql", ""):
            return qty
    return "otro"


def check_adds_segmentacion(segm_sheets: list[str]) -> None:
    for sheet in segm_sheets:
        partes = sheet.split("_")
        if (len(partes) < 3) or not any(char in partes[1] for char in "spe"):
            raise Exception(
                """
                El nombre de las hojas con tablas a cargar debe seguir el
                formato "add_[indicador de queries que la utilizan]_[nombre de la tabla]".
                El indicador se escribe de la siguiente forma:
                    siniestros -> s
                    primas -> p
                    expuestos -> e
                Ejemplo: "add_spe_Canales" o "add_p_Sucursales".
                Corregir el nombre de la hoja.
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


def checks_final_info(tipo_query: str, df: pl.DataFrame) -> None:
    """
    Esta funcion se usa cuando se ejecuta un query de siniestros,
    primas, o expuestos que consolida la informacion necesaria para
    pasar a las transformaciones de la plantilla, sin necesidad de
    hacer procesamiento extra.
    """
    for column in ct.min_cols_tera(tipo_query):
        if column not in df.collect_schema().names():
            raise ValueError(f"""¡Falta la columna {column}!""")

    if (
        "fecha_siniestro" in df.collect_schema().names()
        and df.get_column("fecha_siniestro").dtype != pl.Date
    ):
        raise ValueError("""La columna fecha_siniestro debe estar en formato fecha.""")

    if df.get_column("fecha_registro").dtype != pl.Date:
        raise ValueError("""La columna fecha_registro debe estar en formato fecha.""")


def col_apertura_reservas() -> pl.Expr:
    return pl.concat_str(
        [
            pl.col("codigo_op"),
            pl.col("codigo_ramo_op"),
            pl.col("apertura_canal_desc"),
            pl.col("apertura_amparo_desc"),
        ],
        separator="_",
    ).alias("apertura_reservas")


def read_query(file: str, save_path: str, save_format: str) -> None:
    tipo_query = sini_primas_exp(file)

    if ct.NEGOCIO == "autonomia" and "siniestros" in file:
        extra_processing = True
    else:
        extra_processing = False

    query_negocio = tipo_query != "otro"

    # Tablas de segmentacion
    archivo_segm = f"data/segmentacion_{ct.NEGOCIO}.xlsx"
    segm_sheets = [
        str(sheet)
        for sheet in pd.ExcelFile(archivo_segm).sheet_names
        if str(sheet).startswith("add")
    ]
    if len(segm_sheets) > 0:
        check_adds_segmentacion(segm_sheets)

    segm = [
        pl.read_excel(archivo_segm, sheet_name=str(segm_sheet))
        for segm_sheet in segm_sheets
        if tipo_query[0] in segm_sheet.split("_")[1]
    ]

    queries = open(file).read()
    check_suficiencia_adds(file, queries, segm_sheets)

    con = td.connect(json.dumps(ct.CREDENCIALES_TERADATA))
    cur = con.cursor()

    # Limites para correr queries pesados por partes
    fechas_chunks = list(
        zip(
            pl.date_range(ct.INI_DATE, ct.END_DATE, interval="1mo", eager=True),
            pl.date_range(
                ct.INI_DATE, ct.END_DATE, interval="1mo", eager=True
            ).dt.month_end(),
        )
    )

    add_num = 0
    for n_query, query in enumerate(tqdm(queries.split(sep=";"))):
        if "?" not in query:
            query = query.format(
                mes_primera_ocurrencia=ct.INI_DATE.strftime("%Y%m"),
                mes_corte=ct.END_DATE.strftime("%Y%m"),
                fecha_primera_ocurrencia=ct.INI_DATE,
                fecha_mes_corte=ct.END_DATE,
                dia_reaseguro=ct.DIA_CARGA_REASEGURO,
            )

            if "{chunk_ini}" in query:
                for chunk_ini, chunk_fin in tqdm(fechas_chunks):
                    cur.execute(
                        query.format(
                            chunk_ini=chunk_ini.strftime(format="%Y%m"),
                            chunk_fin=chunk_fin.strftime(format="%Y%m"),
                        )
                    )

            if n_query != len(queries.split(sep=";")) - 1:
                cur.execute(query)
            else:
                df = pl.read_database(query, con)
                df = df.rename(
                    {column: column.lower() for column in df.collect_schema().names()}
                )

                if query_negocio:
                    checks_final_info(tipo_query, df)
                    df = df.select(
                        col_apertura_reservas(),
                        pl.all(),
                    )
                    df.write_csv(f"{save_path}.csv", separator="\t")

                if save_format == "parquet":
                    df.write_parquet(f"{save_path}.parquet")
                elif save_format == "csv":
                    df.write_csv(f"{save_path}.csv", separator="\t")

                print(f"""
                      Query {file} completado. Datos almacenados en 
                      {save_path}.{save_format}.
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
        if not df_faltante.is_empty():
            print(df_faltante)
            raise ValueError("""¡Alerta! Revise las segmentaciones faltantes.""")

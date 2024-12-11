import teradatasql as td
import polars as pl
from tqdm import tqdm
import json
from pandas import ExcelFile
import constantes as ct


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
    segm_sheets_file = [
        segm_sheet for segm_sheet in segm_sheets if file[:1] in segm_sheet.split("_")[1]
    ]
    num_adds = queries.count("?);")
    if num_adds != len(segm_sheets_file):
        raise Exception(
            f"""
            Necesita {num_adds} tablas adicionales para ejecutar el query de {file},
            pero en el Excel "segmentacion.xlsx" hay {len(segm_sheets_file)} hojas
            de este tipo. Por favor, revise las hojas que tiene o revise que el 
            nombre de las hojas siga el formato
            "add_[indicador de queries que la utilizan]_[nombre de la tabla]".
            """
        )


def check_duplicados(add: pl.DataFrame) -> None:
    if len(add) != len(add.unique()):
        raise Exception(
            f"""¡Error! Tiene registros duplicados en la siguiente tabla: {add}."""
        )


def read_query(file: str) -> None:
    # Tablas de segmentacion
    segm_sheets = [
        str(sheet)
        for sheet in ExcelFile("data/segmentacion.xlsx").sheet_names
        if str(sheet)[:3] == "add"
    ]
    if len(segm_sheets) > 0:
        check_adds_segmentacion(segm_sheets)

    segm = [
        pl.read_excel("data/segmentacion.xlsx", sheet_name=str(segm_sheet))
        for segm_sheet in segm_sheets
        if file[:1] in segm_sheet.split("_")[1]
    ]

    queries = (
        open(f"data/queries/{file}.sql")
        .read()
        .format(
            mes_primera_ocurrencia=ct.PARAMS_FECHAS[0][1],
            mes_corte=ct.PARAMS_FECHAS[1][1],
            fecha_primera_ocurrencia=f"{ct.PARAMS_FECHAS[0][1][:4]}-{ct.PARAMS_FECHAS[0][1][4:]}-01",
        )
    )

    check_suficiencia_adds(file, queries, segm_sheets)

    credenciales = {
        "host": "teradata.suranet.com",
        "user": "sebatoec",
        "password": "47^07Ghia0+b",
    }

    con = td.connect(json.dumps(credenciales))
    cur = con.cursor()

    add_num = 0
    for query in tqdm(queries.split(sep=";")):
        if "?" not in query:
            if query != queries.split(sep=";")[-1]:
                cur.execute(query)
            else:
                df = pl.read_database(query, con)
                for column in df.collect_schema().names():
                    df = df.rename({column: column.lower()})

                for column in ct.cols_tera(file):
                    if column not in df.collect_schema().names():
                        raise Exception(f"""¡Falta la columna {column}!""")

                if (
                    file == "siniestros"
                    and df.get_column("fecha_siniestro").dtype != pl.Date
                ):
                    raise Exception(
                        """La columna fecha_siniestro debe estar en formato fecha."""
                    )

                if df.get_column("fecha_registro").dtype != pl.Date:
                    raise Exception(
                        """La columna fecha_registro debe estar en formato fecha."""
                    )

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
                df.write_csv(f"data/raw/{file}.csv", separator="\t")
                df.write_parquet(f"data/raw/{file}.parquet")
        else:
            check_duplicados(segm[add_num])
            cur.executemany(query, segm[add_num].rows())
            add_num += 1

    # Segmentaciones faltantes
    df_faltante = df.filter(
        pl.any_horizontal(
            [(pl.col(col).is_null() | pl.col(col).eq("-1")) for col in ct.APERT_COLS]
        )
    )
    if len(df_faltante) != 0:
        print(df_faltante)
        raise Exception("""¡Alerta! Revise las segmentaciones faltantes.""")

import teradatasql as td
import polars as pl
from tqdm import tqdm
import json
from pandas import ExcelFile


def read_query(file: str) -> None:
    # Tablas de segmentacion
    segm_sheets = [
        sheet
        for sheet in ExcelFile("data/segmentacion.xlsx").sheet_names
        if sheet[:3] == "add"
    ]
    segm = [
        pl.read_excel("data/segmentacion.xlsx", sheet_name=str(segm_sheet))
        for segm_sheet in segm_sheets
        if file[:1] in segm_sheet.split("_")[1]
    ]

    params_fechas = pl.read_excel(
        "data/segmentacion.xlsx", sheet_name="Fechas", has_header=False
    ).rows()

    queries = (
        open(f"data/queries/{file}.sql")
        .read()
        .format(
            mes_primera_ocurrencia=params_fechas[0][1],
            mes_corte=params_fechas[1][1],
            fecha_primera_ocurrencia=f"{params_fechas[0][1][:4]}-{params_fechas[0][1][4:]}-01",
        )
    )

    queries_list = queries.split(sep=";")

    credenciales = {
        "host": "teradata.suranet.com",
        "user": "sebatoec",
        "password": "47^07Ghia0+b",
    }

    con = td.connect(json.dumps(credenciales))
    cur = con.cursor()

    add_num = 0
    for query in tqdm(queries_list):
        if "?" not in query:
            if query != queries_list[-1]:
                cur.execute(query)
            else:
                df = pl.read_database(query, con)
                for column in df.collect_schema().names():
                    df = df.rename({column: column.lower()})

                for column in ["codigo_op", "codigo_ramo_op", "ramo_desc"]:
                    assert (
                        column in df.collect_schema().names()
                    ), f"¡Falta la columna {column}! Es necesaria para las validaciones contables."

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
            cur.executemany(query, segm[add_num].rows())
            add_num += 1

    # Segmentaciones faltantes
    for apertura in ["apertura_canal_desc", "apertura_amparo_desc"]:
        assert (
            "-1" not in df.select(apertura).unique()
        ), f"Alerta! -1 en {apertura}, añadir nueva segmentacion"

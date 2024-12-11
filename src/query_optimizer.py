import polars as pl
from pandas import ExcelFile
import constantes as ct
import teradatasql as td
from tqdm import tqdm
import json
from extraccion import tera_connect

NEGOCIO = "autonomia"


def read_query(file: str) -> None:
    # Tablas de segmentacion
    segm_sheets = [
        str(sheet)
        for sheet in ExcelFile(f"data/segmentacion_{NEGOCIO}.xlsx").sheet_names
        if str(sheet)[:3] == "add"
    ]
    if len(segm_sheets) > 0:
        tera_connect.check_adds_segmentacion(segm_sheets)
    segm = [
        pl.read_excel(f"data/segmentacion_{NEGOCIO}.xlsx", sheet_name=str(segm_sheet))
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

    tera_connect.check_suficiencia_adds(file, queries, segm_sheets)

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
                df.write_csv(f"data/optimizations/{file}.txt")
        else:
            cur.executemany(query, segm[add_num].rows())
            add_num += 1


read_query("expuestos_autonomia_opt")

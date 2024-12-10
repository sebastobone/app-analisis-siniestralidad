import teradatasql as td
import polars as pl
from tqdm import tqdm
import json
from pandas import ExcelFile
import constantes as ct


def read_query(file: str) -> None:
    # Tablas de segmentacion
    segm_sheets = [
        str(sheet)
        for sheet in ExcelFile("data/segmentacion.xlsx").sheet_names
        if str(sheet)[:3] == "add"
    ]
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

                for column in ["codigo_op", "codigo_ramo_op", "ramo_desc"]:
                    if column in df.collect_schema().names():
                        raise Exception(f"""¡Falta la columna {column}! 
                                        Es necesaria para las validaciones 
                                        contables. Agregarla a la salida del query.""")

                if (
                    file == "siniestros"
                    and "atipico" not in df.collect_schema().names()
                ):
                    raise Exception("""¡Falta la columna atipico! Si no 
                                    tiene atipicos, agregue una columna 
                                    con ceros a la salida del query.
                                    """)

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
    df_faltante = df.filter(
        pl.any_horizontal([pl.col(col).is_null() for col in ct.APERT_COLS])
    )
    if len(df_faltante) != 0:
        print(df_faltante)
        raise Exception("""¡Alerta! Revise las segmentaciones faltantes.""")

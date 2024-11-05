import teradatasql as td
import polars as pl
from tqdm import tqdm
import json


def read_query(
    file: str,
    credenciales: dict[str, str],
    adds: dict[str, list[str]],
    params_fechas: list[tuple[str, str]],
) -> pl.DataFrame:
    con = td.connect(json.dumps(credenciales))
    cur = con.cursor()

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
        else:
            if "xlsx" in adds[f"{file}_{add_num}"][0]:
                add = pl.read_excel(
                    adds[f"{file}_{add_num}"][0],
                    sheet_name=adds[f"{file}_{add_num}"][1],
                )
            else:
                add = pl.read_csv(
                    adds[f"{file}_{add_num}"][0],
                    separator="\t",
                )
            cur.executemany(query, add.rows())
            add_num += 1

    # Segmentaciones faltantes
    for apertura in ["apertura_canal_desc", "apertura_amparo_desc"]:
        assert (
            "-1" not in df.select(apertura).unique()
        ), f"Alerta! -1 en {apertura}, añadir nueva segmentacion"

    return df


credenciales = {
    "host": "teradata.suranet.com",  # andrrepe' Sebandre2025$
    "user": "sebatoec",  #'user':'laurzare',#sebatoec
    "password": "48N!BLHvJ#i8",  # 'password':'Sura2021'#48N!BLHvJ#i8
}

params_fechas = pl.read_excel(
    "data/segmentacion.xlsx", sheet_name="Fechas", has_header=False
).rows()

# Tablas de segmentacion
adds = {
    "siniestros_0": ["data/segmentacion.xlsx", "Polizas"],
    "siniestros_1": ["data/segmentacion.xlsx", "Canales"],
    "siniestros_2": ["data/segmentacion.xlsx", "Amparos"],
    "siniestros_3": ["data/segmentacion.xlsx", "Atipicos"],
    "primas_0": ["data/segmentacion.xlsx", "Polizas"],
    "primas_1": ["data/segmentacion.xlsx", "Canales"],
    "primas_2": ["data/segmentacion.xlsx", "Amparos"],
    "expuestos_0": ["data/segmentacion.xlsx", "Polizas"],
    "expuestos_1": ["data/segmentacion.xlsx", "Canales"],
    "expuestos_2": ["data/segmentacion.xlsx", "Amparos"],
}

# df_sinis = read_query("siniestros", credenciales, adds, params_fechas)
df_primas = read_query("primas", credenciales, adds, params_fechas)
# df_expuestos = read_query("expuestos", credenciales, adds, params_fechas)

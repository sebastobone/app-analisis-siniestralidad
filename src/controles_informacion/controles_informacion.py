import polars as pl
from time import sleep
import os
import constantes as ct


def group_tera(
    df: pl.LazyFrame, file: str, group_cols: list[str], qtys: list[str]
) -> pl.LazyFrame:
    df_agrup = df.with_columns(
        mes_mov=pl.col("fecha_registro").dt.year() * 100
        + pl.col("fecha_registro").dt.month()
    )

    if file == "siniestros":
        df_agrup = df_agrup.with_columns(
            mes_ocurr=pl.col("fecha_siniestro").dt.year() * 100
            + pl.col("fecha_siniestro").dt.month()
        )

    df_agrup = (
        df_agrup.select(group_cols + qtys).group_by(group_cols).sum().sort(group_cols)
    )

    return df_agrup


def read_sap(cias: list[str], qtys: list[str], mes_corte: int) -> pl.LazyFrame:
    month_map = pl.LazyFrame(
        [
            ("ENE", 1),
            ("FEB", 2),
            ("MAR", 3),
            ("ABR", 4),
            ("MAY", 5),
            ("JUN", 6),
            ("JUL", 7),
            ("AGO", 8),
            ("SEP", 9),
            ("OCT", 10),
            ("NOV", 11),
            ("DIC", 12),
        ],
        schema=["Nombre_Mes", "Mes"],
        orient="row",
    )

    ct = 0
    dfs_cont = pl.LazyFrame()
    for cia in cias:
        for qty in qtys:
            qty = qty.replace("retenido", "cedido")
            df_cont = (
                pl.read_excel(f"data/afo/{cia}.xlsx", sheet_name=qty)
                .lazy()
                .fill_null(0)
                .with_columns(
                    pl.col("Ejercicio/Período")
                    .str.replace("Período 00", "DIC")
                    .str.split(" ")
                    .cast(pl.Array(pl.String, 2))
                    .arr.to_struct(),
                )
                .unnest("Ejercicio/Período")
                .rename({"field_0": "Nombre_Mes", "field_1": "Anno"})
                .with_columns(
                    pl.col("Nombre_Mes").str.replace_many({"PE2": "DIC", "PE1": "DIC"})
                )
                .join(month_map, on="Nombre_Mes")
            )

            df_cont = (
                df_cont.drop(
                    [
                        "column_1",
                        "Ramo Agr"
                        if "Ramo Agr" in df_cont.collect_schema().names()
                        else "Ramo",
                        "Resultado total",
                        "Nombre_Mes",
                    ]
                )
                .unpivot(
                    index=["Anno", "Mes"],
                    variable_name="codigo_ramo_op",
                    value_name=qty,
                )
                .filter(pl.col(qty) != 0)
                .with_columns(
                    pl.col(qty)
                    * (
                        -1
                        if qty in ["pago_cedido", "aviso_bruto"] or "prima" in qty
                        else 1
                    ),
                    codigo_op=pl.lit("01") if cia == "Generales" else pl.lit("02"),
                )
                .fill_null(0)
                .with_columns(
                    mes_mov=pl.col("Anno").cast(pl.Int32) * 100
                    + pl.col("Mes").cast(pl.Int32)
                )
                .filter(pl.col("mes_mov") <= mes_corte)
                .select(["codigo_op", "codigo_ramo_op", "mes_mov", qty])
            )

            if ct == 0:
                dfs_cont = df_cont
            else:
                dfs_cont = pl.concat([dfs_cont, df_cont], how="diagonal")

            ct += 1

    dfs_cont = (
        dfs_cont.group_by(["codigo_op", "codigo_ramo_op", "mes_mov"])
        .sum()
        .sort(["codigo_op", "codigo_ramo_op", "mes_mov"])
    )

    if "pago_bruto" in qtys:
        dfs_cont = dfs_cont.with_columns(
            pago_retenido=pl.col("pago_bruto") - pl.col("pago_cedido"),
            aviso_retenido=pl.col("aviso_bruto") - pl.col("aviso_cedido"),
        )

    return dfs_cont


def consistencia_historica(
    file: str,
    group_cols: list[str],
    qtys: list[str],
    estado_cuadre: str,
    fuente: str,
) -> pl.DataFrame:
    available_files = [
        f
        for f in os.listdir(f"data/controles_informacion/{ct.NEGOCIO}/{estado_cuadre}")
        if f"{file}_{fuente}" in f
        and "sap_vs_tera" not in f
        and "ramo" not in f
        and "consistencia" not in f
    ]

    dfs = pl.LazyFrame(schema=group_cols)
    meses = set()
    for i, f in enumerate(available_files):
        mes_file = f[-11:-5]
        df = pl.read_excel(
            f"data/controles_informacion/{ct.NEGOCIO}/{estado_cuadre}/{f}"
        ).lazy()
        for qty in qtys:
            df = df.rename({qty: f"{qty}_{mes_file}"})

        dfs = (
            df
            if i == 0
            else dfs.join(df, on=group_cols, how="full", coalesce=True).fill_null(0)
        )

        meses.add(mes_file)

    meses_list = list(meses)
    for qty in qtys:
        for n_mes, mes in enumerate(meses_list[1:]):
            dfs = dfs.with_columns(
                (pl.col(f"{qty}_{mes}") - pl.col(f"{qty}_{meses_list[n_mes]}")).alias(
                    f"diferencia_{qty}_{mes}_{meses_list[n_mes]}"
                )
            )

    dfs_eager = dfs.sort(group_cols).collect()
    dfs_eager.write_excel(
        f"data/controles_informacion/{ct.NEGOCIO}/{estado_cuadre}/{file}_{fuente}_consistencia_historica.xlsx",
    )

    return dfs_eager


def valid_contable(
    df_agrup_ramo: pl.LazyFrame,
    dfs_cont: pl.LazyFrame,
    mes_corte: int,
    qtys: list[str],
) -> pl.LazyFrame:
    valid = df_agrup_ramo.join(
        dfs_cont,
        on=["codigo_op", "codigo_ramo_op", "mes_mov"],
        how="left",
        suffix="_SAP",
    ).fill_null(0)

    for qty in qtys:
        valid = (
            valid.with_columns(
                (pl.col(f"{qty}_SAP") - pl.col(qty)).alias(f"diferencia_{qty}"),
            )
            .with_columns(
                (pl.col(f"diferencia_{qty}") / pl.col(f"{qty}_SAP")).alias(
                    f"dif%_{qty}"
                )
            )
            .drop(qty)
        )

        valid_mes = (
            valid.filter(pl.col("mes_mov") == mes_corte)
            .filter(pl.col(f"dif%_{qty}").abs() > 0.05)
            .collect()
        )

        if valid_mes.shape[0] != 0:
            print(f"¡Alerta! Diferencias significativas en {qty}:")
            print(
                valid_mes.select(
                    [
                        "codigo_op",
                        "codigo_ramo_op",
                        f"{qty}_SAP",
                        f"diferencia_{qty}",
                        f"dif%_{qty}",
                    ]
                )
            )

    return valid


def cuadre_contable(df: pl.LazyFrame, file: str, valid: pl.LazyFrame) -> pl.LazyFrame:
    keys = pl.read_excel(
        "data/segmentacion.xlsx", sheet_name=f"Cuadre_Contable_{file.capitalize()}"
    ).lazy()

    agrups = keys.join(
        df.select(
            keys.collect_schema().names() + ["ramo_desc", "apertura_reservas"]
        ).unique(),
        on=keys.collect_schema().names(),
    )

    dif = (
        valid.with_columns(
            [
                pl.col(column).alias(column.replace("diferencia_", ""))
                for column in valid.collect_schema().names()
                if "diferencia" in column
            ]
        )
        .with_columns(
            fecha_registro=(
                (pl.col("mes_mov") // pl.lit(100)).cast(pl.String)
                + "-"
                + (pl.col("mes_mov") % pl.lit(100)).cast(pl.String).str.zfill(2)
                + "-01"
            ).cast(pl.Date)
        )
        .join(agrups, on=["codigo_op", "codigo_ramo_op"])
        .with_columns(
            conteo_pago=0,
            conteo_incurrido=0,
            conteo_desistido=0,
            atipico=0,
            fecha_siniestro=pl.col("fecha_registro"),
        )
        .select(df.collect_schema().names())
    )

    df_cuadre = (
        pl.concat([df, dif], how="vertical_relaxed")
        .group_by(
            df.collect_schema().names()[
                : df.collect_schema().names().index("fecha_registro") + 1
            ]
        )
        .sum()
    )

    df_cuadre.collect().write_csv(f"data/raw/{ct.NEGOCIO}/{file}.csv", separator="\t")
    df_cuadre.collect().write_parquet(f"data/raw/{ct.NEGOCIO}/{file}.parquet")

    return df_cuadre


def integridad_exactitud(
    df: pl.LazyFrame, estado_cuadre: str, file: str, mes_corte: int, qtys: list[str]
) -> None:
    apr_cols = df.collect_schema().names()[: df.collect_schema().names().index(qtys[0])]
    qty_cols = df.collect_schema().names()[df.collect_schema().names().index(qtys[0]) :]

    apr_cols = [col for col in apr_cols if "fecha" not in col]

    print(apr_cols)
    print(qty_cols)

    rep = (
        df.select(apr_cols + qty_cols)
        .with_columns(numero_registros=1)
        .group_by(apr_cols)
        .sum()
        .sort(apr_cols)
        .collect()
    )

    rep.write_excel(
        f"data/controles_informacion/{ct.NEGOCIO}/{estado_cuadre}/{file}_integridad_exactitud_{mes_corte}.xlsx",
    )


def controles_informacion(
    df: pl.LazyFrame,
    file: str,
    group_cols: list[str],
    qtys: list[str],
    estado_cuadre: str,
) -> pl.LazyFrame:
    mes_corte = ct.PARAMS_FECHAS[1][1]

    # Consistencia historica Tera
    group_tera(df, file, group_cols, qtys).collect().write_excel(
        f"data/controles_informacion/{ct.NEGOCIO}/{estado_cuadre}/{file}_tera_{mes_corte}.xlsx",
    )
    consistencia_historica(file, group_cols, qtys, estado_cuadre, fuente="tera")

    df_agrup_ramo = group_tera(
        df,
        file,
        group_cols=["codigo_op", "codigo_ramo_op", "mes_mov"],
        qtys=qtys,
    )

    if file in ("siniestros", "primas"):
        # Consistencia historica SAP
        dfs_cont = read_sap(["Vida", "Generales"], qtys, int(mes_corte)).filter(
            pl.col("codigo_ramo_op").is_in(
                df.select("codigo_ramo_op")
                .unique()
                .collect()
                .get_column("codigo_ramo_op")
            )
        )

        dfs_cont.collect().write_excel(
            f"data/controles_informacion/{ct.NEGOCIO}/{estado_cuadre}/{file}_sap_{mes_corte}.xlsx",
        )
        consistencia_historica(
            file,
            ["codigo_op", "codigo_ramo_op", "mes_mov"],
            qtys,
            estado_cuadre,
            fuente="sap",
        )

        # Diferencias SAP - Tera
        valid = valid_contable(df_agrup_ramo, dfs_cont, int(mes_corte), qtys)

        valid.collect().write_excel(
            f"data/controles_informacion/{ct.NEGOCIO}/{estado_cuadre}/{file}_sap_vs_tera_{mes_corte}.xlsx",
        )

    elif file == "expuestos":
        valid = pl.LazyFrame()
        df_agrup_ramo.collect().write_excel(
            f"data/controles_informacion/{ct.NEGOCIO}/{estado_cuadre}/expuestos_tera_ramo_{mes_corte}.xlsx",
        )

    integridad_exactitud(df, estado_cuadre, file, mes_corte, qtys)

    return valid


def set_permissions(directory, permission="write"):
    import stat

    for dirpath, dirnames, filenames in os.walk(directory):
        for dirname in dirnames:
            dir_full_path = os.path.join(dirpath, dirname)
            if permission == "read":
                os.chmod(dir_full_path, stat.S_IREAD | stat.S_IEXEC)
            elif permission == "write":
                os.chmod(
                    dir_full_path,
                    stat.S_IRWXU
                    | stat.S_IRGRP
                    | stat.S_IXGRP
                    | stat.S_IROTH
                    | stat.S_IXOTH,
                )
        for filename in filenames:
            file_full_path = os.path.join(dirpath, filename)
            if permission == "read":
                os.chmod(file_full_path, stat.S_IREAD)
            elif permission == "write":
                os.chmod(
                    file_full_path,
                    stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH,
                )


def evidencias_parametros():
    from datetime import datetime
    import openpyxl as xl
    import pyautogui
    import shutil
    import tkinter as tk
    from tkinter import messagebox

    mes_corte = ct.PARAMS_FECHAS[1][1]

    root = tk.Tk()
    root.attributes("-topmost", True)

    user_yes = messagebox.askyesno(
        "¿Continuar ejecución?",
        """¿Desea continuar la ejecución? 
        Si sí, no use el computador mientras termina de correr el proceso. 
        Si no, deberá comenzar el proceso nuevamente.""",
    )

    if not user_yes:
        raise Exception("Proceso interrumpido, vuelva a ejecutar.")

    original_file = "data/segmentacion.xlsx"
    stored_file = (
        f"data/controles_informacion/{ct.NEGOCIO}/{mes_corte}_segmentacion.xlsx"
    )

    shutil.copyfile(original_file, stored_file)

    wb = xl.load_workbook(stored_file)
    sheet_name = "CONTROL_EXTRACCION"
    wb.create_sheet(title=sheet_name)
    new_sheet = wb[sheet_name]

    new_sheet["A1"] = "Fecha y hora del fin de la extraccion"
    new_sheet["A2"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    wb.save(stored_file)
    wb.close()

    # Reloj
    if os.name == "nt":
        pyautogui.hotkey("winleft", "alt", "d")
        sleep(0.5)

    pyautogui.screenshot(
        f"data/controles_informacion/{ct.NEGOCIO}/{mes_corte}_extraccion.png"
    )

    if os.name == "nt":
        pyautogui.hotkey("winleft", "alt", "d")

    # sleep(30)


def generar_controles(file: str) -> None:
    if file == "siniestros":
        qtys = ["pago_bruto", "aviso_bruto", "pago_retenido", "aviso_retenido"]
        group_cols = ["apertura_reservas", "mes_ocurr", "mes_mov"]

    elif file == "primas":
        qtys = [
            "prima_bruta",
            "prima_retenida",
            "prima_bruta_devengada",
            "prima_retenida_devengada",
        ]
        group_cols = ["apertura_reservas", "mes_mov"]

    elif file == "expuestos":
        qtys = ["expuestos"]
        group_cols = ["apertura_reservas", "mes_mov"]

    df = pl.scan_parquet(f"data/raw/{ct.NEGOCIO}/{file}.parquet")

    valid_pre_cuadre = controles_informacion(
        df,
        file,
        group_cols,
        qtys,
        estado_cuadre="pre_cuadre_contable",
    )

    if file in ("siniestros", "primas"):
        df.collect().write_csv(
            f"data/raw/{ct.NEGOCIO}/{file}_pre_cuadre.csv", separator="\t"
        )
        df.collect().write_parquet(f"data/raw/{ct.NEGOCIO}/{file}_pre_cuadre.parquet")
        df_cuadrado = cuadre_contable(df, file, valid_pre_cuadre)
        _ = controles_informacion(
            df_cuadrado,
            file,
            group_cols,
            qtys,
            estado_cuadre="post_cuadre_contable",
        )

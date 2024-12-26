import polars as pl
from time import sleep
import os
from src import utils
from src.controles_informacion.cuadre_contable import cuadre_contable
from run import logger
from src import constantes as ct


def agrupar_tera(
    df: pl.LazyFrame, group_cols: list[str], qtys: list[str]
) -> pl.LazyFrame:
    return df.select(group_cols + qtys).group_by(group_cols).sum().sort(group_cols)


def columna_ramo_sap(qty: str) -> str:
    if "prima" in qty or "pago" in qty:
        return "Ramo Agr"
    else:
        return "Ramo"


def leer_sap(cias: list[str], qtys: list[str], mes_corte: int) -> pl.LazyFrame:
    dfs_sap = []
    for cia in cias:
        for qty in qtys:
            qty = qty.replace("retenido", "cedido")
            df_sap = (
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
                .join(ct.MONTH_MAP, on="Nombre_Mes")
                .drop(
                    [
                        "column_1",
                        columna_ramo_sap(qty),
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
                    fecha_registro=pl.date(
                        pl.col("Anno").cast(pl.Int32), pl.col("Mes"), 1
                    )
                )
                .filter(pl.col("fecha_registro") <= utils.yyyymm_to_date(mes_corte))
                .select(["codigo_op", "codigo_ramo_op", "fecha_registro", qty])
            )

            if (
                utils.yyyymm_to_date(mes_corte)
                not in df_sap.collect().get_column("fecha_registro").unique()
            ):
                raise Exception(
                    f"""¡Error! No se pudo encontrar el mes {mes_corte}
                    en la hoja {qty} del AFO de {cia}. Actualizar el AFO."""
                )

            dfs_sap.append(df_sap)

    df_sap_full = (
        pl.LazyFrame(pl.concat(dfs_sap, how="diagonal"))
        .group_by(["codigo_op", "codigo_ramo_op", "fecha_registro"])
        .sum()
        .sort(["codigo_op", "codigo_ramo_op", "fecha_registro"])
    )

    if "pago_bruto" in qtys:
        df_sap_full = df_sap_full.with_columns(
            pago_retenido=pl.col("pago_bruto") - pl.col("pago_cedido"),
            aviso_retenido=pl.col("aviso_bruto") - pl.col("aviso_cedido"),
        )

    return df_sap_full


def generar_consistencia_historica(
    file: str,
    group_cols: list[str],
    qtys: list[str],
    estado_cuadre: str,
    fuente: str,
) -> None:
    available_files = [
        f
        for f in os.listdir(f"data/controles_informacion/{estado_cuadre}")
        if f"{file}_{fuente}" in f
        and "sap_vs_tera" not in f
        and "ramo" not in f
        and "consistencia" not in f
    ]

    dfs = pl.LazyFrame(schema=group_cols)
    meses = set()
    for i, f in enumerate(available_files):
        mes_file = f[-11:-5]
        df = (
            pl.read_excel(f"data/controles_informacion/{estado_cuadre}/{f}")
            .lazy()
            .rename({qty: f"{qty}_{mes_file}" for qty in qtys})
        )

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

    dfs.sort(group_cols).collect().write_excel(
        f"data/controles_informacion/{estado_cuadre}/{file}_{fuente}_consistencia_historica.xlsx",
    )


def comparar_sap_tera(
    df_tera: pl.LazyFrame,
    df_sap: pl.LazyFrame,
    mes_corte: int,
    qtys: list[str],
) -> pl.DataFrame:
    base_comp = df_tera.join(
        df_sap,
        on=["codigo_op", "codigo_ramo_op", "fecha_registro"],
        how="left",
        suffix="_SAP",
    ).fill_null(0)

    for qty in qtys:
        base_comp = base_comp.with_columns(
            (pl.col(f"{qty}_SAP") - pl.col(qty)).alias(f"diferencia_{qty}"),
        ).with_columns(
            (pl.col(f"diferencia_{qty}") / pl.col(f"{qty}_SAP"))
            .alias(f"dif%_{qty}")
            .fill_nan(0)
        )

        comp_mes = (
            base_comp.filter(
                pl.col("fecha_registro") == utils.yyyymm_to_date(mes_corte)
            )
            .filter(pl.col(f"dif%_{qty}").abs() > 0.05)
            .collect()
        )

        if comp_mes.shape[0] != 0:
            dif = comp_mes.select(
                [
                    "codigo_op",
                    "codigo_ramo_op",
                    qty,
                    f"{qty}_SAP",
                    f"diferencia_{qty}",
                    f"dif%_{qty}",
                ]
            )
            logger.warning(f"""¡Alerta! Diferencias significativas en {qty}: {dif}""")

    return base_comp.collect()


def generar_integridad_exactitud(
    df: pl.LazyFrame, estado_cuadre: str, file: str, mes_corte: int, qtys: list[str]
) -> None:
    apr_cols = df.collect_schema().names()[: df.collect_schema().names().index(qtys[0])]
    qty_cols = df.collect_schema().names()[df.collect_schema().names().index(qtys[0]) :]

    apr_cols = [col for col in apr_cols if "fecha" not in col]

    return (
        df.select(apr_cols + qty_cols)
        .with_columns(numero_registros=1)
        .group_by(apr_cols)
        .sum()
        .sort(apr_cols)
        .collect()
        .write_excel(
            f"data/controles_informacion/{estado_cuadre}/{file}_integridad_exactitud_{mes_corte}.xlsx",
        )
    )


def controles_informacion(
    df: pl.LazyFrame,
    file: str,
    group_cols: list[str],
    qtys: list[str],
    mes_corte: int,
    estado_cuadre: str,
) -> pl.DataFrame:
    agrupar_tera(df, group_cols, qtys).collect().write_excel(
        f"data/controles_informacion/{estado_cuadre}/{file}_tera_{mes_corte}.xlsx",
    )
    generar_consistencia_historica(file, group_cols, qtys, estado_cuadre, fuente="tera")

    df_tera_ramo = agrupar_tera(
        df,
        group_cols=["codigo_op", "codigo_ramo_op", "fecha_registro"],
        qtys=qtys,
    )

    if file in ("siniestros", "primas"):
        df_sap = leer_sap(["Vida", "Generales"], qtys, int(mes_corte)).filter(
            pl.col("codigo_ramo_op").is_in(
                df.select("codigo_ramo_op")
                .unique()
                .collect()
                .get_column("codigo_ramo_op")
            )
        )
        df_sap.collect().write_excel(
            f"data/controles_informacion/{estado_cuadre}/{file}_sap_{mes_corte}.xlsx",
        )
        generar_consistencia_historica(
            file,
            ["codigo_op", "codigo_ramo_op", "fecha_registro"],
            qtys,
            estado_cuadre,
            fuente="sap",
        )

        difs_sap_tera = comparar_sap_tera(df_tera_ramo, df_sap, int(mes_corte), qtys)
        difs_sap_tera.write_excel(
            f"data/controles_informacion/{estado_cuadre}/{file}_sap_vs_tera_{mes_corte}.xlsx",
        )

    elif file == "expuestos":
        difs_sap_tera = pl.DataFrame()

    generar_integridad_exactitud(df, estado_cuadre, file, mes_corte, qtys)

    return difs_sap_tera


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


def generar_evidencias_parametros(negocio: str, mes_corte: int):
    from datetime import datetime
    import openpyxl as xl
    import pyautogui
    import shutil

    original_file = f"data/segmentacion_{negocio}.xlsx"
    stored_file = f"data/controles_informacion/{mes_corte}_segmentacion_{negocio}.xlsx"

    shutil.copyfile(original_file, stored_file)

    wb = xl.load_workbook(stored_file)
    sheet_name = "CONTROL_EXTRACCION"
    wb.create_sheet(title=sheet_name)

    wb[sheet_name]["A1"] = "Fecha y hora del fin de la extraccion"
    wb[sheet_name]["A2"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    wb.save(stored_file)
    wb.close()

    # Reloj
    if os.name == "nt":
        pyautogui.hotkey("winleft", "alt", "d")
        sleep(0.5)

    pyautogui.screenshot(f"data/controles_informacion/{mes_corte}_extraccion.png")

    if os.name == "nt":
        pyautogui.hotkey("winleft", "alt", "d")


def ajustar_fraude(df: pl.LazyFrame):
    fraude = pl.LazyFrame(
        utils.lowercase_columns(
            pl.read_excel("data/segmentacion_soat.xlsx", sheet_name="Ajustes_Fraude")
        )
    ).drop("tipo_ajuste")

    df = (
        pl.concat([df, fraude])
        .group_by(
            df.collect_schema().names()[
                : df.collect_schema().names().index("fecha_registro") + 1
            ]
        )
        .sum()
    )
    df.collect().write_csv("data/raw/siniestros.csv", separator="\t")
    df.collect().write_parquet("data/raw/siniestros.parquet")

    return df


def generar_controles(
    file: str,
    negocio: str,
    mes_corte: int,
    cuadre_contable_sinis: bool = False,
    add_fraude_soat: bool = False,
    cuadre_contable_primas: bool = False,
) -> None:
    if file == "siniestros":
        qtys = ["pago_bruto", "aviso_bruto", "pago_retenido", "aviso_retenido"]
        group_cols = ["apertura_reservas", "fecha_siniestro", "fecha_registro"]

    elif file == "primas":
        qtys = [
            "prima_bruta",
            "prima_retenida",
            "prima_bruta_devengada",
            "prima_retenida_devengada",
        ]
        group_cols = ["apertura_reservas", "fecha_registro"]

    elif file == "expuestos":
        qtys = ["expuestos"]
        group_cols = ["apertura_reservas", "fecha_registro"]

    df = pl.scan_parquet(f"data/raw/{file}.parquet")

    difs_sap_tera_pre_cuadre = controles_informacion(
        df,
        file,
        group_cols,
        qtys,
        mes_corte,
        estado_cuadre="pre_cuadre_contable",
    )

    if (file == "siniestros" and cuadre_contable_sinis) or (
        file == "primas" and cuadre_contable_primas
    ):
        df.collect().write_csv(f"data/raw/{file}_pre_cuadre.csv", separator="\t")
        df = cuadre_contable(negocio, file, df, difs_sap_tera_pre_cuadre.lazy())
        _ = controles_informacion(
            df,
            file,
            group_cols,
            qtys,
            mes_corte,
            estado_cuadre="post_cuadre_contable",
        )

    if negocio == "soat" and file == "siniestros" and add_fraude_soat:
        df.collect().write_csv("data/raw/siniestros_post_cuadre.csv", separator="\t")
        df = ajustar_fraude(df)
        _ = controles_informacion(
            df,
            file,
            group_cols,
            qtys,
            mes_corte,
            estado_cuadre="post_ajustes_fraude",
        )

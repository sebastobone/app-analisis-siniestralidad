import os
from time import sleep
from typing import Literal

import polars as pl

from src import constantes as ct
from src import utils
from src.controles_informacion.cuadre_contable import cuadre_contable
from src.logger_config import logger
from src.models import Parametros


def agrupar_tera(
    df: pl.LazyFrame, group_cols: list[str], qtys: list[str]
) -> pl.LazyFrame:
    return df.select(group_cols + qtys).group_by(group_cols).sum().sort(group_cols)


def columna_ramo_sap(qty: str) -> str:
    if "prima" in qty or "pago" in qty:
        return "Ramo Agr"
    else:
        return "Ramo"


def signo_sap(qty: str) -> int:
    return -1 if qty in ["pago_cedido", "aviso_bruto"] or "prima" in qty else 1


def definir_hojas_afo(qtys: list[str]) -> set[str]:
    hojas_afo = set()
    for qty in qtys:
        if "prima" in qty:
            hojas_afo.update(
                set(
                    (
                        "prima_bruta",
                        "prima_retenida",
                        "prima_bruta_devengada",
                        "prima_retenida_devengada",
                    )
                )
            )
        elif "pago" in qty:
            hojas_afo.update(set(("pago_bruto", "pago_cedido")))
        elif "aviso" in qty:
            hojas_afo.update(set(("aviso_bruto", "aviso_cedido")))
        elif "rpnd" in qty:
            hojas_afo.update(set(("rpnd_bruto", "rpnd_cedido")))
    return hojas_afo


def transformar_hoja_afo(
    df: pl.DataFrame, cia: str, qty: str, mes_corte: int
) -> pl.LazyFrame:
    if (
        f"{ct.NOMBRE_MES[mes_corte % 100]} {mes_corte // 100}"
        not in df.get_column("Ejercicio/Período").unique()
    ):
        logger.error(
            f"""¡Error! No se pudo encontrar el mes {mes_corte}
            en la hoja {qty} del AFO de {cia}. Actualizar el AFO."""
        )
        raise ValueError

    return (
        df.lazy()
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
        .join(ct.MONTH_MAP.lazy(), on="Nombre_Mes")
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
        .with_columns(
            pl.col(qty) * signo_sap(qty),
            codigo_op=pl.lit("01") if cia == "Generales" else pl.lit("02"),
            fecha_registro=pl.date(pl.col("Anno"), pl.col("Mes"), 1),
        )
        .fill_null(0)
        .filter(
            (pl.col("fecha_registro") <= utils.yyyymm_to_date(mes_corte))
            & (pl.col(qty) != 0)
        )
        .select(["codigo_op", "codigo_ramo_op", "fecha_registro", qty])
    )


def crear_columnas_faltantes_sap(df: pl.LazyFrame) -> pl.LazyFrame:
    for qty in df.collect_schema().names():
        if "retenid" in qty:
            df = df.with_columns(
                (pl.col(qty.replace("retenid", "brut")) - pl.col(qty)).alias(
                    qty.replace("retenid", "cedid")
                )
            )
        elif "cedid" in qty:
            df = df.with_columns(
                (pl.col(qty.replace("cedid", "brut")) - pl.col(qty)).alias(
                    qty.replace("cedid", "retenid")
                )
            )

    return df


def consolidar_sap(cias: list[str], qtys: list[str], mes_corte: int) -> pl.DataFrame:
    dfs_sap = []
    for cia in cias:
        for hoja_afo in definir_hojas_afo(qtys):
            dfs_sap.append(
                transformar_hoja_afo(
                    pl.read_excel(f"data/afo/{cia}.xlsx", sheet_name=hoja_afo),
                    cia,
                    hoja_afo,
                    mes_corte,
                )
            )

    df_sap_full = (
        pl.concat(dfs_sap, how="diagonal")
        .group_by(["codigo_op", "codigo_ramo_op", "fecha_registro"])
        .sum()
        .sort(["codigo_op", "codigo_ramo_op", "fecha_registro"])
    )

    df_sap_full = crear_columnas_faltantes_sap(df_sap_full)
    logger.success(f"Cantidades {qtys} leidas del AFO sin errores.")

    return df_sap_full.select(
        ["codigo_op", "codigo_ramo_op", "fecha_registro"] + qtys
    ).collect()


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
        f"data/controles_informacion/{estado_cuadre}/{file}_{fuente}_consistencia_historica.xlsx"
    )

    logger.success(
        f"""Archivo de consistencia historica para {file} - {fuente} - {estado_cuadre}
        generado exitosamente.""".replace("\n", " ")
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
        validate="1:1",
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


def definir_cantidades_control(
    file: Literal["siniestros", "primas", "expuestos"],
) -> tuple[list[str], list[str]]:
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

    return qtys, group_cols


def controles_informacion(
    df: pl.LazyFrame,
    file: Literal["siniestros", "primas", "expuestos"],
    mes_corte: int,
    estado_cuadre: Literal[
        "pre_cuadre_contable", "post_cuadre_contable", "post_ajustes_fraude"
    ],
) -> pl.DataFrame:
    qtys, group_cols = definir_cantidades_control(file)
    agrupar_tera(df, group_cols, qtys).collect().write_excel(
        f"data/controles_informacion/{estado_cuadre}/{file}_tera_{mes_corte}.xlsx",
    )
    generar_consistencia_historica(file, group_cols, qtys, estado_cuadre, fuente="tera")

    df_tera = agrupar_tera(
        df,
        group_cols=["codigo_op", "codigo_ramo_op", "fecha_registro"],
        qtys=qtys,
    )

    if file in ("siniestros", "primas"):
        df_sap = consolidar_sap(["Vida", "Generales"], qtys, int(mes_corte)).filter(
            pl.col("codigo_ramo_op").is_in(
                df.select("codigo_ramo_op")
                .unique()
                .collect()
                .get_column("codigo_ramo_op")
            )
        )
        df_sap.write_excel(
            f"data/controles_informacion/{estado_cuadre}/{file}_sap_{mes_corte}.xlsx",
        )
        generar_consistencia_historica(
            file,
            ["codigo_op", "codigo_ramo_op", "fecha_registro"],
            qtys,
            estado_cuadre,
            fuente="sap",
        )

        difs_sap_tera = comparar_sap_tera(df_tera, df_sap.lazy(), int(mes_corte), qtys)
        difs_sap_tera.write_excel(
            f"data/controles_informacion/{estado_cuadre}/{file}_sap_vs_tera_{mes_corte}.xlsx",
        )

    elif file == "expuestos":
        difs_sap_tera = pl.DataFrame()

    generar_integridad_exactitud(df, estado_cuadre, file, mes_corte, qtys)

    logger.success(
        f"""Revisiones de informacion y generacion de controles terminada
        para {file} en el estado {estado_cuadre}.""".replace("\n", " ")
    )

    return difs_sap_tera


def set_permissions(
    directory: str, permission: Literal["read", "write"] = "write"
) -> None:
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
    import shutil
    from datetime import datetime

    import openpyxl as xl
    import pyautogui

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

    logger.success("Evidencias de controles generadas exitosamente.")


def ajustar_fraude(df: pl.LazyFrame, mes_corte: int):
    fraude = (
        pl.LazyFrame(
            pl.read_excel("data/segmentacion_soat.xlsx", sheet_name="Ajustes_Fraude")
        )
        .drop("tipo_ajuste")
        .filter(pl.col("fecha_registro") <= utils.yyyymm_to_date(mes_corte))
    )

    df = (
        pl.concat([df, fraude], how="vertical_relaxed")
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
    file: Literal["siniestros", "primas", "expuestos"], p: Parametros
) -> None:
    df = pl.scan_parquet(f"data/raw/{file}.parquet")

    difs_sap_tera_pre_cuadre = controles_informacion(
        df,
        file,
        p.mes_corte,
        estado_cuadre="pre_cuadre_contable",
    )

    if (file == "siniestros" and p.cuadre_contable_sinis) or (
        file == "primas" and p.cuadre_contable_primas
    ):
        df.collect().write_csv(f"data/raw/{file}_pre_cuadre.csv", separator="\t")
        df.collect().write_parquet(f"data/raw/{file}_pre_cuadre.parquet")
        df = cuadre_contable(p.negocio, file, df, difs_sap_tera_pre_cuadre.lazy())
        _ = controles_informacion(
            df,
            file,
            p.mes_corte,
            estado_cuadre="post_cuadre_contable",
        )

    if p.negocio == "soat" and file == "siniestros" and p.add_fraude_soat:
        df.collect().write_csv("data/raw/siniestros_post_cuadre.csv", separator="\t")
        df.collect().write_parquet("data/raw/siniestros_post_cuadre.parquet")
        df = ajustar_fraude(df, p.mes_corte)
        _ = controles_informacion(
            df,
            file,
            p.mes_corte,
            estado_cuadre="post_ajustes_fraude",
        )

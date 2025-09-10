from datetime import date
from pathlib import Path

import polars as pl

from src import constantes as ct
from src import utils
from src.logger_config import logger
from src.models import Afos, Parametros

CANTIDADES_NECESARIAS = ct.COLUMNAS_SINIESTROS_CUADRE + list(
    ct.VALORES["primas"].keys()
)


async def procesar_afos(afos: Afos, p: Parametros) -> None:
    if afos.generales:
        contenido = await afos.generales.read()
        guardar_afo(contenido, "Generales")

    if afos.vida:
        contenido = await afos.vida.read()
        guardar_afo(contenido, "Vida")

    afos_necesarios = determinar_afos_necesarios(p.negocio)
    await validar_existencia_afos(afos_necesarios)

    for afo in afos_necesarios:
        hojas_necesarias = {
            hoja: pl.read_excel(f"data/afo/{afo}.xlsx", sheet_name=hoja)
            for hoja in definir_hojas_afo(CANTIDADES_NECESARIAS)
        }
        validar_mes_corte_afo(hojas_necesarias, p.mes_corte, afo)


def guardar_afo(contenido: bytes, cia: str) -> None:
    with open(f"data/afo/{cia}.xlsx", "wb") as f:
        f.write(contenido)
    logger.info(f"AFO de {cia} guardado en data/afo/{cia}.xlsx")


async def validar_existencia_afos(afos_necesarios: list[str]) -> None:
    for afo in afos_necesarios:
        if not Path(f"data/afo/{afo}.xlsx").exists():
            raise FileNotFoundError(
                utils.limpiar_espacios_log(
                    f"""
                    El AFO de {afo} no ha sido almacenado. Carguelo
                    y vuelva a intentar.
                    """
                )
            )


def validar_mes_corte_afo(hojas: dict[str, pl.DataFrame], mes_corte: date, cia: str):
    mes_corte_afo = f"{ct.NOMBRE_MES[mes_corte.month]} {mes_corte.year}"

    for qty, df in hojas.items():
        if mes_corte_afo not in df.get_column("Ejercicio/Período").unique().to_list():
            raise ValueError(
                utils.limpiar_espacios_log(
                    f"""
                    ¡Error! No se pudo encontrar el mes {mes_corte_afo}
                    en la hoja {qty} del AFO de {cia}. Actualice el AFO
                    y carguelo de nuevo.
                    """
                )
            )


def determinar_afos_necesarios(negocio: str) -> list[str]:
    companias = (
        utils.obtener_aperturas(negocio, "siniestros")
        .get_column("codigo_op")
        .unique()
        .to_list()
    )
    afos = []
    if "01" in companias:
        afos.append("Generales")
    if "02" in companias:
        afos.append("Vida")
    return afos


async def consolidar_sap(
    negocio: str, qtys: list[str], mes_corte: date
) -> pl.DataFrame:
    dfs_sap = []
    for cia in determinar_afos_necesarios(negocio):
        for hoja_afo in definir_hojas_afo(qtys):
            df = pl.read_excel(f"data/afo/{cia}.xlsx", sheet_name=hoja_afo)
            dfs_sap.append(await transformar_hoja_afo(df, cia, hoja_afo, mes_corte))

    return (
        pl.DataFrame(pl.concat(dfs_sap, how="diagonal"))
        .group_by(["codigo_op", "codigo_ramo_op", "fecha_registro"])
        .sum()
        .sort(["codigo_op", "codigo_ramo_op", "fecha_registro"])
        .pipe(crear_columnas_faltantes_sap)
        .select(["codigo_op", "codigo_ramo_op", "fecha_registro"] + qtys)
    )


async def transformar_hoja_afo(
    df: pl.DataFrame, cia: str, qty: str, mes_corte: date
) -> pl.DataFrame:
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
        .drop(["column_1", columna_ramo_sap(qty), "Resultado total", "Nombre_Mes"])
        .unpivot(index=["Anno", "Mes"], variable_name="codigo_ramo_op", value_name=qty)
        .with_columns(
            pl.col(qty).cast(pl.Float64) * signo_sap(qty),
            codigo_op=pl.lit("01") if cia == "Generales" else pl.lit("02"),
            fecha_registro=pl.date(pl.col("Anno"), pl.col("Mes"), 1),
        )
        .fill_null(0)
        .filter((pl.col("fecha_registro") <= mes_corte) & (pl.col(qty) != 0))
        .select(["codigo_op", "codigo_ramo_op", "fecha_registro", qty])
        .collect()
    )


def definir_hojas_afo(qtys: list[str]) -> set[str]:
    hojas_afo = set()
    for qty in qtys:
        if "prima" in qty:
            hojas_afo.update(set(ct.VALORES["primas"].keys()))
        elif "pago" in qty:
            hojas_afo.update(set(("pago_bruto", "pago_cedido")))
        elif "aviso" in qty:
            hojas_afo.update(set(("aviso_bruto", "aviso_cedido")))
        elif "rpnd" in qty:
            hojas_afo.update(set(("rpnd_bruto", "rpnd_cedido")))
    return hojas_afo


def crear_columnas_faltantes_sap(df: pl.DataFrame) -> pl.DataFrame:
    new_cols = []
    for qty in df.collect_schema().names():
        if "retenid" in qty:
            new_cols.append(
                (pl.col(qty.replace("retenid", "brut")) - pl.col(qty)).alias(
                    qty.replace("retenid", "cedid")
                )
            )
        elif "cedid" in qty:
            new_cols.append(
                (pl.col(qty.replace("cedid", "brut")) - pl.col(qty)).alias(
                    qty.replace("cedid", "retenid")
                )
            )
    return df.with_columns(new_cols)


def columna_ramo_sap(qty: str) -> str:
    if "prima" in qty or "pago" in qty:
        return "Ramo Agr"
    else:
        return "Ramo"


def signo_sap(qty: str) -> int:
    return -1 if qty in ["pago_cedido", "aviso_bruto"] or "prima" in qty else 1

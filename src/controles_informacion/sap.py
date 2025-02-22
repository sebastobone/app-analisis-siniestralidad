import polars as pl

from src import constantes as ct
from src import utils
from src.logger_config import logger


async def consolidar_sap(
    cias: list[str], qtys: list[str], mes_corte: int
) -> pl.DataFrame:
    dfs_sap = []
    for cia in cias:
        for hoja_afo in definir_hojas_afo(qtys):
            dfs_sap.append(
                await transformar_hoja_afo(
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

    return df_sap_full.select(["codigo_op", "codigo_ramo_op", "fecha_registro"] + qtys)


async def transformar_hoja_afo(
    df: pl.DataFrame, cia: str, qty: str, mes_corte: int
) -> pl.DataFrame:
    if (
        f"{ct.NOMBRE_MES[mes_corte % 100]} {mes_corte // 100}"
        not in df.get_column("Ejercicio/Período").unique()
    ):
        logger.error(
            utils.limpiar_espacios_log(
                f"""
                ¡Error! No se pudo encontrar el mes {mes_corte}
                en la hoja {qty} del AFO de {cia}. Actualizar el AFO.
                """
            )
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
            pl.col(qty).cast(pl.Float64) * signo_sap(qty),
            codigo_op=pl.lit("01") if cia == "Generales" else pl.lit("02"),
            fecha_registro=pl.date(pl.col("Anno"), pl.col("Mes"), 1),
        )
        .fill_null(0)
        .filter(
            (pl.col("fecha_registro") <= utils.yyyymm_to_date(mes_corte))
            & (pl.col(qty) != 0)
        )
        .select(["codigo_op", "codigo_ramo_op", "fecha_registro", qty])
        .collect()
    )


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


def crear_columnas_faltantes_sap(df: pl.DataFrame) -> pl.DataFrame:
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


def columna_ramo_sap(qty: str) -> str:
    if "prima" in qty or "pago" in qty:
        return "Ramo Agr"
    else:
        return "Ramo"


def signo_sap(qty: str) -> int:
    return -1 if qty in ["pago_cedido", "aviso_bruto"] or "prima" in qty else 1

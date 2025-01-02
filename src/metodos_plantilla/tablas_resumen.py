from typing import Literal

import polars as pl
import xlwings as xw

from src import constantes as ct
from src import utils
from src.metodos_plantilla import insumos as ins


def df_aperturas() -> pl.LazyFrame:
    return pl.scan_parquet("data/raw/siniestros.parquet")


def aperturas(negocio: str) -> pl.DataFrame:
    return (
        df_aperturas()
        .with_columns(ramo_desc=utils.complementar_col_ramo_desc())
        .select(["apertura_reservas", "ramo_desc"] + utils.columnas_aperturas(negocio))
        .drop(["codigo_op", "codigo_ramo_op"])
        .unique()
        .sort("apertura_reservas")
        .collect()
    )


def generar_tabla(
    sheet: xw.Sheet, df: pl.DataFrame, table_name: str, loc: tuple[int, int]
) -> None:
    df_pd = df.to_pandas()
    if table_name in [table.name for table in sheet.tables]:
        sheet.tables[table_name].update(df_pd, index=False)
    else:
        _ = sheet.tables.add(
            source=sheet.cells(loc[0], loc[1]), name=table_name
        ).update(df_pd, index=False)


def tablas_resumen(
    path_plantilla: str,
    periodicidades: list[list[str]],
    tipo_analisis: Literal["triangulos", "entremes"],
    aperturas: pl.LazyFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    diagonales = ins.df_triangulos(path_plantilla)
    atipicos = ins.df_atipicos(path_plantilla)
    expuestos = ins.df_expuestos(path_plantilla)
    primas = ins.df_primas(path_plantilla)

    base_cols = aperturas.collect_schema().names()[1:]

    diagonales = (
        diagonales.filter(pl.col("diagonal") == 1)
        .join(
            pl.LazyFrame(
                periodicidades,
                schema=["apertura_reservas", "periodicidad_ocurrencia"],
                orient="row",
            ),
            on=["apertura_reservas", "periodicidad_ocurrencia"],
        )
        .select(
            [
                "apertura_reservas",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
            + ct.COLUMNAS_QTYS
        )
    )

    if tipo_analisis == "entremes":
        ult_ocurr = (
            ins.df_ult_ocurr(path_plantilla)
            .join(
                pl.LazyFrame(
                    periodicidades,
                    schema=["apertura_reservas", "periodicidad_triangulo"],
                    orient="row",
                ),
                on=["apertura_reservas", "periodicidad_triangulo"],
            )
            .drop("periodicidad_triangulo")
        )

        diagonales = (
            diagonales.filter(
                pl.col("periodo_ocurrencia")
                != pl.col("periodo_ocurrencia").max().over("apertura_reservas")
            )
            .collect()
            .vstack(ult_ocurr.collect())
            .lazy()
        )

    diagonales_df = (
        diagonales.join(aperturas, on="apertura_reservas")
        .select(
            [
                "apertura_reservas",
            ]
            + aperturas.collect_schema().names()[1:]
            + [
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
            + ct.COLUMNAS_QTYS
        )
        .join(
            expuestos.drop("vigentes"),
            on=base_cols + ["periodicidad_ocurrencia", "periodo_ocurrencia"],
            how="left",
        )
        .join(
            primas,
            on=base_cols + ["periodicidad_ocurrencia", "periodo_ocurrencia"],
            how="left",
        )
        .fill_null(0)
        .with_columns(
            frec_ultimate=0,
            conteo_ultimate=0,
            seve_ultimate_bruto=0,
            seve_ultimate_retenido=0,
            plata_ultimate_bruto=0,
            plata_ultimate_contable_bruto=0,
            plata_ultimate_retenido=0,
            plata_ultimate_contable_retenido=0,
            aviso_bruto=pl.col("incurrido_bruto") - pl.col("pago_bruto"),
            aviso_retenido=pl.col("incurrido_retenido") - pl.col("pago_retenido"),
            ibnr_bruto=0,
            ibnr_contable_bruto=0,
            ibnr_retenido=0,
            ibnr_contable_retenido=0,
        )
        .sort(["apertura_reservas", "periodo_ocurrencia"])
        .collect()
    )

    atipicos_df = (
        atipicos.join(aperturas, on="apertura_reservas")
        .select(
            [
                "apertura_reservas",
            ]
            + aperturas.collect_schema().names()[1:]
            + [
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
            + ct.COLUMNAS_QTYS
        )
        .join(
            expuestos.drop("vigentes"),
            on=base_cols + ["periodicidad_ocurrencia", "periodo_ocurrencia"],
            how="left",
        )
        .join(
            primas,
            on=base_cols + ["periodicidad_ocurrencia", "periodo_ocurrencia"],
            how="left",
        )
        .fill_null(0)
        .with_columns(
            frec_ultimate=pl.col("conteo_incurrido") / pl.col("expuestos"),
            conteo_ultimate=pl.col("conteo_incurrido"),
            seve_ultimate_bruto=pl.col("incurrido_bruto") / pl.col("conteo_incurrido"),
            seve_ultimate_retenido=pl.col("incurrido_retenido")
            / pl.col("conteo_incurrido"),
            plata_ultimate_bruto=pl.col("incurrido_bruto"),
            plata_ultimate_contable_bruto=pl.col("incurrido_bruto"),
            plata_ultimate_retenido=pl.col("incurrido_retenido"),
            plata_ultimate_contable_retenido=pl.col("incurrido_retenido"),
            aviso_bruto=pl.col("incurrido_bruto") - pl.col("pago_bruto"),
            aviso_retenido=pl.col("incurrido_retenido") - pl.col("pago_retenido"),
            ibnr_bruto=0,
            ibnr_contable_bruto=0,
            ibnr_retenido=0,
            ibnr_contable_retenido=0,
        )
        .sort(["apertura_reservas", "periodo_ocurrencia"])
        .collect()
    )

    return diagonales_df, expuestos.collect(), primas.collect(), atipicos_df

import polars as pl

from src import utils
from src.logger_config import logger


def cuadre_contable(
    negocio: str, file: str, df: pl.LazyFrame, dif_sap_vs_tera: pl.LazyFrame
) -> pl.LazyFrame:
    if negocio == "soat":
        return cuadre_contable_soat(file, df, dif_sap_vs_tera)
    elif negocio == "autonomia":
        return cuadre_contable_autonomia(file, df, dif_sap_vs_tera)
    else:
        logger.error(
            f"""Definio hacer el cuadre contable, pero el negocio 
            {negocio} no tiene ninguna estrategia de cuadre implementada.
            """
        )
        raise ValueError


def concat_df_dif(df: pl.LazyFrame, dif: pl.LazyFrame) -> pl.LazyFrame:
    return (
        pl.concat([df, dif], how="vertical_relaxed")
        .group_by(
            df.collect_schema().names()[
                : df.collect_schema().names().index("fecha_registro") + 1
            ]
        )
        .sum()
        .sort(
            df.collect_schema().names()[
                : df.collect_schema().names().index("fecha_registro") + 1
            ]
        )
    )


def cuadre_contable_autonomia(
    file: str, df: pl.LazyFrame, dif_sap_vs_tera: pl.LazyFrame
) -> pl.LazyFrame:
    agrups = utils.lowercase_columns(
        pl.read_excel(
            "data/segmentacion_autonomia.xlsx",
            sheet_name=f"Cuadre_Contable_{file.capitalize()}",
        )
    )

    dif = (
        dif_sap_vs_tera.with_columns(
            [
                pl.col(column).alias(column.replace("diferencia_", ""))
                for column in dif_sap_vs_tera.collect_schema().names()
                if "diferencia" in column
            ]
        )
        .join(pl.LazyFrame(agrups), on=["codigo_op", "codigo_ramo_op"])
        .with_columns(
            conteo_pago=0,
            conteo_incurrido=0,
            conteo_desistido=0,
            atipico=0,
            fecha_siniestro=pl.col("fecha_registro"),
        )
        .select(df.collect_schema().names())
    )

    df_cuadre = concat_df_dif(df, dif).collect()

    df_cuadre.write_csv(f"data/raw/{file}.csv", separator="\t")
    df_cuadre.write_parquet(f"data/raw/{file}.parquet")

    return df_cuadre.lazy()


def apertura_dif_soat() -> pl.LazyFrame:
    """Modificable, aca se decide en que apertura vamos a meter la diferencia SAP-Tera"""
    return pl.LazyFrame(
        {
            "codigo_op": ["01"],
            "codigo_ramo_op": ["041"],
            "ramo_desc": ["AUTOS OBLIGATORIO"],
            "apertura_canal_desc": ["DIGITAL"],
            "apertura_amparo_desc": ["Total"],
            "tipo_vehiculo": ["MOTO"],
        }
    ).with_columns(utils.col_apertura_reservas("soat"))


def cuadre_contable_soat(
    file: str, df: pl.LazyFrame, dif_sap_vs_tera: pl.LazyFrame
) -> pl.LazyFrame:
    ramos = df.select(["codigo_op", "codigo_ramo_op", "ramo_desc"]).unique()

    dif = (
        dif_sap_vs_tera.filter(
            pl.col("fecha_registro") == pl.col("fecha_registro").max()
        )
        .select(
            ["codigo_op", "codigo_ramo_op", "fecha_registro"]
            + [
                col
                for col in dif_sap_vs_tera.collect_schema().names()
                if "diferencia" in col
            ]
        )
        .rename(
            {
                col: col.replace("diferencia_", "")
                for col in dif_sap_vs_tera.collect_schema().names()
                if "diferencia" in col
            }
        )
        .join(ramos, on=["codigo_op", "codigo_ramo_op"])
    )

    dif = dif.drop(
        [
            column
            for column in dif.collect_schema().names()
            if column not in df.collect_schema().names()
        ]
    )

    dif = dif.join(apertura_dif_soat(), on=["codigo_op", "codigo_ramo_op"])

    if file == "siniestros":
        dif = dif.with_columns(
            conteo_pago=0,
            conteo_incurrido=0,
            conteo_desistido=0,
            atipico=0,
            fecha_siniestro=pl.col("fecha_registro"),
        )
    elif file == "primas":
        dif = dif.with_columns(prima_devengada_mod=0)

    df_cuadre = concat_df_dif(df, dif.select(df.collect_schema().names())).collect()

    df_cuadre.write_csv(f"data/raw/{file}.csv", separator="\t")
    df_cuadre.write_parquet(f"data/raw/{file}.parquet")

    return df_cuadre.lazy()

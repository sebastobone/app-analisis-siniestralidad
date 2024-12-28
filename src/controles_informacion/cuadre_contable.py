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
            """)
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


def cuadre_contable_soat(
    file: str, df: pl.LazyFrame, dif_sap_vs_tera: pl.LazyFrame
) -> pl.LazyFrame:
    ramos = df.select(["codigo_op", "codigo_ramo_op", "ramo_desc"]).unique()

    dif = (
        dif_sap_vs_tera.filter(pl.col("mes_mov") == pl.col("mes_mov").max())
        .rename(
            {
                "DIFERENCIA_PRIMAS_EMI_BRUTO": "prima_bruta",
                "DIFERENCIA_PRIMAS_EMI_RETENIDO": "prima_retenida",
                "DIFERENCIA_PRIMAS_DEV_BRUTO": "prima_bruta_devengada",
                "DIFERENCIA_PRIMAS_DEV_RETENIDO": "prima_retenida_devengada",
                "DIFERENCIA_PAGOS_BRUTO": "pago_bruto",
                "DIFERENCIA_PAGOS_RETENIDO": "pago_retenido",
                "DIFERENCIA_RSA_BRUTO": "aviso_bruto",
                "DIFERENCIA_RSA_RETENIDO": "aviso_retenido",
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

    # Modificable, aca se decide en que apertura vamos a meter la diferencia SAP-Tera
    apertura_dif = pl.LazyFrame(
        {
            "codigo_op": ["01"],
            "codigo_ramo_op": ["041"],
            "ramo_desc": ["AUTOS OBLIGATORIO"],
            "apertura_canal_desc": ["DIGITAL"],
            "apertura_amparo_desc": ["Total"],
            "tipo_vehiculo": ["MOTO"],
        }
    ).with_columns(utils.col_apertura_reservas("soat"))

    dif = dif.join(apertura_dif, on=["codigo_op", "codigo_ramo_op"])

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

    df_cuadre = concat_df_dif(df, dif).collect()

    df_cuadre.write_csv(f"data/raw/{file}.csv", separator="\t")
    df_cuadre.write_parquet(f"data/raw/{file}.parquet")

    return df_cuadre.lazy()

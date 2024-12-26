import polars as pl
from src import utils


def cuadre_contable(
    negocio: str, file: str, df: pl.DataFrame, dif_sap_vs_tera: pl.DataFrame
) -> pl.LazyFrame:
    if negocio == "soat":
        return cuadre_contable_soat(file, df, dif_sap_vs_tera)
    elif negocio == "autonomia":
        return cuadre_contable_autonomia(file, df, dif_sap_vs_tera)
    return pl.LazyFrame()


def concat_df_dif(df: pl.DataFrame, dif: pl.DataFrame) -> pl.DataFrame:
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
    file: str, df: pl.DataFrame, dif_sap_vs_tera: pl.DataFrame
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
        .with_columns(
            fecha_registro=pl.date(
                pl.col("mes_mov") // pl.lit(100), pl.col("mes_mov") % pl.lit(100), 1
            )
        )
        .join(pl.DataFrame(agrups), on=["codigo_op", "codigo_ramo_op"])
        .with_columns(
            conteo_pago=0,
            conteo_incurrido=0,
            conteo_desistido=0,
            atipico=0,
            fecha_siniestro=pl.col("fecha_registro"),
        )
        .select(df.collect_schema().names())
    )

    df_cuadre = concat_df_dif(df, dif)

    df_cuadre.write_csv(f"data/raw/{file}.csv", separator="\t")
    df_cuadre.write_parquet(f"data/raw/{file}.parquet")

    return df_cuadre.lazy()


def cuadre_contable_soat(
    file: str, df: pl.DataFrame, dif_sap_vs_tera: pl.DataFrame
) -> pl.LazyFrame:
    ramos = df.select(["codigo_op", "codigo_ramo_op", "ramo_desc"]).unique()

    dif = (
        dif_sap_vs_tera.filter(pl.col("mes_mov") == pl.col("mes_mov").max())
        .rename(
            {
                "DIFERENCIA_PRIMAS_EMI_BRUTO": "Prima_Bruta",
                "DIFERENCIA_PRIMAS_EMI_RETENIDO": "Prima_Retenida",
                "DIFERENCIA_PRIMAS_DEV_BRUTO": "Prima_Bruta_Devengada",
                "DIFERENCIA_PRIMAS_DEV_RETENIDO": "Prima_Retenida_Devengada",
                "DIFERENCIA_PAGOS_BRUTO": "Pago_Bruto",
                "DIFERENCIA_PAGOS_RETENIDO": "Pago_Retenido",
                "DIFERENCIA_RSA_BRUTO": "Aviso_Bruto",
                "DIFERENCIA_RSA_RETENIDO": "Aviso_Retenido",
            }
        )
        .join(ramos, on=["codigo_op", "codigo_ramo_op"])
        .with_columns(
            fecha_registro=pl.date(pl.col("mes_mov") // 100, pl.col("mes_mov") % 100, 1)
        )
    )

    dif = dif.drop(
        [
            column
            for column in dif.collect_schema().names()
            if column not in df.collect_schema().names()
        ]
    )

    # Modificable, aca se decide en que apertura vamos a meter la diferencia SAP-Tera
    apertura_dif = pl.DataFrame(
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

    df_cuadre = concat_df_dif(df, dif)

    df_cuadre.write_csv(f"data/raw/{file}.csv", separator="\t")
    df_cuadre.write_parquet(f"data/raw/{file}.parquet")

    return df_cuadre.lazy()

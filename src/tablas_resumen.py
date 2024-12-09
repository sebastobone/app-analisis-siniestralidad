import polars as pl


def tablas_resumen(
    path_plantilla: str, periodicidades: list[list[str]], tipo_analisis: str
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    diagonales = pl.scan_parquet(
        f"{path_plantilla}/../data/processed/base_triangulos.parquet"
    )
    atipicos = pl.scan_parquet(
        f"{path_plantilla}/../data/processed/base_atipicos.parquet"
    )
    aperts = pl.scan_csv(
        f"{path_plantilla}/../data/processed/aperturas.csv", separator="\t"
    )
    expuestos = pl.scan_parquet(f"{path_plantilla}/../data/processed/expuestos.parquet")
    primas = pl.scan_parquet(f"{path_plantilla}/../data/processed/primas.parquet")

    BASE_COLS = aperts.collect_schema().names()[1:]

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
                "pago_bruto",
                "incurrido_bruto",
                "pago_retenido",
                "incurrido_retenido",
                "conteo_pago",
                "conteo_incurrido",
                "conteo_desistido",
            ]
        )
    )

    if tipo_analisis == "Entremes":
        ult_ocurr = (
            pl.scan_parquet(
                f"{path_plantilla}/../data/processed/base_ultima_ocurrencia.parquet"
            )
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
        diagonales.join(aperts, on="apertura_reservas")
        .select(
            [
                "apertura_reservas",
            ]
            + aperts.collect_schema().names()[1:]
            + [
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "pago_bruto",
                "incurrido_bruto",
                "pago_retenido",
                "incurrido_retenido",
                "conteo_pago",
                "conteo_incurrido",
                "conteo_desistido",
            ]
        )
        .join(
            expuestos.drop("vigentes"),
            on=BASE_COLS + ["periodicidad_ocurrencia", "periodo_ocurrencia"],
            how="left",
        )
        .join(
            primas,
            on=BASE_COLS + ["periodicidad_ocurrencia", "periodo_ocurrencia"],
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
        atipicos.join(aperts, on="apertura_reservas")
        .select(
            [
                "apertura_reservas",
            ]
            + aperts.collect_schema().names()[1:]
            + [
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "pago_bruto",
                "incurrido_bruto",
                "pago_retenido",
                "incurrido_retenido",
                "conteo_pago",
                "conteo_incurrido",
                "conteo_desistido",
            ]
        )
        .join(
            expuestos.drop("vigentes"),
            on=BASE_COLS + ["periodicidad_ocurrencia", "periodo_ocurrencia"],
            how="left",
        )
        .join(
            primas,
            on=BASE_COLS + ["periodicidad_ocurrencia", "periodo_ocurrencia"],
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

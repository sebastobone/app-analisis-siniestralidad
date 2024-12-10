import polars as pl
from datetime import date
from math import floor, ceil
import constantes as ct


def col_ramo_desc() -> pl.Expr:
    return pl.concat_str(
        pl.col("codigo_op"),
        pl.col("codigo_ramo_op"),
        pl.col("ramo_desc"),
        separator=" - ",
    )


def aperturas() -> None:
    return (
        pl.scan_parquet("data/raw/siniestros.parquet")
        .with_columns(ramo_desc=col_ramo_desc())
        .filter(pl.col("fecha_registro") >= pl.col("fecha_siniestro"))
        .select(["apertura_reservas"] + ct.BASE_COLS)
        .unique()
        .sort("apertura_reservas")
        .collect()
        .write_csv("data/processed/aperturas.csv", separator="\t")
    )


def sinis_prep():
    df_sinis = (
        pl.scan_parquet(
            "data/raw/siniestros.parquet",
        )
        .with_columns(ramo_desc=col_ramo_desc())
        .filter(pl.col("fecha_registro") >= pl.col("fecha_siniestro"))
    )

    df_sinis = df_sinis.with_columns(
        [
            (pl.col(f"pago_{attr}") + pl.col(f"aviso_{attr}")).alias(
                f"incurrido_{attr}"
            )
            for attr in ["bruto", "retenido"]
        ],
        conteo_incurrido=pl.col("conteo_incurrido") - pl.col("conteo_desistido"),
    ).select(
        [
            "apertura_reservas",
            "atipico",
            "fecha_siniestro",
            "fecha_registro",
            "pago_bruto",
            "pago_retenido",
            "incurrido_bruto",
            "incurrido_retenido",
            "conteo_pago",
            "conteo_incurrido",
            "conteo_desistido",
        ]
    )

    df_sinis_tipicos = df_sinis.filter(pl.col("atipico") == 0).drop("atipico")
    df_sinis_atipicos = df_sinis.filter(pl.col("atipico") == 1).drop("atipico")

    bases_fechas = []
    for tipo_fecha in ["fecha_siniestro", "fecha_registro"]:
        bases_fechas.append(
            pl.LazyFrame(
                pl.date_range(
                    ct.INI_DATE,
                    ct.END_DATE,
                    interval="1mo",
                    eager=True,
                ).alias(tipo_fecha)
            )
        )

    base = (
        df_sinis_tipicos.select("apertura_reservas")
        .unique()
        .join(bases_fechas[0], how="cross")
        .join(bases_fechas[1], how="cross")
        .filter(pl.col("fecha_siniestro") <= pl.col("fecha_registro"))
    )

    df_sinis_tipicos = df_sinis_tipicos.join(
        base,
        on=["apertura_reservas", "fecha_siniestro", "fecha_registro"],
        how="full",
        coalesce=True,
    ).fill_null(0)

    return df_sinis_tipicos, df_sinis_atipicos


def date_to_yyyymm(column: pl.Expr, grain: str) -> tuple[pl.Expr]:
    return (
        (
            column.dt.year() * 100
            + (column.dt.month() / pl.lit(ct.PERIODICIDADES[grain])).ceil()
            * pl.lit(ct.PERIODICIDADES[grain])
        ).cast(pl.Int32),
    )


def triangulos(
    df_tri: pl.LazyFrame,
    origin_grain: str,
    development_grain: str,
) -> pl.LazyFrame:
    anno = ct.END_DATE.year
    mes = ct.END_DATE.month
    periodicidad = ct.PERIODICIDADES[origin_grain]

    mes_ult_ocurr_triangulos = (
        date(anno - 1, 12, 1)
        if mes < periodicidad
        else date(anno, floor(mes / periodicidad) * periodicidad, 1)
    )

    df_tri = (
        df_tri.filter(
            pl.col("fecha_registro")
            <= (
                ct.END_DATE
                if ct.TIPO_ANALISIS == "Entremes"
                else mes_ult_ocurr_triangulos
            )
        )
        .with_columns(
            periodo_ocurrencia=date_to_yyyymm(pl.col("fecha_siniestro"), origin_grain)[
                0
            ],
            periodo_desarrollo=date_to_yyyymm(
                pl.col("fecha_registro"), development_grain
            )[0],
        )
        .drop(["fecha_siniestro", "fecha_registro"])
        .group_by(
            [
                "apertura_reservas",
                "periodo_ocurrencia",
                "periodo_desarrollo",
            ]
        )
        .sum()
        .sort(["apertura_reservas", "periodo_ocurrencia", "periodo_desarrollo"])
        .with_columns(
            [
                pl.col(qty_column)
                .cum_sum()
                .over(["apertura_reservas", "periodo_ocurrencia"])
                for qty_column in [
                    "pago_bruto",
                    "pago_retenido",
                    "incurrido_bruto",
                    "incurrido_retenido",
                    "conteo_pago",
                    "conteo_incurrido",
                    "conteo_desistido",
                ]
            ],
            index_desarrollo=pl.col("periodo_desarrollo")
            .cum_count()
            .over(["apertura_reservas", "periodo_ocurrencia"]),
            periodicidad_ocurrencia=pl.lit(origin_grain),
            periodicidad_desarrollo=pl.lit(development_grain),
        )
        .with_columns(
            diagonal=pl.when(
                pl.col("index_desarrollo")
                == pl.col("index_desarrollo")
                .max()
                .over(
                    [
                        "apertura_reservas",
                        "periodo_ocurrencia",
                        "periodicidad_desarrollo",
                    ]
                )
            )
            .then(1)
            .otherwise(0)
        )
        .select(
            [
                "apertura_reservas",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "periodicidad_desarrollo",
                "periodo_desarrollo",
                "diagonal",
                "index_desarrollo",
                "pago_bruto",
                "pago_retenido",
                "incurrido_bruto",
                "incurrido_retenido",
                "conteo_pago",
                "conteo_incurrido",
                "conteo_desistido",
            ]
        )
    )

    return df_tri


def diagonales(
    df_tri: pl.LazyFrame, origin_grain: str, ultima_ocurr: bool = False
) -> pl.LazyFrame:
    anno = ct.END_DATE.year
    mes = ct.END_DATE.month
    periodicidad = ct.PERIODICIDADES[origin_grain]

    mes_prim_ocurr_periodo_act = date(
        anno, ceil(mes / periodicidad) * periodicidad - (periodicidad - 1), 1
    )

    df_diagonales = (
        df_tri.filter(
            pl.col("fecha_siniestro")
            >= (mes_prim_ocurr_periodo_act if ultima_ocurr else ct.INI_DATE)
        )
        .with_columns(
            periodo_ocurrencia=date_to_yyyymm(pl.col("fecha_siniestro"), "Mensual")[0],
            periodicidad_triangulo=pl.lit(origin_grain),
            periodicidad_ocurrencia=pl.lit("Mensual"),
        )
        .select(
            [
                "apertura_reservas",
                "periodicidad_triangulo",
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
        .group_by(
            [
                "apertura_reservas",
                "periodicidad_triangulo",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
        )
        .sum()
        .sort(
            [
                "apertura_reservas",
                "periodicidad_triangulo",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
        )
    )

    if not ultima_ocurr:
        df_diagonales = df_diagonales.drop("periodicidad_triangulo")

    return df_diagonales


def bases_siniestros() -> None:
    df_sinis_tipicos, df_sinis_atipicos = sinis_prep()

    if ct.TIPO_ANALISIS == "Triangulos":
        base_triangulos = pl.concat(
            [
                triangulos(df_sinis_tipicos, "Mensual", "Mensual"),
                triangulos(df_sinis_tipicos, "Trimestral", "Trimestral"),
                triangulos(df_sinis_tipicos, "Semestral", "Semestral"),
                triangulos(df_sinis_tipicos, "Anual", "Anual"),
            ]
        )
    else:
        base_triangulos = pl.concat(
            [
                triangulos(df_sinis_tipicos, "Trimestral", "Mensual"),
                triangulos(df_sinis_tipicos, "Semestral", "Mensual"),
                triangulos(df_sinis_tipicos, "Anual", "Mensual"),
            ]
        )
        base_ult_ocurr = pl.concat(
            [
                diagonales(df_sinis_tipicos, "Trimestral", True),
                diagonales(df_sinis_tipicos, "Semestral", True),
                diagonales(df_sinis_tipicos, "Anual", True),
            ]
        )
        base_ult_ocurr.collect().write_parquet(
            "data/processed/base_ultima_ocurrencia.parquet"
        )

    base_triangulos.collect().write_parquet("data/processed/base_triangulos.parquet")
    diagonales(df_sinis_atipicos, "Mensual").collect().write_parquet(
        "data/processed/base_atipicos.parquet"
    )


def bases_primas_expuestos(qty: str, qty_cols: list[str]) -> None:
    def fechas_pdn(col: pl.Expr) -> tuple[pl.Expr, pl.Expr, pl.Expr, pl.Expr]:
        return (
            (col.dt.year() * 100 + col.dt.month()).alias("Mensual"),
            (col.dt.year() * 100 + (col.dt.month() / 3).ceil() * 3).alias("Trimestral"),
            (col.dt.year() * 100 + (col.dt.month() / 6).ceil() * 6).alias("Semestral"),
            col.dt.year().alias("Anual"),
        )

    df_group = (
        pl.scan_csv(
            f"data/raw/{qty}.csv",
            separator="\t",
            try_parse_dates=True,
            schema_overrides={"codigo_op": pl.String, "codigo_ramo_op": pl.String},
        )
        .with_columns(
            fechas_pdn(pl.col("fecha_registro")),
            ramo_desc=col_ramo_desc(),
        )
        .drop(["apertura_reservas", "codigo_op", "codigo_ramo_op", "fecha_registro"])
        .unpivot(
            index=ct.BASE_COLS + qty_cols,
            variable_name="periodicidad_ocurrencia",
            value_name="periodo_ocurrencia",
        )
        .with_columns(pl.col("periodo_ocurrencia").cast(pl.Int32))
        .group_by(
            ct.BASE_COLS
            + [
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
        )
    )

    if qty == "primas":
        df = df_group.sum()
    elif qty == "expuestos":
        df = df_group.mean()

    return (
        df.sort(
            ct.BASE_COLS
            + [
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
        )
        .collect()
        .write_parquet(f"data/processed/{qty}.parquet")
    )

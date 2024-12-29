import pandas as pd
import polars as pl

from src.metodos_plantilla import insumos as ins


def base_plantillas(
    path_plantilla: str,
    apertura: str,
    atributo: str,
    periodicidades: list[list[str]],
    cantidades: list[str],
) -> pd.DataFrame:
    return (
        ins.df_diagonales(path_plantilla)
        .filter(pl.col("apertura_reservas") == apertura)
        .join(
            pl.LazyFrame(
                periodicidades,
                schema=["apertura_reservas", "periodicidad_ocurrencia"],
                orient="row",
            ),
            on=["apertura_reservas", "periodicidad_ocurrencia"],
        )
        .drop(["diagonal", "periodo_desarrollo"])
        .unpivot(
            index=[
                "apertura_reservas",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "periodicidad_desarrollo",
                "index_desarrollo",
            ],
            variable_name="cantidad",
            value_name="valor",
        )
        .with_columns(
            atributo=pl.when(pl.col("cantidad").str.contains("retenido"))
            .then(pl.lit("retenido"))
            .otherwise(pl.lit("bruto")),
            cantidad=pl.col("cantidad").str.replace_many(
                {"_bruto": "", "_retenido": ""}
            ),
        )
        .filter((pl.col("atributo") == atributo) & pl.col("cantidad").is_in(cantidades))
        .collect()
        .to_pandas()
        .pivot(
            index="periodo_ocurrencia",
            columns=["cantidad", "index_desarrollo"],
            values="valor",
        )
    )

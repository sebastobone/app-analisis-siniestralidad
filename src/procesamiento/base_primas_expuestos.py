import polars as pl
import src.constantes as ct
from src import utils


def bases_primas_expuestos(qty: str, negocio: str) -> None:
    def fechas_pdn(col: pl.Expr) -> tuple[pl.Expr, pl.Expr, pl.Expr, pl.Expr]:
        return (
            (col.dt.year() * 100 + col.dt.month()).alias("Mensual"),
            (col.dt.year() * 100 + (col.dt.month() / 3).ceil() * 3).alias("Trimestral"),
            (col.dt.year() * 100 + (col.dt.month() / 6).ceil() * 6).alias("Semestral"),
            col.dt.year().alias("Anual"),
        )

    qty_cols = (
        [
            "prima_bruta",
            "prima_bruta_devengada",
            "prima_retenida",
            "prima_retenida_devengada",
        ]
        if qty == "primas"
        else ["expuestos", "vigentes"]
    )

    df_group = (
        pl.scan_parquet(f"data/raw/{qty}.parquet")
        .with_columns(
            fechas_pdn(pl.col("fecha_registro")),
            ramo_desc=utils.col_ramo_desc(),
        )
        .drop(["apertura_reservas", "codigo_op", "codigo_ramo_op", "fecha_registro"])
        .unpivot(
            index=["ramo_desc"] + ct.columnas_aperturas(negocio)[2:] + qty_cols,
            variable_name="periodicidad_ocurrencia",
            value_name="periodo_ocurrencia",
        )
        .with_columns(pl.col("periodo_ocurrencia").cast(pl.Int32))
        .group_by(
            ["ramo_desc"]
            + ct.columnas_aperturas(negocio)[2:]
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
            ["ramo_desc"]
            + ct.columnas_aperturas(negocio)[2:]
            + [
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
        )
        .collect()
        .write_parquet(f"data/processed/{qty}.parquet")
    )

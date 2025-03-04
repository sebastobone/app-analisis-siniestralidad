from datetime import date

import polars as pl
import pytest
from src import utils


@pytest.fixture(autouse=True)
def guardar_bases_ficticias(
    mock_siniestros: pl.LazyFrame,
    mock_primas: pl.LazyFrame,
    mock_expuestos: pl.LazyFrame,
) -> None:
    mock_siniestros.collect().write_parquet("data/raw/siniestros.parquet")
    mock_primas.collect().write_parquet("data/raw/primas.parquet")
    mock_expuestos.collect().write_parquet("data/raw/expuestos.parquet")


def agregar_meses_params(params_form: dict[str, str], rango_meses: tuple[date, date]):
    params_form.update(
        {
            "mes_inicio": str(utils.date_to_yyyymm(rango_meses[0])),
            "mes_corte": str(utils.date_to_yyyymm(rango_meses[1])),
        }
    )

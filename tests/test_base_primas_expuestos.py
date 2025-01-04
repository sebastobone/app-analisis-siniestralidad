import polars as pl
import pytest
from src.procesamiento import base_primas_expuestos as base


@pytest.mark.unit
@pytest.mark.parametrize(
    "periodicidad_ocurrencia", ["Mensual", "Trimestral", "Mensual", "Anual"]
)
def test_primas(periodicidad_ocurrencia: str, mock_primas: pl.LazyFrame):
    processed_data = (
        base.generar_base_primas_expuestos(mock_primas, "primas", "mock")
        .filter(pl.col("periodicidad_ocurrencia") == periodicidad_ocurrencia)
        .get_column("prima_bruta")
        .sum()
    )
    original_data = mock_primas.collect().get_column("prima_bruta").sum()
    assert abs(processed_data - original_data) < 100


@pytest.mark.unit
def test_expuestos(mock_expuestos: pl.LazyFrame):
    processed_data = (
        base.generar_base_primas_expuestos(mock_expuestos, "expuestos", "mock")
        .filter(pl.col("periodicidad_ocurrencia") == "Mensual")
        .get_column("expuestos")
        .sum()
    )
    original_data = mock_expuestos.collect().get_column("expuestos").sum()
    assert abs(processed_data - original_data) < 100

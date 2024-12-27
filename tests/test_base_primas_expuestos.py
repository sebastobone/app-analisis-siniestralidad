import polars as pl
from src.procesamiento import base_primas_expuestos as base


def test_primas(mock_primas: pl.LazyFrame):
    processed_data = (
        base.bases_primas_expuestos(mock_primas, "primas", "mock")
        .filter(pl.col("periodicidad_ocurrencia") == "Mensual")
        .get_column("prima_bruta")
        .sum()
    )
    original_data = mock_primas.collect().get_column("prima_bruta").sum()
    assert abs(processed_data - original_data) < 100


def test_expuestos(mock_expuestos: pl.LazyFrame):
    processed_data = (
        base.bases_primas_expuestos(mock_expuestos, "expuestos", "mock")
        .filter(pl.col("periodicidad_ocurrencia") == "Mensual")
        .get_column("expuestos")
        .sum()
    )
    original_data = mock_expuestos.collect().get_column("expuestos").sum()
    assert abs(processed_data - original_data) < 100

import polars as pl
import pytest
from src.procesamiento import base_primas_expuestos as base

from tests.conftest import assert_igual


@pytest.mark.unit
@pytest.mark.parametrize(
    "periodicidad_ocurrencia", ["Mensual", "Trimestral", "Mensual", "Anual"]
)
def test_generar_base_primas(periodicidad_ocurrencia: str, mock_primas: pl.LazyFrame):
    data_procesada = base.generar_base_primas_expuestos(
        mock_primas, "primas", "mock"
    ).filter(pl.col("periodicidad_ocurrencia") == periodicidad_ocurrencia)
    data_original = mock_primas.collect()
    assert_igual(data_procesada, data_original, "prima_bruta")


@pytest.mark.unit
def test_generar_base_expuestos(mock_expuestos: pl.LazyFrame):
    data_procesada = base.generar_base_primas_expuestos(
        mock_expuestos, "expuestos", "mock"
    ).filter(pl.col("periodicidad_ocurrencia") == "Mensual")
    data_original = mock_expuestos.collect()
    assert_igual(data_procesada, data_original, "expuestos")

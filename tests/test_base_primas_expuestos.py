from datetime import date

import polars as pl
import pytest
from src import utils
from src.procesamiento import base_primas_expuestos as base

from tests.conftest import assert_igual


@pytest.mark.unit
@pytest.mark.parametrize(
    "periodicidad_ocurrencia", ["Mensual", "Trimestral", "Mensual", "Anual"]
)
def test_generar_base_primas(
    periodicidad_ocurrencia: str, rango_meses: tuple[date, date]
):
    data_original = utils.generar_mock_primas(rango_meses)

    data_procesada = base.generar_base_primas_expuestos(
        data_original.lazy(), "primas", "demo"
    ).filter(pl.col("periodicidad_ocurrencia") == periodicidad_ocurrencia)
    assert_igual(data_procesada, data_original, "prima_bruta")


@pytest.mark.unit
def test_generar_base_expuestos(rango_meses: tuple[date, date]):
    data_original = utils.generar_mock_expuestos(rango_meses)
    data_procesada = base.generar_base_primas_expuestos(
        data_original.lazy(), "expuestos", "demo"
    ).filter(pl.col("periodicidad_ocurrencia") == "Mensual")
    assert_igual(data_procesada, data_original, "expuestos")

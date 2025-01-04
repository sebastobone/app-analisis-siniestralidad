from datetime import date

import polars as pl
import pytest
from numpy.random import randint
from src.procesamiento import base_primas_expuestos as base

from tests.conftest import mock_expuestos, mock_primas


@pytest.mark.unit
@pytest.mark.parametrize(
    "periodicidad_ocurrencia", ["Mensual", "Trimestral", "Mensual", "Anual"]
)
def test_primas(periodicidad_ocurrencia: str):
    mes_inicio = date(randint(2010, 2019), randint(1, 12), 1)
    mes_corte = date(randint(2020, 2030), randint(1, 12), 1)
    df_primas = mock_primas(mes_inicio, mes_corte)
    processed_data = (
        base.generar_base_primas_expuestos(df_primas, "primas", "mock")
        .filter(pl.col("periodicidad_ocurrencia") == periodicidad_ocurrencia)
        .get_column("prima_bruta")
        .sum()
    )
    original_data = df_primas.collect().get_column("prima_bruta").sum()
    assert abs(processed_data - original_data) < 100


@pytest.mark.unit
def test_expuestos():
    mes_inicio = date(randint(2010, 2019), randint(1, 12), 1)
    mes_corte = date(randint(2020, 2030), randint(1, 12), 1)
    df_expuestos = mock_expuestos(mes_inicio, mes_corte)
    processed_data = (
        base.generar_base_primas_expuestos(df_expuestos, "expuestos", "mock")
        .filter(pl.col("periodicidad_ocurrencia") == "Mensual")
        .get_column("expuestos")
        .sum()
    )
    original_data = df_expuestos.collect().get_column("expuestos").sum()
    assert abs(processed_data - original_data) < 100

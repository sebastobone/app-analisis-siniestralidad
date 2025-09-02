from datetime import date

import polars as pl
import pytest
from src import utils
from src.informacion.mocks import generar_mock
from src.procesamiento import base_primas_expuestos as base

from tests.conftest import assert_igual


@pytest.mark.unit
@pytest.mark.parametrize(
    "periodicidad_ocurrencia", ["Mensual", "Trimestral", "Mensual", "Anual"]
)
def test_generar_base_primas(
    periodicidad_ocurrencia: str, rango_meses: tuple[date, date]
):
    mes_inicio_int = utils.date_to_yyyymm(rango_meses[0])
    mes_corte_int = utils.date_to_yyyymm(rango_meses[1])
    data_original = generar_mock(mes_inicio_int, mes_corte_int, "primas")

    data_procesada = base.generar_base_primas_expuestos(
        data_original.lazy(), "primas", "demo"
    ).filter(pl.col("periodicidad_ocurrencia") == periodicidad_ocurrencia)
    assert_igual(data_procesada, data_original, "prima_bruta")


@pytest.mark.unit
def test_generar_base_expuestos(rango_meses: tuple[date, date]):
    mes_inicio_int = utils.date_to_yyyymm(rango_meses[0])
    mes_corte_int = utils.date_to_yyyymm(rango_meses[1])
    data_original = generar_mock(mes_inicio_int, mes_corte_int, "expuestos")
    data_procesada = base.generar_base_primas_expuestos(
        data_original.lazy(), "expuestos", "demo"
    ).filter(pl.col("periodicidad_ocurrencia") == "Mensual")
    assert_igual(data_procesada, data_original, "expuestos")

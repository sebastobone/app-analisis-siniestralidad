from datetime import date

import polars as pl
import pytest
from src import utils


@pytest.mark.unit
@pytest.mark.parametrize(
    "mes_corte, num_ocurrencias, num_alturas, resultado_esperado",
    [
        # Mensuales
        (date(2022, 1, 1), 10, 10, 1),
        (date(2022, 6, 1), 10, 10, 1),
        (date(2023, 8, 1), 10, 10, 1),
        # Trimestrales
        (date(2022, 10, 1), 10, 30, 1),
        (date(2022, 11, 1), 10, 30, 2),
        (date(2022, 12, 1), 10, 30, 3),
        (date(2023, 1, 1), 10, 30, 1),
        (date(2023, 2, 1), 10, 30, 2),
        (date(2023, 3, 1), 10, 30, 3),
        # Semestrales
        (date(2022, 7, 1), 10, 60, 1),
        (date(2022, 10, 1), 10, 60, 4),
        (date(2022, 12, 1), 10, 60, 6),
        (date(2023, 1, 1), 10, 60, 1),
        (date(2023, 4, 1), 10, 60, 4),
        (date(2023, 6, 1), 10, 60, 6),
        # Anuales
        (date(2022, 1, 1), 10, 120, 1),
        (date(2022, 6, 1), 10, 120, 6),
        (date(2022, 12, 1), 10, 120, 12),
    ],
)
def test_mes_del_periodo(
    mes_corte: date, num_ocurrencias: int, num_alturas: int, resultado_esperado: int
):
    assert (
        utils.mes_del_periodo(mes_corte, num_ocurrencias, num_alturas)
        == resultado_esperado
    )


@pytest.mark.unit
def test_generalizar_tipos_columnas_resultados():
    df = pl.DataFrame(
        {
            "apertura_reservas": [1, 2],
            "mes_corte": [1, 2],
            "atipico": [1, 2],
            "tipo_analisis": [1, 2],
        }
    )

    df = utils.generalizar_tipos_columnas_resultados(df)

    assert df.schema == {
        "apertura_reservas": pl.String,
        "mes_corte": pl.Float64,
        "atipico": pl.Float64,
        "tipo_analisis": pl.String,
    }

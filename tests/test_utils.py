from datetime import date

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

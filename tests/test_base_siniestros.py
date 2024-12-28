from datetime import date
from typing import Literal
from unittest.mock import patch

import polars as pl
import pytest
from src import utils
from src.procesamiento import base_siniestros as base


def mes_inicio_mock(mock_siniestros: pl.LazyFrame) -> date:
    return mock_siniestros.collect().get_column("fecha_siniestro").min()  # type: ignore


def plata_original(
    mock_siniestros: pl.LazyFrame,
    mes_inferior: date,
    mes_superior: date,
    atipicos: Literal[1, 0],
) -> float:
    return (
        mock_siniestros.with_columns(
            pl.col("fecha_siniestro").clip(upper_bound=pl.col("fecha_registro"))
        )
        .filter(
            (pl.col("fecha_siniestro").is_between(mes_inferior, mes_superior))
            & (pl.col("fecha_registro").is_between(mes_inferior, mes_superior))
            & (pl.col("atipico") == atipicos)
        )
        .collect()
        .get_column("pago_bruto")
        .sum()
    )


@pytest.mark.parametrize(
    "mes_corte, periodicidad_ocurrencia, expected_ult_ocurr",
    [
        (date(2024, 7, 1), "Mensual", date(2024, 7, 1)),
        (date(2024, 6, 1), "Trimestral", date(2024, 6, 1)),
        (date(2024, 8, 1), "Trimestral", date(2024, 6, 1)),
        (date(2024, 9, 1), "Semestral", date(2024, 6, 1)),
        (date(2024, 9, 1), "Anual", date(2023, 12, 1)),
    ],
)
def test_analisis_triangulos(
    mock_siniestros: pl.LazyFrame,
    mes_corte: date,
    periodicidad_ocurrencia: str,
    expected_ult_ocurr: date,
):
    mes_inicio = mes_inicio_mock(mock_siniestros)
    with patch("src.procesamiento.base_siniestros.guardar_archivos"):
        base_triangulos, _, base_atipicos = base.generar_bases_siniestros(
            mock_siniestros,
            "triangulos",
            mes_inicio,
            mes_corte,
        )

    base_triangulos = base_triangulos.filter(
        pl.col("periodicidad_ocurrencia") == periodicidad_ocurrencia
    )

    mes_inicio_tipicos = utils.date_to_yyyymm(mes_inicio, periodicidad_ocurrencia)

    assert base_triangulos.get_column("periodo_ocurrencia").min() == mes_inicio_tipicos
    assert base_triangulos.get_column(
        "periodo_ocurrencia"
    ).max() == utils.date_to_yyyymm(expected_ult_ocurr)
    assert base_triangulos.get_column("periodo_desarrollo").min() == mes_inicio_tipicos
    assert base_triangulos.get_column(
        "periodo_desarrollo"
    ).max() == utils.date_to_yyyymm(expected_ult_ocurr)

    plata_original_tipicos = plata_original(
        mock_siniestros, mes_inicio, expected_ult_ocurr, 0
    )
    plata_procesada_tipicos = (
        base_triangulos.filter(pl.col("diagonal") == 1).get_column("pago_bruto").sum()
    )

    assert abs(plata_original_tipicos - plata_procesada_tipicos) < 100

    mes_inicio_atipicos = utils.date_to_yyyymm(mes_inicio)

    assert base_atipicos.get_column("periodo_ocurrencia").min() == mes_inicio_atipicos
    assert base_atipicos.get_column("periodo_ocurrencia").max() == utils.date_to_yyyymm(
        mes_corte
    )


@pytest.mark.parametrize(
    """mes_corte, periodicidad_ocurrencia, expected_ult_ocurr_triangulo,
    expected_prim_ocurr_entremes""",
    [
        (date(2024, 6, 1), "Trimestral", date(2024, 6, 1), date(2024, 4, 1)),
        (date(2024, 8, 1), "Trimestral", date(2024, 9, 1), date(2024, 7, 1)),
        (date(2024, 9, 1), "Semestral", date(2024, 12, 1), date(2024, 7, 1)),
        (date(2024, 9, 1), "Anual", date(2024, 12, 1), date(2024, 1, 1)),
    ],
)
def test_analisis_entremes(
    mock_siniestros: pl.LazyFrame,
    mes_corte: date,
    periodicidad_ocurrencia: str,
    expected_ult_ocurr_triangulo: date,
    expected_prim_ocurr_entremes: date,
):
    mes_inicio = mes_inicio_mock(mock_siniestros)

    with patch("src.procesamiento.base_siniestros.guardar_archivos"):
        base_triangulos, base_ult_ocurr, base_atipicos = base.generar_bases_siniestros(
            mock_siniestros, "entremes", mes_inicio, mes_corte
        )

    base_triangulos = base_triangulos.filter(
        pl.col("periodicidad_ocurrencia") == periodicidad_ocurrencia
    )

    mes_inicio_tipicos = utils.date_to_yyyymm(mes_inicio, periodicidad_ocurrencia)

    assert base_triangulos.get_column("periodo_ocurrencia").min() == mes_inicio_tipicos
    assert base_triangulos.get_column(
        "periodo_desarrollo"
    ).min() == utils.date_to_yyyymm(mes_inicio)

    assert base_triangulos.get_column(
        "periodo_ocurrencia"
    ).max() == utils.date_to_yyyymm(expected_ult_ocurr_triangulo)
    assert base_triangulos.get_column(
        "periodo_desarrollo"
    ).max() == utils.date_to_yyyymm(mes_corte)

    plata_original_tipicos = plata_original(mock_siniestros, mes_inicio, mes_corte, 0)

    plata_procesada = (
        base_triangulos.filter(pl.col("diagonal") == 1).get_column("pago_bruto").sum()
    )

    assert abs(plata_original_tipicos - plata_procesada) < 100

    base_ult_ocurr = base_ult_ocurr.filter(
        pl.col("periodicidad_triangulo") == periodicidad_ocurrencia
    )

    assert base_ult_ocurr.get_column(
        "periodo_ocurrencia"
    ).min() == utils.date_to_yyyymm(expected_prim_ocurr_entremes)
    assert base_ult_ocurr.get_column(
        "periodo_ocurrencia"
    ).max() == utils.date_to_yyyymm(mes_corte)

    plata_original_ult_ocurr = plata_original(
        mock_siniestros, expected_prim_ocurr_entremes, mes_corte, 0
    )
    plata_procesada = base_ult_ocurr.get_column("pago_bruto").sum()

    assert abs(plata_original_ult_ocurr - plata_procesada) < 100

    assert base_atipicos.get_column("periodo_ocurrencia").min() == utils.date_to_yyyymm(
        mes_inicio
    )
    assert base_atipicos.get_column("periodo_ocurrencia").max() == utils.date_to_yyyymm(
        mes_corte
    )

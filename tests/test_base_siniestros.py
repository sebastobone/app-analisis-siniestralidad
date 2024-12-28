from datetime import date
from unittest.mock import patch

import polars as pl
import pytest
from src import utils
from src.procesamiento import base_siniestros as base


def mes_inicio_mock(mock_siniestros: pl.LazyFrame) -> date:
    return mock_siniestros.collect().get_column("fecha_siniestro").min()  # type: ignore


@pytest.mark.parametrize(
    "mes_corte, periodicidad_ocurrencia, expected_ult_ocurr",
    [
        (202407, "Mensual", 202407),
        (202406, "Trimestral", 202406),
        (202408, "Trimestral", 202406),
        (202409, "Semestral", 202406),
        (202409, "Anual", 202312),
    ],
)
def test_analisis_triangulos(
    mock_siniestros: pl.LazyFrame,
    mes_corte: int,
    periodicidad_ocurrencia: str,
    expected_ult_ocurr: int,
):
    with patch("src.procesamiento.base_siniestros.guardar_archivos"):
        base_triangulos, _, base_atipicos = base.generar_bases_siniestros(
            mock_siniestros,
            "triangulos",
            utils.date_to_yyyymm(mes_inicio_mock(mock_siniestros)),
            mes_corte,
        )

    base_triangulos = base_triangulos.filter(
        pl.col("periodicidad_ocurrencia") == periodicidad_ocurrencia
    )

    mes_inicio_tipicos = utils.date_to_yyyymm(
        mes_inicio_mock(mock_siniestros), periodicidad_ocurrencia
    )

    assert base_triangulos.get_column("periodo_ocurrencia").min() == mes_inicio_tipicos
    assert base_triangulos.get_column("periodo_ocurrencia").max() == expected_ult_ocurr
    assert base_triangulos.get_column("periodo_desarrollo").min() == mes_inicio_tipicos
    assert base_triangulos.get_column("periodo_desarrollo").max() == expected_ult_ocurr

    plata_original_tipicos = (
        mock_siniestros.with_columns(
            pl.col("fecha_siniestro").clip(upper_bound=pl.col("fecha_registro"))
        )
        .filter(
            (pl.col("fecha_siniestro") <= utils.yyyymm_to_date(expected_ult_ocurr))
            & (pl.col("fecha_registro") <= utils.yyyymm_to_date(expected_ult_ocurr))
            & (pl.col("atipico") == 0)
        )
        .collect()
        .get_column("pago_bruto")
        .sum()
    )

    plata_procesada_tipicos = (
        base_triangulos.filter(pl.col("diagonal") == 1).get_column("pago_bruto").sum()
    )

    assert abs(plata_original_tipicos - plata_procesada_tipicos) < 100

    mes_inicio_atipicos = utils.date_to_yyyymm(
        mes_inicio_mock(mock_siniestros), "Mensual"
    )

    assert base_atipicos.get_column("periodo_ocurrencia").min() == mes_inicio_atipicos
    assert base_atipicos.get_column("periodo_ocurrencia").max() == mes_corte


@pytest.mark.parametrize(
    """mes_corte, periodicidad_ocurrencia, expected_ult_ocurr_triangulo,
    expected_prim_ocurr_entremes""",
    [
        (202406, "Trimestral", 202406, 202404),
        (202408, "Trimestral", 202409, 202407),
        (202409, "Semestral", 202412, 202407),
        (202409, "Anual", 202412, 202401),
    ],
)
def test_analisis_entremes(
    mock_siniestros: pl.LazyFrame,
    mes_corte: int,
    periodicidad_ocurrencia: str,
    expected_ult_ocurr_triangulo: int,
    expected_prim_ocurr_entremes: int,
):
    mes_inicio = utils.date_to_yyyymm(mes_inicio_mock(mock_siniestros))
    with patch("src.procesamiento.base_siniestros.guardar_archivos"):
        base_triangulos, base_ult_ocurr, base_atipicos = base.generar_bases_siniestros(
            mock_siniestros, "entremes", mes_inicio, mes_corte
        )

    base_triangulos = base_triangulos.filter(
        pl.col("periodicidad_ocurrencia") == periodicidad_ocurrencia
    )

    mes_inicio_tipicos = utils.date_to_yyyymm(
        mes_inicio_mock(mock_siniestros), periodicidad_ocurrencia
    )

    assert base_triangulos.get_column("periodo_ocurrencia").min() == mes_inicio_tipicos
    assert base_triangulos.get_column("periodo_desarrollo").min() == mes_inicio
    assert (
        base_triangulos.get_column("periodo_ocurrencia").max()
        == expected_ult_ocurr_triangulo
    )
    assert base_triangulos.get_column("periodo_desarrollo").max() == mes_corte

    plata_original = (
        mock_siniestros.with_columns(
            pl.col("fecha_siniestro").clip(upper_bound=pl.col("fecha_registro"))
        )
        .filter(
            (pl.col("fecha_siniestro") <= utils.yyyymm_to_date(mes_corte))
            & (pl.col("fecha_registro") <= utils.yyyymm_to_date(mes_corte))
            & (pl.col("atipico") == 0)
        )
        .collect()
        .get_column("pago_bruto")
        .sum()
    )

    plata_procesada = (
        base_triangulos.filter(pl.col("diagonal") == 1).get_column("pago_bruto").sum()
    )

    assert abs(plata_original - plata_procesada) < 100

    base_ult_ocurr = base_ult_ocurr.filter(
        pl.col("periodicidad_triangulo") == periodicidad_ocurrencia
    )

    assert (
        base_ult_ocurr.get_column("periodo_ocurrencia").min()
        == expected_prim_ocurr_entremes
    )
    assert base_ult_ocurr.get_column("periodo_ocurrencia").max() == mes_corte

    plata_original = (
        mock_siniestros.with_columns(
            pl.col("fecha_siniestro").clip(upper_bound=pl.col("fecha_registro"))
        )
        .filter(
            (
                pl.col("fecha_siniestro").is_between(
                    utils.yyyymm_to_date(expected_prim_ocurr_entremes),
                    utils.yyyymm_to_date(mes_corte),
                )
            )
            & (
                pl.col("fecha_registro").is_between(
                    utils.yyyymm_to_date(expected_prim_ocurr_entremes),
                    utils.yyyymm_to_date(mes_corte),
                )
            )
            & (pl.col("atipico") == 0)
        )
        .collect()
        .get_column("pago_bruto")
        .sum()
    )

    plata_procesada = base_ult_ocurr.get_column("pago_bruto").sum()

    assert abs(plata_original - plata_procesada) < 100

    assert base_atipicos.get_column("periodo_ocurrencia").min() == mes_inicio
    assert base_atipicos.get_column("periodo_ocurrencia").max() == mes_corte

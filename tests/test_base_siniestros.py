from datetime import date
from typing import Literal

import polars as pl
import pytest
from src import utils
from src.informacion.mocks import generar_mock
from src.procesamiento import base_siniestros as base

from tests.conftest import assert_igual


def filtrar_fechas(
    df_siniestros: pl.LazyFrame, rango_meses: tuple[date, date]
) -> pl.DataFrame:
    return (
        df_siniestros.with_columns(
            pl.col("fecha_siniestro").clip(upper_bound=pl.col("fecha_registro"))
        )
        .filter(
            (pl.col("fecha_siniestro").is_between(*rango_meses))
            & (pl.col("fecha_registro").is_between(*rango_meses))
        )
        .collect()
    )


@pytest.mark.fast
@pytest.mark.parametrize(
    "mes_corte, origin_grain, expected",
    [
        (date(2022, 1, 1), "Mensual", date(2022, 1, 1)),
        (date(2022, 9, 1), "Trimestral", date(2022, 9, 1)),
        (date(2022, 10, 1), "Trimestral", date(2022, 9, 1)),
        (date(2022, 6, 1), "Semestral", date(2022, 6, 1)),
        (date(2022, 9, 1), "Semestral", date(2022, 6, 1)),
        (date(2022, 6, 1), "Anual", date(2021, 12, 1)),
        (date(2022, 12, 1), "Anual", date(2022, 12, 1)),
    ],
)
def test_mes_ult_ocurr_triangulos(
    mes_corte: date,
    origin_grain: Literal["Mensual", "Trimestral", "Semestral", "Anual"],
    expected: date,
) -> None:
    assert base.mes_ult_ocurr_triangulos(mes_corte, origin_grain) == expected


@pytest.mark.fast
@pytest.mark.parametrize(
    "mes_corte, origin_grain, expected",
    [
        (date(2022, 1, 1), "Mensual", date(2022, 1, 1)),
        (date(2022, 9, 1), "Trimestral", date(2022, 7, 1)),
        (date(2022, 10, 1), "Trimestral", date(2022, 10, 1)),
        (date(2022, 6, 1), "Semestral", date(2022, 1, 1)),
        (date(2022, 9, 1), "Semestral", date(2022, 7, 1)),
        (date(2022, 12, 1), "Anual", date(2022, 1, 1)),
        (date(2023, 1, 1), "Anual", date(2023, 1, 1)),
    ],
)
def test_mes_prim_ocurr_periodo_act(
    mes_corte: date,
    origin_grain: Literal["Mensual", "Trimestral", "Semestral", "Anual"],
    expected: date,
) -> None:
    assert base.mes_prim_ocurr_periodo_act(mes_corte, origin_grain) == expected


@pytest.mark.fast
@pytest.mark.parametrize(
    "periodicidad_ocurrencia",
    ["Mensual", "Trimestral", "Semestral", "Anual"],
)
def test_analisis_triangulos(
    periodicidad_ocurrencia: Literal["Mensual", "Trimestral", "Semestral", "Anual"],
    rango_meses: tuple[date, date],
):
    mock_siniestros = generar_mock(rango_meses, "siniestros")
    base_triangulos, _ = base.generar_bases_siniestros(
        mock_siniestros.lazy(), "triangulos", *rango_meses
    )

    base_triangulos = base_triangulos.filter(
        pl.col("periodicidad_ocurrencia") == periodicidad_ocurrencia
    )

    mes_corte_tipicos = base.mes_ult_ocurr_triangulos(
        rango_meses[1], periodicidad_ocurrencia
    )

    mes_inicio = rango_meses[0]

    col_ocurr = base_triangulos.get_column("periodo_ocurrencia")
    assert col_ocurr.min() == utils.date_to_yyyymm(mes_inicio, periodicidad_ocurrencia)
    assert col_ocurr.max() == utils.date_to_yyyymm(mes_corte_tipicos)

    col_dllo = base_triangulos.get_column("periodo_desarrollo")
    assert col_dllo.min() == utils.date_to_yyyymm(mes_inicio, periodicidad_ocurrencia)
    assert col_dllo.max() == utils.date_to_yyyymm(mes_corte_tipicos)

    plata_original = filtrar_fechas(
        mock_siniestros.lazy(), (mes_inicio, mes_corte_tipicos)
    )
    plata_procesada = base_triangulos.filter(pl.col("diagonal") == 1)

    assert_igual(plata_procesada, plata_original, "pago_bruto")


@pytest.mark.fast
@pytest.mark.parametrize(
    "periodicidad_ocurrencia",
    ["Trimestral", "Semestral", "Anual"],
)
def test_analisis_entremes(
    periodicidad_ocurrencia: Literal["Trimestral", "Semestral", "Anual"],
    rango_meses: tuple[date, date],
):
    mock_siniestros = generar_mock(rango_meses, "siniestros")
    base_triangulos, base_ult_ocurr = base.generar_bases_siniestros(
        mock_siniestros.lazy(), "entremes", *rango_meses
    )

    base_triangulos = base_triangulos.filter(
        pl.col("periodicidad_ocurrencia") == periodicidad_ocurrencia
    )

    mes_inicio = rango_meses[0]
    mes_corte = rango_meses[1]

    col_ocurr = base_triangulos.get_column("periodo_ocurrencia")
    assert col_ocurr.min() == utils.date_to_yyyymm(mes_inicio, periodicidad_ocurrencia)
    assert col_ocurr.max() == utils.date_to_yyyymm(mes_corte, periodicidad_ocurrencia)

    col_dllo = base_triangulos.get_column("periodo_desarrollo")
    assert col_dllo.min() == utils.date_to_yyyymm(mes_inicio)
    assert col_dllo.max() == utils.date_to_yyyymm(mes_corte)

    plata_original = filtrar_fechas(mock_siniestros.lazy(), rango_meses)
    plata_procesada = base_triangulos.filter(pl.col("diagonal") == 1)

    assert_igual(plata_procesada, plata_original, "pago_bruto")

    base_ult_ocurr = base_ult_ocurr.filter(
        pl.col("periodicidad_triangulo") == periodicidad_ocurrencia
    )

    prim_ocurr = base.mes_prim_ocurr_periodo_act(mes_corte, periodicidad_ocurrencia)

    col_ocurr_ult = base_ult_ocurr.get_column("periodo_ocurrencia")
    assert col_ocurr_ult.min() == utils.date_to_yyyymm(prim_ocurr)
    assert col_ocurr_ult.max() == utils.date_to_yyyymm(mes_corte)

    plata_original_ult_ocurr = filtrar_fechas(
        mock_siniestros.lazy(), (prim_ocurr, mes_corte)
    )

    assert_igual(base_ult_ocurr, plata_original_ult_ocurr, "pago_bruto")

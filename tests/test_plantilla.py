import os
from datetime import date
from typing import Literal
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from src import constantes as ct
from src import plantilla as plant
from src.procesamiento import base_primas_expuestos, base_siniestros


@pytest.mark.parametrize(
    "mes_corte, tipo_analisis, plantilla",
    [
        (202312, "triangulos", "frec"),
        (202312, "triangulos", "seve"),
        (202312, "triangulos", "plata"),
        # (202312, "entremes"),
    ],
)
@patch("src.metodos_plantilla.insumos.df_primas")
@patch("src.metodos_plantilla.insumos.df_expuestos")
@patch("src.metodos_plantilla.insumos.df_atipicos")
@patch("src.metodos_plantilla.insumos.df_ult_ocurr")
@patch("src.metodos_plantilla.insumos.df_diagonales")
@patch("src.plantilla.tablas_resumen.df_aperturas")
def test_preparar_plantilla(
    mock_df_aperturas: MagicMock,
    mock_df_diagonales: MagicMock,
    mock_df_ult_ocurr: MagicMock,
    mock_df_atipicos: MagicMock,
    mock_df_expuestos: MagicMock,
    mock_df_primas: MagicMock,
    mock_siniestros: pl.LazyFrame,
    mock_expuestos: pl.LazyFrame,
    mock_primas: pl.LazyFrame,
    mes_corte: int,
    tipo_analisis: Literal["triangulos", "entremes"],
    plantilla: Literal["frec", "seve", "plata", "entremes"],
):
    base_triangulos, base_ult_ocurr, base_atipicos = (
        base_siniestros.generar_bases_siniestros(
            mock_siniestros, tipo_analisis, date(2018, 12, 1), date(2023, 12, 1)
        )
    )

    mock_df_aperturas.return_value = mock_siniestros
    mock_df_diagonales.return_value = base_triangulos.lazy()
    mock_df_ult_ocurr.return_value = base_ult_ocurr.lazy()
    mock_df_atipicos.return_value = base_atipicos.lazy()
    mock_df_expuestos.return_value = (
        base_primas_expuestos.generar_base_primas_expuestos(
            mock_expuestos, "expuestos", "mock"
        ).lazy()
    )
    mock_df_primas.return_value = base_primas_expuestos.generar_base_primas_expuestos(
        mock_primas, "primas", "mock"
    ).lazy()

    if os.path.exists("tests/mock_plantilla.xlsm"):
        os.remove("tests/mock_plantilla.xlsm")

    wb = plant.abrir_plantilla("tests/mock_plantilla.xlsm")
    wb = plant.preparar_plantilla(wb, mes_corte, tipo_analisis, "mock")

    assert wb.sheets["Main"]["A4"].value == "Mes corte"
    assert wb.sheets["Main"]["B4"].value == mes_corte
    assert wb.sheets["Main"]["A5"].value == "Mes anterior"
    assert wb.sheets["Main"]["B5"].value == (
        mes_corte - 1 if mes_corte % 100 != 1 else ((mes_corte // 100) - 1) * 100 + 12
    )

    if tipo_analisis == "triangulos":
        assert not wb.sheets["Plantilla_Entremes"].visible
        assert wb.sheets["Plantilla_Frec"].visible
        assert wb.sheets["Plantilla_Seve"].visible
        assert wb.sheets["Plantilla_Plata"].visible
    elif tipo_analisis == "entremes":
        assert wb.sheets["Plantilla_Entremes"].visible
        assert not wb.sheets["Plantilla_Frec"].visible
        assert not wb.sheets["Plantilla_Seve"].visible
        assert not wb.sheets["Plantilla_Plata"].visible

    assert "aperturas" in [table.name for table in wb.sheets["Main"].tables]
    assert "periodicidades" in [table.name for table in wb.sheets["Main"].tables]

    df_original = base_triangulos.filter(
        (pl.col("periodicidad_ocurrencia") == "Trimestral") & (pl.col("diagonal") == 1)
    ).select(
        [
            "apertura_reservas",
            "periodicidad_ocurrencia",
            "periodo_ocurrencia",
        ]
        + ct.COLUMNAS_QTYS
    )

    if tipo_analisis == "entremes":
        df_original = df_original.filter(
            pl.col("periodo_ocurrencia")
            != pl.col("periodo_ocurrencia").max().over("apertura_reservas")
        ).vstack(
            base_ult_ocurr.filter(
                pl.col("periodicidad_triangulo") == "Trimestral"
            ).drop("periodicidad_triangulo")
        )

    df_plantilla = ct.sheet_to_dataframe(wb, "Aux_Totales").collect()

    assert df_original.shape[0] == df_plantilla.shape[0]
    assert (
        abs(
            df_original.get_column("pago_bruto").sum()
            - df_plantilla.get_column("pago_bruto").sum()
        )
        < 100
    )

    wb = plant.generar_plantilla(wb, plantilla, "01_001_A_D", "bruto", mes_corte)

    # wb.close()

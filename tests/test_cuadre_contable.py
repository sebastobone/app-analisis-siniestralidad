from unittest.mock import MagicMock, patch

import pytest
import numpy as np
import polars as pl
from src import utils
from src.controles_informacion import controles_informacion as ctrl
from src.controles_informacion import cuadre_contable as cuadre

from tests.test_controles_informacion import mock_hoja_afo


@pytest.mark.parametrize("mes_corte", [202104, 202206, 202312])
@pytest.mark.parametrize(
    "qty", ["pago_bruto", "pago_retenido", "aviso_bruto", "aviso_retenido"]
)
@patch("src.controles_informacion.cuadre_contable.apertura_dif_soat")
@patch("src.controles_informacion.controles_informacion.pl.read_excel")
def test_cuadre_contable_soat(
    mock_read_excel: MagicMock,
    apertura_dif_soat: MagicMock,
    mock_siniestros: pl.LazyFrame,
    mes_corte: int,
    qty: str,
) -> None:
    mock_read_excel.return_value = mock_hoja_afo(mes_corte, "pago_bruto")
    apertura_dif_soat.return_value = pl.LazyFrame(
        {
            "codigo_op": ["01"],
            "codigo_ramo_op": ["001"],
            "ramo_desc": ["RAMO1"],
            "apertura_1": ["A"],
            "apertura_2": ["D"],
        }
    ).with_columns(utils.col_apertura_reservas("mock"))

    qtys = ["pago_bruto", "pago_retenido", "aviso_bruto", "aviso_retenido"]

    df_sap = (
        ctrl.consolidar_sap(["Generales"], ["pago_bruto", "pago_retenido"], mes_corte)
        .with_columns(
            aviso_bruto=pl.col("pago_bruto") * np.random.random(),
            aviso_retenido=pl.col("pago_retenido") * np.random.random(),
        )
        .filter((pl.col("codigo_ramo_op") == "001") & (pl.col("codigo_op") == "01"))
    )

    mock_soat = mock_siniestros.filter(
        (pl.col("codigo_ramo_op") == "001") & (pl.col("codigo_op") == "01")
    )

    df_tera = ctrl.agrupar_tera(
        mock_soat, ["codigo_op", "codigo_ramo_op", "fecha_registro"], qtys
    )

    dif_sap_vs_tera = ctrl.comparar_sap_tera(
        df_tera, df_sap.lazy(), mes_corte, qtys
    ).filter(pl.col("fecha_registro") <= utils.yyyymm_to_date(mes_corte))

    df_cuadre = cuadre.cuadre_contable(
        "soat", "siniestros", mock_soat, dif_sap_vs_tera.lazy()
    )

    cifra_sap = (
        df_sap.filter(pl.col("fecha_registro") == utils.yyyymm_to_date(mes_corte))
        .get_column(qty)
        .sum()
    )

    cifra_final = (
        df_cuadre.filter(pl.col("fecha_registro") == utils.yyyymm_to_date(mes_corte))
        .collect()
        .get_column(qty)
        .sum()
    )

    assert abs(cifra_sap - cifra_final) < 100

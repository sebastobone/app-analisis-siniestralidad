import polars as pl
from unittest.mock import patch
from src.controles_informacion import controles_informacion as ctrl
from src.app import Parametros
from datetime import date
from src import utils
from src import constantes as ct
import numpy as np
import pytest


def fechas_sap(params: Parametros) -> list[date]:
    return pl.date_range(
        utils.yyyymm_to_date(params.mes_inicio),
        utils.yyyymm_to_date(params.mes_corte),
        interval="1mo",
        eager=True,
    ).to_list()


def mock_hoja_afo(params: Parametros, qty: str) -> pl.DataFrame:
    fechas = fechas_sap(params)
    num_rows = len(fechas)

    signo = ctrl.signo_sap(qty)

    return pl.DataFrame(
        {
            "Ejercicio/Período": [
                f"{ct.NOMBRE_MES[fecha.month]} {fecha.year}" for fecha in fechas
            ],
            "column_1": ["Importe ML" for _ in range(num_rows)],
            ctrl.columna_ramo_sap(qty): ["COP" for _ in range(num_rows)],
            "001": np.random.random(size=num_rows) * signo * 1e8,
            "002": np.random.random(size=num_rows) * signo * 1e8,
            "003": np.random.random(size=num_rows) * signo * 1e8,
        }
    ).with_columns(pl.sum_horizontal("001", "002", "003").alias("Resultado total"))


@pytest.mark.parametrize("cia", ["Vida", "Generales"])
@pytest.mark.parametrize(
    "qty",
    [
        "pago_bruto",
        "pago_cedido",
        "aviso_bruto",
        "aviso_cedido",
        "prima_bruta",
        "prima_bruta_devengada",
        "prima_retenida",
        "prima_retenida_devengada",
    ],
)
def test_transformar_hoja_afo(params: Parametros, cia: str, qty: str):
    df_original = mock_hoja_afo(params, qty)
    sum_original = sum(
        [df_original.get_column(column).sum() for column in ["001", "002", "003"]]
    ) * ctrl.signo_sap(qty)

    df_processed = ctrl.transformar_hoja_afo(df_original, cia, qty, params.mes_corte)
    sum_processed = df_processed.collect().get_column(qty).sum()

    assert abs(sum_original - sum_processed) < 100

    hoja_afo_incompleta = df_original.slice(0, df_original.shape[0] - 1)
    with pytest.raises(ValueError) as exc_info:
        ctrl.transformar_hoja_afo(hoja_afo_incompleta, cia, qty, params.mes_corte)
    print(exc_info.value)

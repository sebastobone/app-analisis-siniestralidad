from datetime import date

import numpy as np
import polars as pl
import pytest
from src import constantes as ct
from src import utils
from src.controles_informacion import controles_informacion as ctrl


@pytest.mark.unit
@pytest.mark.parametrize(
    "qtys, expected",
    [
        (
            ["prima_bruta", "prima_cedida"],
            set(
                (
                    "prima_bruta",
                    "prima_retenida",
                    "prima_bruta_devengada",
                    "prima_retenida_devengada",
                )
            ),
        ),
        (
            ["pago_retenido", "aviso_bruto"],
            set(("pago_bruto", "pago_cedido", "aviso_bruto", "aviso_cedido")),
        ),
        (["rpnd_retenido"], set(("rpnd_bruto", "rpnd_cedido"))),
    ],
)
def test_definir_hojas_afo(qtys: list[str], expected: set[str]):
    assert ctrl.definir_hojas_afo(qtys) == expected


def fechas_sap(mes_corte: int) -> list[date]:
    return pl.date_range(
        utils.yyyymm_to_date(201001),
        utils.yyyymm_to_date(mes_corte),
        interval="1mo",
        eager=True,
    ).to_list()


def mock_hoja_afo(mes_corte: int, qty: str) -> pl.DataFrame:
    fechas = fechas_sap(mes_corte)
    num_rows = len(fechas)

    signo = ctrl.signo_sap(qty)

    return pl.DataFrame(
        {
            "Ejercicio/Período": [
                f"{ct.NOMBRE_MES[fecha.month]} {fecha.year}" for fecha in fechas
            ],
            "column_1": ["Importe ML" for _ in range(num_rows)],
            ctrl.columna_ramo_sap(qty): ["COP" for _ in range(num_rows)],
            "001": np.random.random(size=num_rows) * signo * 1e9,
            "002": np.random.random(size=num_rows) * signo * 1e9,
            "003": np.random.random(size=num_rows) * signo * 1e9,
        }
    ).with_columns(pl.sum_horizontal("001", "002", "003").alias("Resultado total"))


@pytest.mark.unit
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
def test_transformar_hoja_afo(cia: str, qty: str, rango_meses: tuple[date, date]):
    mes_corte_int = utils.date_to_yyyymm(rango_meses[1])
    df_original = mock_hoja_afo(mes_corte_int, qty)
    sum_original = sum(
        [df_original.get_column(column).sum() for column in ["001", "002", "003"]]
    ) * ctrl.signo_sap(qty)

    df_processed = ctrl.transformar_hoja_afo(df_original, cia, qty, mes_corte_int)
    sum_processed = df_processed.get_column(qty).sum()

    assert abs(sum_original - sum_processed) < 100

    hoja_afo_incompleta = df_original.slice(0, df_original.shape[0] - 1)
    with pytest.raises(ValueError):
        ctrl.transformar_hoja_afo(hoja_afo_incompleta, cia, qty, mes_corte_int)


@pytest.mark.unit
@pytest.mark.parametrize(
    "cias, qtys, mes_corte, expected_columns",
    [
        (
            ["Vida", "Generales"],
            ["prima_bruta", "prima_cedida"],
            202201,
            ["prima_bruta", "prima_cedida"],
        ),
        (
            ["Vida"],
            ["pago_retenido"],
            202201,
            ["pago_retenido"],
        ),
    ],
)
def test_consolidar_sap(cias, qtys, mes_corte, expected_columns):
    result = ctrl.consolidar_sap(cias, qtys, mes_corte)
    assert (
        result.collect_schema().names()
        == ["codigo_op", "codigo_ramo_op", "fecha_registro"] + expected_columns
    )
    assert result.shape[0] > 0

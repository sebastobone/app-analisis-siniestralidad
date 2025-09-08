from datetime import date

import numpy as np
import polars as pl
import pytest
from src import constantes as ct
from src.controles_informacion import generacion as gen
from src.controles_informacion import sap


@pytest.mark.fast
@pytest.mark.parametrize(
    "qtys, expected",
    [
        (
            ["prima_bruta", "prima_cedida"],
            set(ct.Valores().model_dump()["primas"].keys()),
        ),
        (
            ["pago_retenido", "aviso_bruto"],
            set(("pago_bruto", "pago_cedido", "aviso_bruto", "aviso_cedido")),
        ),
        (["rpnd_retenido"], set(("rpnd_bruto", "rpnd_cedido"))),
    ],
)
def test_definir_hojas_afo(qtys: list[str], expected: set[str]):
    assert sap.definir_hojas_afo(qtys) == expected


def fechas_sap(mes_corte: date) -> list[date]:
    return pl.date_range(
        date(2010, 1, 1), mes_corte, interval="1mo", eager=True
    ).to_list()


def mock_hoja_afo(mes_corte: date, qty: str) -> pl.DataFrame:
    fechas = fechas_sap(mes_corte)
    num_rows = len(fechas)

    signo = sap.signo_sap(qty)

    return pl.DataFrame(
        {
            "Ejercicio/Período": [
                f"{ct.NOMBRE_MES[fecha.month]} {fecha.year}" for fecha in fechas
            ],
            "column_1": ["Importe ML" for _ in range(num_rows)],
            sap.columna_ramo_sap(qty): ["COP" for _ in range(num_rows)],
            "001": np.random.random(size=num_rows) * signo * 1e9,
            "002": np.random.random(size=num_rows) * signo * 1e9,
            "003": np.random.random(size=num_rows) * signo * 1e9,
        }
    ).with_columns(pl.sum_horizontal("001", "002", "003").alias("Resultado total"))


@pytest.mark.asyncio
@pytest.mark.fast
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
async def test_transformar_hoja_afo(cia: str, qty: str, rango_meses: tuple[date, date]):
    df_original = mock_hoja_afo(rango_meses[1], qty)
    sum_original = sum(
        [df_original.get_column(column).sum() for column in ["001", "002", "003"]]
    ) * sap.signo_sap(qty)

    df_processed = await sap.transformar_hoja_afo(df_original, cia, qty, rango_meses[1])
    sum_processed = df_processed.get_column(qty).sum()

    assert abs(sum_original - sum_processed) < 100

    hoja_afo_incompleta = df_original.slice(0, df_original.shape[0] - 1)
    with pytest.raises(ValueError):
        await sap.transformar_hoja_afo(hoja_afo_incompleta, cia, qty, rango_meses[1])


@pytest.mark.asyncio
@pytest.mark.fast
async def test_verificar_existencia_afos():
    with pytest.raises(FileNotFoundError):
        await gen.verificar_existencia_afos("demo")

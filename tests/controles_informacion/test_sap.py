import json
from datetime import date

import numpy as np
import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import constantes as ct
from src.controles_informacion import sap
from src.models import Parametros
from tests.conftest import CONTENT_TYPES, ingresar_parametros


@pytest.mark.fast
def test_afo_desactualizado(client: TestClient):
    _ = ingresar_parametros(
        client,
        Parametros(
            negocio="demo",
            mes_inicio=date(2010, 1, 1),
            mes_corte=date(2035, 1, 1),
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test",
        ),
        {
            "archivo_segmentacion": (
                "segmentacion_test.xlsx",
                open("data/segmentacion_demo.xlsx", "rb"),
                CONTENT_TYPES["xlsx"],
            )
        },
    )

    controles = {
        "cuadrar_siniestros": True,
        "cuadrar_primas": True,
        "archivos_siniestros": None,
        "archivos_primas": None,
        "archivos_expuestos": None,
    }

    afos = [
        (
            "generales",
            (
                "Generales.xlsx",
                open("data/afo/Generales.xlsx", "rb"),
                CONTENT_TYPES["xlsx"],
            ),
        ),
        (
            "vida",
            ("Vida.xlsx", open("data/afo/Vida.xlsx", "rb"), CONTENT_TYPES["xlsx"]),
        ),
    ]

    with pytest.raises(ValueError, match="No se pudo encontrar el mes"):
        _ = client.post(
            "/generar-controles", data={"controles": json.dumps(controles)}, files=afos
        )


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

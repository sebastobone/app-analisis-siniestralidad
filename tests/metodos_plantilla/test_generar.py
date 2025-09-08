from datetime import date
from pathlib import Path
from typing import Literal

import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import constantes as ct
from src.informacion.mocks import generar_mock
from src.metodos_plantilla import abrir, actualizar, generar, preparar
from src.models import Parametros
from src.procesamiento import base_siniestros
from tests.conftest import ingresar_parametros


@pytest.fixture(autouse=True)
def params(client: TestClient, rango_meses: tuple[date, date]) -> Parametros:
    return ingresar_parametros(
        client,
        Parametros(
            negocio="demo",
            mes_inicio=rango_meses[0],
            mes_corte=rango_meses[1],
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test",
        ),
    )


@pytest.mark.fast
@pytest.mark.parametrize(
    "tipo_analisis, periodicidad_ocurrencia",
    [
        ("triangulos", "Mensual"),
        ("triangulos", "Trimestral"),
        ("triangulos", "Semestral"),
        ("triangulos", "Anual"),
        ("entremes", "Trimestral"),
        ("entremes", "Semestral"),
        ("entremes", "Anual"),
    ],
)
def test_forma_triangulo(
    tipo_analisis: Literal["triangulos", "entremes"],
    periodicidad_ocurrencia: Literal["Mensual", "Trimestral", "Semestral", "Anual"],
    rango_meses: tuple[date, date],
):
    mock_siniestros = generar_mock(rango_meses, "siniestros")
    base_triangulos, _ = base_siniestros.generar_bases_siniestros(
        mock_siniestros.lazy(), tipo_analisis, *rango_meses
    )

    df = generar.crear_triangulo_base_plantilla(
        base_triangulos.lazy(),
        "01_040_A_D",
        "bruto",
        pl.DataFrame(
            {
                "apertura_reservas": ["01_040_A_D"],
                "periodicidad_ocurrencia": [periodicidad_ocurrencia],
            }
        ),
        ["pago", "incurrido"],
    )

    if tipo_analisis == "triangulos":
        assert df.shape[0] == df.shape[1] // 2
    elif tipo_analisis == "entremes":
        assert (
            df.shape[0] * ct.PERIODICIDADES[periodicidad_ocurrencia] >= df.shape[1] // 2
        )


@pytest.mark.plantilla
def test_plantilla_no_preparada(client: TestClient, rango_meses: tuple[date, date]):
    p = ingresar_parametros(
        client,
        Parametros(
            negocio="demo",
            mes_inicio=rango_meses[0],
            mes_corte=rango_meses[1],
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test_no_prep",
        ),
    )
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    with pytest.raises(preparar.PlantillaNoPreparadaError):
        _ = client.post(
            "/generar-plantilla",
            data={"apertura": "01_040_A_D", "atributo": "bruto", "plantilla": "plata"},
        )

    wb.close()
    Path(f"plantillas/{p.nombre_plantilla}.xlsm").unlink()


@pytest.mark.plantilla
def test_generar_severidad(client: TestClient, params: Parametros):
    wb = abrir.abrir_plantilla(f"plantillas/{params.nombre_plantilla}.xlsm")

    _ = client.post("/preparar-plantilla")

    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_040_A_D", "atributo": "bruto", "plantilla": "severidad"},
    )

    apertura_en_frecuencia = actualizar.obtener_apertura_actual(wb, "frecuencia")
    apertura_en_severidad = actualizar.obtener_apertura_actual(wb, "severidad")
    assert apertura_en_frecuencia == apertura_en_severidad

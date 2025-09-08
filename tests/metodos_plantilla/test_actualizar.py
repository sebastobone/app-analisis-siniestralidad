from datetime import date

import pytest
from fastapi.testclient import TestClient
from src.metodos_plantilla import abrir, actualizar
from src.models import Parametros
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


@pytest.mark.plantilla
def test_actualizar_sin_generar(client: TestClient):
    _ = client.post("/preparar-plantilla")
    with pytest.raises(actualizar.PlantillaNoGeneradaError):
        _ = client.post(
            "/actualizar-plantilla",
            data={"apertura": "01_040_A_D", "atributo": "bruto", "plantilla": "plata"},
        )


@pytest.mark.plantilla
def test_actualizar_diferentes_periodicidades(client: TestClient):
    _ = client.post("/preparar-plantilla")
    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_040_A_D", "atributo": "bruto", "plantilla": "plata"},
    )

    with pytest.raises(actualizar.PeriodicidadDiferenteError):
        _ = client.post(
            "/actualizar-plantilla",
            data={"apertura": "01_041_A_D", "atributo": "bruto", "plantilla": "plata"},
        )


@pytest.mark.plantilla
def test_actualizar_severidad(client: TestClient, params: Parametros):
    wb = abrir.abrir_plantilla(f"plantillas/{params.nombre_plantilla}.xlsm")

    _ = client.post("/preparar-plantilla")

    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_040_A_D", "atributo": "bruto", "plantilla": "severidad"},
    )

    _ = client.post(
        "/actualizar-plantilla",
        data={"apertura": "01_040_A_E", "atributo": "bruto", "plantilla": "severidad"},
    )

    apertura_en_frecuencia = actualizar.obtener_apertura_actual(wb, "frecuencia")
    apertura_en_severidad = actualizar.obtener_apertura_actual(wb, "severidad")
    assert apertura_en_frecuencia == apertura_en_severidad

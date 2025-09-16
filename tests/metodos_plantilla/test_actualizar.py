from datetime import date

import pytest
import xlwings as xw
from fastapi.testclient import TestClient
from src import constantes as ct
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


@pytest.mark.plantilla
def test_actualizar_al_preparar(client: TestClient, params: Parametros):
    """
    Verifica que, al preparar una plantilla, las hojas ya generadas
    se actualicen con los nuevos datos.
    """
    wb_test = abrir.abrir_plantilla(f"plantillas/{params.nombre_plantilla}.xlsm")

    client.post("/preparar-plantilla")
    client.post(
        "/generar-plantilla",
        data={"apertura": "01_040_A_D", "atributo": "bruto", "plantilla": "plata"},
    )

    cifra_1 = obtener_cifra(wb_test.sheets["Plata"])

    # Volvemos a ingresar parametros para que se generen nuevos mocks
    ingresar_parametros(client, params)
    client.post("/preparar-plantilla")

    cifra_2 = obtener_cifra(wb_test.sheets["Plata"])

    assert cifra_1 != cifra_2


def obtener_cifra(hoja: xw.Sheet) -> float:
    return hoja.cells(
        ct.FILA_INI_PLANTILLAS + ct.HEADER_TRIANGULOS, ct.COL_OCURRS_PLANTILLAS + 1
    ).value

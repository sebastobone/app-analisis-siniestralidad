from datetime import date

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from src.models import Parametros
from tests.conftest import ingresar_parametros


@pytest.mark.plantilla
def test_actualizar_grafica_factores(
    client: TestClient, rango_meses: tuple[date, date]
):
    _ = ingresar_parametros(
        client,
        Parametros(
            negocio="demo",
            mes_inicio=rango_meses[0],
            mes_corte=rango_meses[1],
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test",
        ),
    )

    _ = client.post("/preparar-plantilla")
    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_001_A_D", "atributo": "bruto", "plantilla": "plata"},
    )
    response = client.post(
        "/ajustar-grafica-factores",
        data={"apertura": "01_001_A_D", "atributo": "bruto", "plantilla": "plata"},
    )

    assert response.status_code == status.HTTP_200_OK

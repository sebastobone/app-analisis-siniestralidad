from datetime import date

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from tests.conftest import agregar_meses_params, correr_queries, vaciar_directorios_test


@pytest.mark.plantilla
@pytest.mark.integration
def test_actualizar_grafica_factores(
    client: TestClient, rango_meses: tuple[date, date]
):
    vaciar_directorios_test()

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form)

    correr_queries(client)

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

    vaciar_directorios_test()

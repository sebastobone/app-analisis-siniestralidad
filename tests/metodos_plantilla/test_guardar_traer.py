import os
from datetime import date

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from tests.conftest import agregar_meses_params, correr_queries


@pytest.mark.plantilla
def test_guardar_traer_triangulos(client: TestClient, rango_meses: tuple[date, date]):
    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", params=params_form)
    correr_queries(client)
    _ = client.post("/preparar-plantilla")

    rangos = [
        "EXCLUSIONES",
        "VENTANAS",
        "TIPO_FACTORES_SELECCIONADOS",
        "FACTORES_SELECCIONADOS",
        "MET_PAGO_INCURRIDO",
        "ULTIMATE",
        "METODOLOGIA",
        "BASE",
        "INDICADOR",
        "COMENTARIOS",
    ]

    apertura = "01_001_A_D"
    atributo = "bruto"
    for plantilla in ["frecuencia", "severidad", "plata"]:
        guardar_traer_apertura(client, rangos, apertura, atributo, plantilla)


@pytest.mark.plantilla
def test_guardar_traer_entremes(client: TestClient, rango_meses: tuple[date, date]):
    apertura = "01_001_A_D"
    atributo = "bruto"
    plantilla = "completar_diagonal"

    rangos = [
        "EXCLUSIONES",
        "VENTANAS",
        "TIPO_FACTORES_SELECCIONADOS",
        "FACTORES_SELECCIONADOS",
    ]

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)
    params_form.update({"mes_corte": "202412"})

    _ = client.post("/ingresar-parametros", params=params_form)
    correr_queries(client)
    _ = client.post("/preparar-plantilla")
    guardar_traer_apertura(client, rangos, apertura, atributo, "plata")
    _ = client.post("/almacenar-analisis")

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "entremes",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)
    params_form.update({"mes_corte": "202501"})

    _ = client.post("/ingresar-parametros", params=params_form)
    correr_queries(client)
    _ = client.post(
        "/preparar-plantilla",
        data={
            "referencia_actuarial": "triangulos",
            "referencia_contable": "triangulos",
        },
    )

    with pytest.raises(FileNotFoundError):
        _ = client.post("/traer-entremes")

    response = client.post("/guardar-entremes")
    assert response.status_code == status.HTTP_200_OK

    response = client.post("/traer-entremes")
    assert response.status_code == status.HTTP_200_OK

    guardar_traer_apertura(client, rangos, apertura, atributo, plantilla)


@pytest.mark.plantilla
def guardar_traer_apertura(
    client: TestClient, rangos: list[str], apertura: str, atributo: str, plantilla: str
) -> None:
    _ = client.post(
        "/generar-plantilla",
        data={
            "apertura": apertura,
            "atributo": atributo,
            "plantilla": plantilla,
        },
    )

    archivos_guardados = [
        f"wb_test.xlsm_{apertura}_{atributo}_{plantilla.capitalize()}_{nombre_rango}"
        for nombre_rango in rangos
    ]

    with pytest.raises(FileNotFoundError):
        _ = client.post(
            "/traer-apertura",
            data={
                "apertura": apertura,
                "atributo": atributo,
                "plantilla": plantilla,
            },
        )

    response = client.post(
        "/guardar-apertura",
        data={"apertura": apertura, "atributo": atributo, "plantilla": plantilla},
    )
    assert response.status_code == status.HTTP_200_OK
    for archivo in archivos_guardados:
        assert os.path.exists(f"data/db/{archivo}.parquet")

    response = client.post(
        "/traer-apertura",
        data={"apertura": apertura, "atributo": atributo, "plantilla": plantilla},
    )
    assert response.status_code == status.HTTP_200_OK

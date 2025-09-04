from datetime import date

import pytest
from fastapi.testclient import TestClient
from src.metodos_plantilla import abrir, actualizar
from tests.conftest import agregar_meses_params, correr_queries, vaciar_directorios_test


@pytest.mark.plantilla
def test_actualizar_sin_generar(client: TestClient, rango_meses: tuple[date, date]):
    vaciar_directorios_test()

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", params=params_form).json()

    correr_queries(client)

    _ = client.post("/preparar-plantilla")

    with pytest.raises(actualizar.PlantillaNoGeneradaError):
        _ = client.post(
            "/actualizar-plantilla",
            data={"apertura": "01_001_A_D", "atributo": "bruto", "plantilla": "plata"},
        )

    vaciar_directorios_test()


@pytest.mark.plantilla
def test_actualizar_diferentes_periodicidades(
    client: TestClient, rango_meses: tuple[date, date]
):
    vaciar_directorios_test()

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", params=params_form).json()

    correr_queries(client)

    _ = client.post("/preparar-plantilla")

    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_001_A_D", "atributo": "bruto", "plantilla": "plata"},
    )

    with pytest.raises(actualizar.PeriodicidadDiferenteError):
        _ = client.post(
            "/actualizar-plantilla",
            data={"apertura": "01_002_A_D", "atributo": "bruto", "plantilla": "plata"},
        )

    vaciar_directorios_test()


@pytest.mark.plantilla
def test_actualizar_severidad(client: TestClient, rango_meses: tuple[date, date]):
    vaciar_directorios_test()

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", params=params_form).json()
    wb = abrir.abrir_plantilla(f"plantillas/{params_form['nombre_plantilla']}.xlsm")

    correr_queries(client)

    _ = client.post("/preparar-plantilla")

    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_001_A_D", "atributo": "bruto", "plantilla": "severidad"},
    )

    _ = client.post(
        "/actualizar-plantilla",
        data={"apertura": "01_001_A_E", "atributo": "bruto", "plantilla": "severidad"},
    )

    apertura_en_frecuencia = actualizar.obtener_apertura_actual(wb, "frecuencia")
    apertura_en_severidad = actualizar.obtener_apertura_actual(wb, "severidad")
    assert apertura_en_frecuencia == apertura_en_severidad

    vaciar_directorios_test()

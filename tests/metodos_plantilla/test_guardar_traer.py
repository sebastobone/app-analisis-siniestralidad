import os
from datetime import date

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from tests.conftest import agregar_meses_params, vaciar_directorio


@pytest.mark.plantilla
@pytest.mark.integration
def test_guardar_traer(client: TestClient, rango_meses: tuple[date, date]):
    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form)

    _ = client.post("/correr-query-siniestros")
    _ = client.post("/correr-query-primas")
    _ = client.post("/correr-query-expuestos")

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
        _ = client.post(
            "/generar-plantilla",
            data={
                "apertura": "01_001_A_D",
                "atributo": "bruto",
                "plantilla": plantilla,
            },
        )

        archivos_guardados = [
            f"{apertura}_{atributo}_{plantilla.capitalize()}_{nombre_rango}"
            for nombre_rango in rangos
        ]

        vaciar_directorio("data/db")
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

    vaciar_directorio("data/raw")
    vaciar_directorio("data/processed")
    vaciar_directorio("data/db")

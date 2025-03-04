import os
from datetime import date

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from tests.conftest import vaciar_directorio
from tests.metodos_plantilla.conftest import agregar_meses_params


@pytest.mark.plantilla
@pytest.mark.integration
def test_guardar_traer(client: TestClient, rango_meses: tuple[date, date]):
    params_form = {
        "negocio": "mock",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    response = client.post("/ingresar-parametros", data=params_form)

    _ = client.post("/preparar-plantilla")

    rangos = [
        "EXCLUSIONES",
        "VENTANAS",
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
            "/modos-plantilla",
            data={
                "apertura": "01_001_A_D",
                "atributo": "bruto",
                "plantilla": plantilla,
                "modo": "generar",
            },
        )

        archivos_guardados = [
            f"{apertura}_{atributo}_{plantilla.capitalize()}_{nombre_rango}"
            for nombre_rango in rangos
        ]

        vaciar_directorio("data/db")
        with pytest.raises(FileNotFoundError):
            _ = client.post(
                "/modos-plantilla",
                data={
                    "apertura": apertura,
                    "atributo": atributo,
                    "plantilla": plantilla,
                    "modo": "traer",
                },
            )

        response = client.post(
            "/modos-plantilla",
            data={
                "apertura": apertura,
                "atributo": atributo,
                "plantilla": plantilla,
                "modo": "guardar",
            },
        )
        assert response.status_code == status.HTTP_200_OK
        for archivo in archivos_guardados:
            assert os.path.exists(f"data/db/{archivo}.parquet")

        response = client.post(
            "/modos-plantilla",
            data={
                "apertura": apertura,
                "atributo": atributo,
                "plantilla": plantilla,
                "modo": "traer",
            },
        )
        assert response.status_code == status.HTTP_200_OK

    vaciar_directorio("data/raw")
    vaciar_directorio("data/processed")
    vaciar_directorio("data/db")

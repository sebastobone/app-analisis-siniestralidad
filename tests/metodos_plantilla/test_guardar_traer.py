import os
from datetime import date

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from src.models import Parametros
from tests.conftest import agregar_meses_params, vaciar_directorios_test


@pytest.mark.plantilla
@pytest.mark.integration
def test_guardar_traer(client: TestClient, rango_meses: tuple[date, date]):
    vaciar_directorios_test()

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    response = client.post("/ingresar-parametros", data=params_form).json()
    p = Parametros.model_validate(response)

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
            f"{p.nombre_plantilla}.xlsm_{apertura}_{atributo}_{plantilla.capitalize()}_{nombre_rango}"
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

    vaciar_directorios_test()

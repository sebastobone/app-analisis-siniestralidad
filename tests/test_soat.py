from typing import Literal

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session
from src import app


@pytest.fixture
def params_soat() -> dict[str, str]:
    return {
        "negocio": "soat",
        "mes_inicio": "201901",
        "mes_corte": "202401",
        "tipo_analisis": "triangulos",
        "aproximar_reaseguro": "False",
        "nombre_plantilla": "plantilla_test_soat",
        "cuadre_contable_sinis": "True",
        "add_fraude_soat": "False",
        "cuadre_contable_primas": "False",
    }


@pytest.mark.soat
@pytest.mark.end_to_end
@pytest.mark.teradata
def test_info_soat(
    client: TestClient, test_session: Session, params_soat: dict[str, str]
):
    cookies = {"session_id": "test-session-id"}
    _ = client.post("/ingresar-parametros", data=params_soat, cookies=cookies)

    for query in ["siniestros", "primas", "expuestos"]:
        _ = client.post(f"/correr-query-{query}", cookies=cookies)

    _ = client.post("/generar-controles", cookies=cookies)


@pytest.mark.soat
@pytest.mark.end_to_end
@pytest.mark.parametrize(
    "tipo_analisis, plantillas",
    [
        ("triangulos", ["frec", "seve"]),
        ("triangulos", ["plata"]),
        ("entremes", ["entremes"]),
    ],
)
def test_plantilla_soat(
    client: TestClient,
    test_session: Session,
    params_soat: dict[str, str],
    tipo_analisis: Literal["triangulos", "entremes"],
    plantillas: list[Literal["frec", "seve", "plata", "entremes"]],
):
    params_soat_mod = params_soat.copy()
    params_soat_mod["tipo_analisis"] = tipo_analisis

    cookies = {"session_id": "test-session-id"}
    _ = client.post("/ingresar-parametros", data=params_soat, cookies=cookies)

    _ = client.post("/preparar-plantilla", cookies=cookies)
    _ = client.post("/guardar-plantilla", cookies=cookies)
    _ = client.post("/preparar-plantilla", cookies=cookies)

    for plantilla in plantillas:
        _ = app.modos_plantilla(
            plantilla, "guardar_todo", test_session, "test-session-id"
        )

    _ = client.post("/almacenar-analisis", cookies=cookies)

    _ = client.post("/actualizar-wb-resultados", cookies=cookies)
    _ = client.post("/generar-informe-ar", cookies=cookies)

import pytest
from fastapi.testclient import TestClient


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
def test_info_soat(client: TestClient, params_soat: dict[str, str]):
    cookies = {"session_id": "test-session-id"}
    _ = client.post("/ingresar-parametros", data=params_soat, cookies=cookies)

    for query in ["siniestros", "primas", "expuestos"]:
        _ = client.post(f"/correr-query-{query}", cookies=cookies)

    _ = client.post("/generar-controles", cookies=cookies)

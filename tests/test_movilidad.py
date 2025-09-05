import pytest
from fastapi.testclient import TestClient
from src import constantes as ct
from src.models import Parametros

from tests.conftest import correr_queries, validar_cuadre


@pytest.mark.asyncio
@pytest.mark.movilidad
@pytest.mark.teradata
async def test_info_movilidad(client: TestClient):
    params = {
        "negocio": "movilidad",
        "mes_inicio": "201401",
        "mes_corte": "202412",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "plantilla_test_movilidad",
    }

    response = client.post("/ingresar-parametros", params=params)
    p = Parametros.model_validate_json(response.read())

    correr_queries(client)

    _ = client.post("/generar-controles")

    await validar_cuadre(
        "movilidad", "siniestros", ct.COLUMNAS_SINIESTROS_CUADRE, p.mes_corte
    )
    await validar_cuadre(
        "movilidad", "primas", ct.Valores().model_dump()["primas"].keys(), p.mes_corte
    )

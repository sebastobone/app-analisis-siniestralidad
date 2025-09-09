from datetime import date

import pytest
from fastapi.testclient import TestClient
from src import constantes as ct
from src.models import Parametros

from tests.conftest import correr_queries, ingresar_parametros, validar_cuadre


@pytest.mark.asyncio
@pytest.mark.movilidad
@pytest.mark.teradata
async def test_info_movilidad(client: TestClient):
    p = ingresar_parametros(
        client,
        Parametros(
            negocio="movilidad",
            mes_inicio=date(2014, 1, 1),
            mes_corte=date(2024, 12, 31),
            tipo_analisis="triangulos",
            nombre_plantilla="plantilla_movilidad",
        ),
    )

    correr_queries(client)

    _ = client.post("/generar-controles")

    await validar_cuadre(
        "movilidad", "siniestros", ct.COLUMNAS_SINIESTROS_CUADRE, p.mes_corte
    )
    await validar_cuadre(
        "movilidad", "primas", ct.Valores().model_dump()["primas"].keys(), p.mes_corte
    )

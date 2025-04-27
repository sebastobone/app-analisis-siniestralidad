from typing import Literal

import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import constantes as ct
from src.controles_informacion.sap import consolidar_sap
from src.models import Parametros

from tests.conftest import assert_igual, vaciar_directorios_test


@pytest.mark.asyncio
@pytest.mark.movilidad
@pytest.mark.integration
@pytest.mark.teradata
async def test_info_movilidad(client: TestClient):
    vaciar_directorios_test()

    params = {
        "negocio": "movilidad",
        "mes_inicio": "201401",
        "mes_corte": "202412",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "plantilla_test_movilidad",
        "cuadre_contable_sinis": "True",
        "cuadre_contable_primas": "True",
    }

    response = client.post("/ingresar-parametros", data=params)
    p = Parametros.model_validate_json(response.read())

    _ = client.post("/correr-query-siniestros")
    _ = client.post("/correr-query-primas")
    _ = client.post("/correr-query-expuestos")

    _ = client.post("/generar-controles")

    await validar_cuadre("siniestros", ct.COLUMNAS_SINIESTROS_CUADRE, p.mes_corte)
    await validar_cuadre("primas", ct.COLUMNAS_VALORES_TERADATA["primas"], p.mes_corte)

    vaciar_directorios_test()


async def validar_cuadre(
    file: Literal["siniestros", "primas"],
    columnas_cantidades: list[str],
    mes_corte: int,
) -> None:
    meses_cuadre = pl.read_excel(
        "data/segmentacion_movilidad.xlsx", sheet_name=f"Meses_cuadre_{file}"
    ).filter(pl.col("incluir") == 1)
    base_post_cuadre = pl.read_parquet(f"data/raw/{file}.parquet").join(
        meses_cuadre, on="fecha_registro"
    )
    sap = (
        (await consolidar_sap("movilidad", columnas_cantidades, mes_corte))
        .filter(pl.col("codigo_ramo_op") == "040")
        .join(meses_cuadre, on="fecha_registro")
    )
    for col in columnas_cantidades:
        assert_igual(base_post_cuadre, sap, col)

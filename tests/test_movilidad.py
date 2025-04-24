from typing import Literal

import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import constantes as ct
from src import utils
from src.controles_informacion.sap import consolidar_sap
from src.models import Parametros

from tests.conftest import assert_igual, vaciar_directorio


@pytest.mark.asyncio
@pytest.mark.movilidad
@pytest.mark.integration
@pytest.mark.teradata
async def test_info_movilidad(client: TestClient):
    vaciar_directorio("data/raw")
    vaciar_directorio("data/controles_informacion")
    vaciar_directorio("data/controles_informacion/pre_cuadre_contable")
    vaciar_directorio("data/controles_informacion/post_cuadre_contable")

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

    await validar_cuadre(
        "siniestros", ct.COLUMNAS_SINIESTROS_CUADRE, p.mes_inicio, p.mes_corte
    )
    await validar_cuadre(
        "primas", ct.COLUMNAS_VALORES_TERADATA["primas"], p.mes_inicio, p.mes_corte
    )

    vaciar_directorio("data/raw")
    vaciar_directorio("data/controles_informacion")
    vaciar_directorio("data/controles_informacion/pre_cuadre_contable")
    vaciar_directorio("data/controles_informacion/post_cuadre_contable")


async def validar_cuadre(
    file: Literal["siniestros", "primas"],
    columnas_cantidades: list[str],
    mes_inicio: int,
    mes_corte: int,
) -> None:
    mes_inicio_dt = utils.yyyymm_to_date(mes_inicio)
    base_post_cuadre = pl.read_parquet(f"data/raw/{file}.parquet")
    sap = (await consolidar_sap("movilidad", columnas_cantidades, mes_corte)).filter(
        pl.col("codigo_ramo_op") == "040"
    )
    for col in columnas_cantidades:
        assert_igual(
            base_post_cuadre.filter(pl.col("fecha_registro") >= mes_inicio_dt),
            sap.filter(pl.col("fecha_registro") >= mes_inicio_dt),
            col,
        )

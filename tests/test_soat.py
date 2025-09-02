import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import constantes as ct
from src import utils
from src.models import Parametros

from tests.conftest import correr_queries, vaciar_directorios_test, validar_cuadre


@pytest.mark.asyncio
@pytest.mark.soat
@pytest.mark.teradata
async def test_info_soat(client: TestClient):
    vaciar_directorios_test()

    params = {
        "negocio": "soat",
        "mes_inicio": "201901",
        "mes_corte": "202401",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "plantilla_test_soat",
    }

    response = client.post("/ingresar-parametros", data=params)
    p = Parametros.model_validate_json(response.read())

    correr_queries(client)

    _ = client.post("/generar-controles")

    mes_corte_dt = utils.yyyymm_to_date(p.mes_corte)

    df_sinis_post_cuadre = pl.read_parquet("data/raw/siniestros_post_cuadre.parquet")
    df_sinis_post_ajustes = pl.read_parquet("data/raw/siniestros.parquet")

    await validar_cuadre(
        "soat", "siniestros", ct.COLUMNAS_SINIESTROS_CUADRE, p.mes_corte
    )
    await validar_cuadre(
        "soat", "primas", ct.Valores().model_dump()["primas"].keys(), p.mes_corte
    )

    df_ajustes_fraude = (
        pl.read_excel("data/segmentacion_soat.xlsx", sheet_name="Ajustes_Fraude")
        .drop("tipo_ajuste")
        .filter(pl.col("fecha_registro") <= mes_corte_dt)
    )

    for col in ct.COLUMNAS_SINIESTROS_CUADRE:
        assert (
            abs(
                df_sinis_post_ajustes.get_column(col).sum()
                - df_sinis_post_cuadre.get_column(col).sum()
                - df_ajustes_fraude.get_column(col).sum()
            )
            < 100
        )

    vaciar_directorios_test()

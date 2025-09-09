from datetime import date

import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import constantes as ct
from src.models import Parametros

from tests.conftest import correr_queries, ingresar_parametros, validar_cuadre


@pytest.mark.asyncio
@pytest.mark.soat
@pytest.mark.teradata
async def test_info_soat(client: TestClient):
    p = ingresar_parametros(
        client,
        Parametros(
            negocio="soat",
            mes_inicio=date(2019, 1, 1),
            mes_corte=date(2024, 12, 31),
            tipo_analisis="triangulos",
            nombre_plantilla="plantilla_soat",
        ),
    )

    correr_queries(client)

    _ = client.post("/generar-controles")

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
        .filter(pl.col("fecha_registro") <= p.mes_corte)
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

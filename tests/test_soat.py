import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import constantes as ct
from src import utils
from src.controles_informacion.sap import consolidar_sap
from src.models import Parametros

from tests.conftest import assert_diferente, assert_igual, vaciar_directorio


@pytest.mark.asyncio
@pytest.mark.soat
@pytest.mark.integration
@pytest.mark.teradata
async def test_info_soat(client: TestClient):
    vaciar_directorio("data/raw")
    vaciar_directorio("data/controles_informacion")
    vaciar_directorio("data/controles_informacion/pre_cuadre_contable")
    vaciar_directorio("data/controles_informacion/post_cuadre_contable")
    vaciar_directorio("data/controles_informacion/post_ajustes_fraude")

    params = {
        "negocio": "soat",
        "mes_inicio": "201901",
        "mes_corte": "202401",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "plantilla_test_soat",
        "cuadre_contable_sinis": "True",
        "add_fraude_soat": "True",
        "cuadre_contable_primas": "True",
    }

    response = client.post("/ingresar-parametros", data=params)
    p = Parametros.model_validate_json(response.read())

    _ = client.post("/correr-query-siniestros")
    _ = client.post("/correr-query-primas")
    _ = client.post("/correr-query-expuestos")

    _ = client.post("/generar-controles")

    mes_inicio_dt = utils.yyyymm_to_date(p.mes_inicio)
    mes_corte_dt = utils.yyyymm_to_date(p.mes_corte)

    df_sinis_post_cuadre = pl.read_parquet("data/raw/siniestros_post_cuadre.parquet")
    df_sinis_post_ajustes = pl.read_parquet("data/raw/siniestros.parquet")

    df_ajustes_fraude = (
        pl.read_excel("data/segmentacion_soat.xlsx", sheet_name="Ajustes_Fraude")
        .drop("tipo_ajuste")
        .filter(pl.col("fecha_registro") <= mes_corte_dt)
    )

    sap_siniestros = (
        await consolidar_sap("soat", ct.COLUMNAS_SINIESTROS_CUADRE, p.mes_corte)
    ).filter(pl.col("codigo_ramo_op") == "041")

    for col in ct.COLUMNAS_SINIESTROS_CUADRE:
        assert_igual(
            df_sinis_post_cuadre.filter(pl.col("fecha_registro") >= mes_inicio_dt),
            sap_siniestros.filter(pl.col("fecha_registro") >= mes_inicio_dt),
            col,
        )
        assert (
            abs(
                df_sinis_post_ajustes.get_column(col).sum()
                - df_sinis_post_cuadre.get_column(col).sum()
                - df_ajustes_fraude.get_column(col).sum()
            )
            < 100
        )

    df_primas_post_cuadre = pl.read_parquet("data/raw/primas.parquet")

    sap_primas = (await consolidar_sap("soat", ct.COLUMNAS_PRIMAS, p.mes_corte)).filter(
        pl.col("codigo_ramo_op") == "041"
    )

    for col in ["prima_bruta", "prima_retenida", "prima_retenida_devengada"]:
        assert_igual(
            df_primas_post_cuadre.filter(pl.col("fecha_registro") >= mes_inicio_dt),
            sap_primas.filter(pl.col("fecha_registro") >= mes_inicio_dt),
            col,
        )

    assert_diferente(
        df_primas_post_cuadre.filter(pl.col("fecha_registro") >= mes_inicio_dt),
        sap_primas.filter(pl.col("fecha_registro") >= mes_inicio_dt),
        "prima_bruta_devengada",
    )

    vaciar_directorio("data/raw")
    vaciar_directorio("data/controles_informacion")
    vaciar_directorio("data/controles_informacion/pre_cuadre_contable")
    vaciar_directorio("data/controles_informacion/post_cuadre_contable")
    vaciar_directorio("data/controles_informacion/post_ajustes_fraude")

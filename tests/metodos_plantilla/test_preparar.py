import os
from datetime import date

import polars as pl
import pytest
import xlwings as xw
from fastapi import status
from fastapi.testclient import TestClient
from src import constantes as ct
from src import utils
from src.models import Parametros
from src.procesamiento.base_siniestros import mes_ult_ocurr_triangulos
from tests.conftest import assert_igual, vaciar_directorio
from tests.metodos_plantilla.conftest import agregar_meses_params


@pytest.mark.plantilla
@pytest.mark.integration
def test_preparar_triangulos(
    client: TestClient, rango_meses: tuple[date, date], wb_test: xw.Book
):
    params_form = {
        "negocio": "mock",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    response = client.post("/ingresar-parametros", data=params_form)
    p = Parametros.model_validate_json(response.read())

    response = client.post("/preparar-plantilla")

    assert response.status_code == status.HTTP_200_OK
    assert os.path.exists(f"plantillas/{p.nombre_plantilla}.xlsm")

    assert not wb_test.sheets["Entremes"].visible
    assert wb_test.sheets["Frecuencia"].visible
    assert wb_test.sheets["Severidad"].visible
    assert wb_test.sheets["Plata"].visible

    base_tipicos_original = pl.read_parquet(
        "data/processed/base_triangulos.parquet"
    ).filter(
        (pl.col("periodicidad_ocurrencia") == "Trimestral") & (pl.col("diagonal") == 1)
    )
    base_atipicos_original = pl.read_parquet("data/processed/base_atipicos.parquet")

    base_tipicos_plantilla = utils.sheet_to_dataframe(wb_test, "Resumen")
    base_atipicos_plantilla = utils.sheet_to_dataframe(wb_test, "Atipicos")

    assert base_tipicos_original.shape[0] == base_tipicos_plantilla.shape[0]
    for columna in ct.COLUMNAS_QTYS:
        assert_igual(base_tipicos_original, base_tipicos_plantilla, columna)

    assert base_atipicos_original.shape[0] == base_atipicos_plantilla.shape[0]
    for columna in ct.COLUMNAS_QTYS:
        assert_igual(base_atipicos_original, base_atipicos_plantilla, columna)

    vaciar_directorio("data/raw")
    vaciar_directorio("data/processed")


@pytest.mark.plantilla
@pytest.mark.integration
def test_preparar_entremes_sin_resultados_anteriores(
    client: TestClient, rango_meses: tuple[date, date]
):
    vaciar_directorio("output/resultados")

    params_form = {
        "negocio": "mock",
        "tipo_analisis": "entremes",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form)

    with pytest.raises(ValueError):
        _ = client.post("/preparar-plantilla")


@pytest.mark.plantilla
@pytest.mark.integration
def test_preparar_entremes(
    client: TestClient, wb_test: xw.Book, rango_meses: tuple[date, date]
):
    params_form = {
        "negocio": "mock",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)
    mes_corte_para_triangulo = utils.date_to_yyyymm(
        mes_ult_ocurr_triangulos(rango_meses[1], origin_grain="Trimestral")
    )
    params_form.update({"mes_corte": str(mes_corte_para_triangulo)})

    _ = client.post("/ingresar-parametros", data=params_form)
    _ = client.post("/preparar-plantilla")
    _ = client.post(
        "/modos-plantilla",
        data={
            "apertura": "01_001_A_D",
            "atributo": "bruto",
            "plantilla": "plata",
            "modo": "guardar_todo",
        },
    )

    _ = client.post("/almacenar-analisis")

    siguiente_mes = (
        mes_corte_para_triangulo + 1
        if mes_corte_para_triangulo % 100 < 12
        else (mes_corte_para_triangulo // 100) * 100 + 1
    )
    params_form.update({"tipo_analisis": "entremes", "mes_corte": str(siguiente_mes)})
    _ = client.post("/ingresar-parametros", data=params_form)
    _ = client.post("/preparar-plantilla")

    assert wb_test.sheets["Entremes"].visible
    assert not wb_test.sheets["Frecuencia"].visible
    assert not wb_test.sheets["Severidad"].visible
    assert not wb_test.sheets["Plata"].visible

    base_tipicos_original = (
        pl.read_parquet("data/processed/base_triangulos.parquet")
        .filter(
            (pl.col("periodicidad_ocurrencia") == "Trimestral")
            & (pl.col("diagonal") == 1)
        )
        .filter(
            pl.col("periodo_ocurrencia")
            != pl.col("periodo_ocurrencia").max().over("apertura_reservas")
        )
        .select(
            ["apertura_reservas", "periodicidad_ocurrencia", "periodo_ocurrencia"]
            + ct.COLUMNAS_QTYS
        )
        .vstack(
            pl.read_parquet("data/processed/base_ultima_ocurrencia.parquet")
            .filter(pl.col("periodicidad_triangulo") == "Trimestral")
            .drop("periodicidad_triangulo")
        )
    )

    base_atipicos_original = pl.read_parquet("data/processed/base_atipicos.parquet")

    base_tipicos_plantilla = utils.sheet_to_dataframe(wb_test, "Resumen")
    base_atipicos_plantilla = utils.sheet_to_dataframe(wb_test, "Atipicos")

    assert base_tipicos_original.shape[0] == base_tipicos_plantilla.shape[0]
    for columna in ct.COLUMNAS_QTYS:
        assert_igual(base_tipicos_original, base_tipicos_plantilla, columna)

    assert base_atipicos_original.shape[0] == base_atipicos_plantilla.shape[0]
    for columna in ct.COLUMNAS_QTYS:
        assert_igual(base_atipicos_original, base_atipicos_plantilla, columna)

    vaciar_directorio("data/raw")
    vaciar_directorio("data/processed")
    vaciar_directorio("data/db")
    vaciar_directorio("output/resultados")

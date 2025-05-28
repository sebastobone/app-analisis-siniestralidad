import os
from datetime import date

import polars as pl
import pytest
import xlwings as xw
from fastapi import status
from fastapi.testclient import TestClient
from src import constantes as ct
from src import utils
from src.metodos_plantilla import abrir, preparar
from src.models import Parametros
from tests.conftest import (
    agregar_meses_params,
    assert_igual,
    correr_queries,
    vaciar_directorios_test,
)


@pytest.mark.plantilla
@pytest.mark.integration
def test_preparar_triangulos(client: TestClient, rango_meses: tuple[date, date]):
    vaciar_directorios_test()

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    response = client.post("/ingresar-parametros", data=params_form).json()
    p = Parametros.model_validate(response)

    correr_queries(client)

    wb_test = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    response = client.post("/preparar-plantilla")

    assert response.status_code == status.HTTP_200_OK
    assert os.path.exists(f"plantillas/{p.nombre_plantilla}.xlsm")

    assert not wb_test.sheets["Entremes"].visible
    assert wb_test.sheets["Frecuencia"].visible
    assert wb_test.sheets["Severidad"].visible
    assert wb_test.sheets["Plata"].visible

    periodicidades = utils.obtener_aperturas("demo", "siniestros").select(
        "apertura_reservas", "periodicidad_ocurrencia"
    )

    base_siniestros_original = (
        pl.read_parquet("data/processed/base_triangulos.parquet")
        .filter(pl.col("diagonal") == 1)
        .join(
            periodicidades,
            on=["apertura_reservas", "periodicidad_ocurrencia"],
            how="inner",
        )
    )
    base_tipicos_original = base_siniestros_original.filter(pl.col("atipico") == 0)
    base_atipicos_original = base_siniestros_original.filter(pl.col("atipico") == 1)

    base_tipicos_plantilla = utils.sheet_to_dataframe(wb_test, "Resumen")
    base_atipicos_plantilla = utils.sheet_to_dataframe(wb_test, "Atipicos")

    assert base_tipicos_original.shape[0] == base_tipicos_plantilla.shape[0]
    for columna in ct.COLUMNAS_QTYS:
        assert_igual(base_tipicos_original, base_tipicos_plantilla, columna)

    assert base_atipicos_original.shape[0] == base_atipicos_plantilla.shape[0]
    for columna in ct.COLUMNAS_QTYS:
        assert_igual(base_atipicos_original, base_atipicos_plantilla, columna)

    vaciar_directorios_test()


@pytest.mark.plantilla
@pytest.mark.integration
def test_preparar_entremes_sin_resultados_anteriores(
    client: TestClient, rango_meses: tuple[date, date]
):
    vaciar_directorios_test()

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "entremes",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form)

    with pytest.raises(preparar.AnalisisAnterioresNoEncontradosError):
        _ = client.get("/obtener-analisis-anteriores")

    vaciar_directorios_test()


@pytest.mark.plantilla
@pytest.mark.integration
def test_preparar_entremes(client: TestClient, rango_meses: tuple[date, date]):
    vaciar_directorios_test()

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)
    params_form.update({"mes_corte": "202412"})

    _ = client.post("/ingresar-parametros", data=params_form)

    correr_queries(client)

    _ = client.post("/preparar-plantilla")
    _ = client.post(
        "/guardar-todo",
        data={"apertura": "01_001_A_D", "atributo": "bruto", "plantilla": "plata"},
    )

    _ = client.post("/almacenar-analisis")

    params_form.update({"tipo_analisis": "entremes", "mes_corte": "202501"})
    response = client.post("/ingresar-parametros", data=params_form).json()
    p = Parametros.model_validate(response)
    wb_test = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    correr_queries(client)

    _ = client.post(
        "/preparar-plantilla",
        data={
            "referencia_actuarial": "triangulos",
            "referencia_contable": "triangulos",
        },
    )

    assert wb_test.sheets["Entremes"].visible
    assert not wb_test.sheets["Frecuencia"].visible
    assert not wb_test.sheets["Severidad"].visible
    assert not wb_test.sheets["Plata"].visible

    validar_cifras_entremes(wb_test)

    _ = client.post("/almacenar-analisis")

    for siguiente_mes in ["202502", "202503"]:
        params_form.update({"tipo_analisis": "entremes", "mes_corte": siguiente_mes})
        _ = client.post("/ingresar-parametros", data=params_form)
        correr_queries(client)
        _ = client.post("/preparar-plantilla")
        _ = client.post("/almacenar-analisis")

    params_form.update({"tipo_analisis": "triangulos", "mes_corte": "202503"})
    _ = client.post("/ingresar-parametros", data=params_form)
    _ = client.post("/preparar-plantilla")
    _ = client.post("/guardar-todo")
    _ = client.post("/almacenar-analisis")

    params_form.update({"tipo_analisis": "entremes", "mes_corte": "202504"})
    _ = client.post("/ingresar-parametros", data=params_form)

    correr_queries(client)

    _ = client.post(
        "/preparar-plantilla",
        data={"referencia_actuarial": "triangulos", "referencia_contable": "entremes"},
    )

    _ = client.post("/almacenar-analisis")

    vaciar_directorios_test()


def validar_cifras_entremes(wb_test: xw.Book) -> None:
    periodicidades = utils.obtener_aperturas("demo", "siniestros").select(
        "apertura_reservas", "periodicidad_ocurrencia"
    )

    base_siniestros_original = (
        pl.read_parquet("data/processed/base_triangulos.parquet")
        .join(
            periodicidades,
            on=["apertura_reservas", "periodicidad_ocurrencia"],
            how="inner",
        )
        .filter(pl.col("diagonal") == 1)
        .filter(
            pl.col("periodo_ocurrencia")
            != pl.col("periodo_ocurrencia").max().over("apertura_reservas")
        )
        .select(
            [
                "apertura_reservas",
                "atipico",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
            + ct.COLUMNAS_QTYS
        )
        .vstack(
            pl.read_parquet("data/processed/base_ultima_ocurrencia.parquet")
            .join(
                periodicidades.rename(
                    {"periodicidad_ocurrencia": "periodicidad_triangulo"}
                ),
                on=["apertura_reservas", "periodicidad_triangulo"],
                how="inner",
            )
            .drop("periodicidad_triangulo")
        )
    )

    base_tipicos_original = base_siniestros_original.filter(pl.col("atipico") == 0)
    base_atipicos_original = base_siniestros_original.filter(pl.col("atipico") == 1)

    base_tipicos_plantilla = utils.sheet_to_dataframe(wb_test, "Resumen")
    base_atipicos_plantilla = utils.sheet_to_dataframe(wb_test, "Atipicos")

    assert base_tipicos_original.shape[0] == base_tipicos_plantilla.shape[0]
    for columna in ct.COLUMNAS_QTYS:
        assert_igual(base_tipicos_original, base_tipicos_plantilla, columna)

    assert base_atipicos_original.shape[0] == base_atipicos_plantilla.shape[0]
    for columna in ct.COLUMNAS_QTYS:
        assert_igual(base_atipicos_original, base_atipicos_plantilla, columna)

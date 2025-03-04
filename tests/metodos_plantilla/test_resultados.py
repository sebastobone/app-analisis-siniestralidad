import os
from datetime import date

import polars as pl
import pytest
import xlwings as xw
from fastapi import status
from fastapi.testclient import TestClient
from sqlmodel import Session
from src import utils
from src.app import obtener_parametros_usuario
from src.metodos_plantilla import abrir
from src.metodos_plantilla.guardar_traer.rangos_parametros import (
    obtener_indice_en_rango,
)
from tests.conftest import assert_diferente, assert_igual, vaciar_directorio
from tests.metodos_plantilla.conftest import agregar_meses_params


@pytest.mark.plantilla
@pytest.mark.integration
def test_actualizar_resultados(
    client: TestClient,
    client_2: TestClient,
    test_session: Session,
    rango_meses: tuple[date, date],
):
    apertura = "01_001_A_D"
    atributo = "bruto"
    apertura_2 = "01_001_A_E"

    params_form_usuario_1 = {
        "negocio": "mock",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    params_form_usuario_2 = {
        "negocio": "mock",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test_u2",
    }

    agregar_meses_params(params_form_usuario_1, rango_meses)
    agregar_meses_params(params_form_usuario_2, rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form_usuario_1)
    p1 = obtener_parametros_usuario(test_session, "test-usuario-1")
    _ = client_2.post("/ingresar-parametros", data=params_form_usuario_2)
    p2 = obtener_parametros_usuario(test_session, "test-usuario-2")

    # Usuario 1 estima apertura 1
    _ = client.post("/preparar-plantilla")
    wb_u1 = abrir.abrir_plantilla(f"plantillas/{p1.nombre_plantilla}.xlsm")

    _ = client.post(
        "/modos-plantilla",
        data={
            "apertura": apertura,
            "atributo": atributo,
            "plantilla": "plata",
            "modo": "generar",
        },
    )

    _ = client.post(
        "/modos-plantilla",
        data={
            "apertura": apertura,
            "atributo": atributo,
            "plantilla": "plata",
            "modo": "guardar",
        },
    )
    _ = client.post("/almacenar-analisis")
    wb_u1.close()

    # Usuario 2 estima apertura 1
    _ = client_2.post("/preparar-plantilla")
    wb_u2 = abrir.abrir_plantilla(f"plantillas/{p2.nombre_plantilla}.xlsm")

    _ = client_2.post(
        "/modos-plantilla",
        data={
            "apertura": apertura,
            "atributo": atributo,
            "plantilla": "plata",
            "modo": "generar",
        },
    )

    hoja_plata = wb_u2.sheets["Plata"]
    celda_metodologia = (
        obtener_indice_en_rango("metodologia", hoja_plata.range("A1:A1000")),
        2,
    )
    hoja_plata.range(celda_metodologia).value = "incurrido"

    _ = client_2.post(
        "/modos-plantilla",
        data={
            "apertura": apertura,
            "atributo": atributo,
            "plantilla": "plata",
            "modo": "guardar",
        },
    )

    # Usuario 2 estima apertura 2
    _ = client_2.post(
        "/modos-plantilla",
        data={
            "apertura": apertura_2,
            "atributo": atributo,
            "plantilla": "plata",
            "modo": "generar",
        },
    )
    _ = client_2.post(
        "/modos-plantilla",
        data={
            "apertura": apertura_2,
            "atributo": atributo,
            "plantilla": "plata",
            "modo": "guardar",
        },
    )

    _ = client_2.post("/almacenar-analisis")
    wb_u2.close()

    response = client.post("/actualizar-wb-resultados")
    assert response.status_code == status.HTTP_200_OK

    wb_res = xw.Book("output/resultados.xlsx")
    info_wb_res = utils.sheet_to_dataframe(wb_res, "Resultados")

    # Verificamos que solamente se guarden aperturas calculadas
    assert sorted(
        info_wb_res.filter(pl.col("atipico") == 0)
        .get_column("apertura_reservas")
        .unique()
        .to_list()
    ) == ["01_001_A_D", "01_001_A_E"]

    # Verificamos que, en caso de aperturas guardadas por dos usuarios,
    # se lea solamente el archivo mas reciente
    info_u1 = pl.read_parquet(
        f"output/resultados/{p1.nombre_plantilla}_{p1.mes_corte}.parquet"
    )
    info_u2 = pl.read_parquet(
        f"output/resultados/{p2.nombre_plantilla}_{p2.mes_corte}.parquet"
    )
    filtro_apertura = pl.col("apertura_reservas") == "01_001_A_D"

    assert_diferente(
        info_u1.filter(filtro_apertura),
        info_u2.filter(filtro_apertura),
        "plata_ultimate_bruto",
    )
    assert_igual(
        info_wb_res.filter((filtro_apertura) & (pl.col("mes_corte") == p2.mes_corte)),
        info_u2.filter(filtro_apertura),
        "plata_ultimate_bruto",
    )

    os.remove(f"plantillas/{p2.nombre_plantilla}.xlsm")
    wb_res.close()

    vaciar_directorio("data/raw")
    vaciar_directorio("data/processed")
    vaciar_directorio("data/db")
    vaciar_directorio("output/resultados")

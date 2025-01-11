import os

import polars as pl
import pytest
import xlwings as xw
from fastapi import status
from fastapi.testclient import TestClient
from sqlmodel import Session
from src import plantilla as plant
from src import utils
from src.app import obtener_parametros_usuario

from tests.test_plantilla import mock_informacion_cruda


@pytest.mark.plantilla
@pytest.mark.integration
def test_actualizar_resultados(
    client: TestClient,
    client_2: TestClient,
    test_session: Session,
    mock_siniestros: pl.LazyFrame,
    mock_primas: pl.LazyFrame,
    mock_expuestos: pl.LazyFrame,
):
    params_form_usuario_1 = {
        "negocio": "mock",
        "mes_inicio": "201501",
        "mes_corte": "203012",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "plantilla_test_usuario_1",
    }
    params_form_usuario_2 = {
        "negocio": "mock",
        "mes_inicio": "201501",
        "mes_corte": "203012",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "plantilla_test_usuario_2",
    }

    _ = client.post("/ingresar-parametros", data=params_form_usuario_1)
    p1 = obtener_parametros_usuario(test_session, "test-usuario-1")

    _ = client_2.post("/ingresar-parametros", data=params_form_usuario_2)
    p2 = obtener_parametros_usuario(test_session, "test-usuario-2")

    mock_informacion_cruda(mock_siniestros, mock_primas, mock_expuestos)

    _ = client.post("/preparar-plantilla")
    wb_u1 = plant.abrir_plantilla(f"plantillas/{p1.nombre_plantilla}.xlsm")

    _ = client.post("/modos-plantilla", data={"plant": "plata", "modo": "generar"})
    _ = client.post("/modos-plantilla", data={"plant": "plata", "modo": "guardar"})
    _ = client.post("/almacenar-analisis")

    _ = client_2.post("/preparar-plantilla")

    wb_u2 = plant.abrir_plantilla(f"plantillas/{p2.nombre_plantilla}.xlsm")

    _ = client_2.post("/modos-plantilla", data={"plant": "plata", "modo": "generar"})
    _ = client_2.post("/modos-plantilla", data={"plant": "plata", "modo": "guardar"})

    wb_u2.sheets["Plantilla_Plata"].range((2, 3)).value = "01_001_A_E"
    wb_u2.sheets["Plantilla_Plata"].range((4, 3)).value = "Incurrido"
    _ = client_2.post("/modos-plantilla", data={"plant": "plata", "modo": "generar"})
    _ = client_2.post("/modos-plantilla", data={"plant": "plata", "modo": "guardar"})
    _ = client_2.post("/almacenar-analisis")

    response = client.post("/actualizar-wb-resultados")
    assert response.status_code == status.HTTP_200_OK

    wb_res = xw.Book("output/resultados.xlsx")
    info_wb_res = utils.sheet_to_dataframe(wb_res, "Resultados").collect()

    # Verificamos que solamente se guarden aperturas calculadas
    assert sorted(
        info_wb_res.filter(pl.col("atipico") == 0)
        .get_column("apertura_reservas")
        .unique()
        .to_list()
    ) == [
        "01_001_A_D",
        "01_001_A_E",
    ]

    info_u2 = pl.scan_parquet(
        f"output/resultados/{p2.nombre_plantilla}_{p2.mes_corte}.parquet"
    ).collect()

    # Verificamos que, en caso de aperturas guardadas por dos usuarios,
    # se lea solamente el archivo mas reciente
    assert (
        info_wb_res.filter(pl.col("apertura_reservas") == "01_001_A_D")
        .get_column("plata_ultimate_bruto")
        .sum()
        == info_u2.filter(pl.col("apertura_reservas") == "01_001_A_D")
        .get_column("plata_ultimate_bruto")
        .sum()
    )

    wb_u1.close()
    os.remove(f"plantillas/{p1.nombre_plantilla}.xlsm")

    wb_u2.close()
    os.remove(f"plantillas/{p2.nombre_plantilla}.xlsm")

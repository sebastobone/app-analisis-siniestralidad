import os
from datetime import date

import polars as pl
import pytest
import xlwings as xw
from fastapi import status
from fastapi.testclient import TestClient
from sqlmodel import Session
from src import plantilla as plant
from src import utils
from src.app import obtener_parametros_usuario

from tests.conftest import assert_igual
from tests.test_plantilla import agregar_meses_params


@pytest.fixture(autouse=True)
def guardar_bases_ficticias(
    mock_siniestros: pl.LazyFrame,
    mock_primas: pl.LazyFrame,
    mock_expuestos: pl.LazyFrame,
) -> None:
    mock_siniestros.collect().write_parquet("data/raw/siniestros.parquet")
    mock_primas.collect().write_parquet("data/raw/primas.parquet")
    mock_expuestos.collect().write_parquet("data/raw/expuestos.parquet")


@pytest.mark.plantilla
@pytest.mark.integration
def test_actualizar_resultados(
    client: TestClient,
    client_2: TestClient,
    test_session: Session,
    mes_inicio: date,
    mes_corte: date,
):
    params_form_usuario_1 = {
        "negocio": "mock",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "plantilla_test_usuario_1",
    }
    params_form_usuario_2 = {
        "negocio": "mock",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "plantilla_test_usuario_2",
    }

    agregar_meses_params(params_form_usuario_1, mes_inicio, mes_corte)
    agregar_meses_params(params_form_usuario_2, mes_inicio, mes_corte)

    _ = client.post("/ingresar-parametros", data=params_form_usuario_1)
    p1 = obtener_parametros_usuario(test_session, "test-usuario-1")

    _ = client_2.post("/ingresar-parametros", data=params_form_usuario_2)
    p2 = obtener_parametros_usuario(test_session, "test-usuario-2")

    _ = client.post("/preparar-plantilla")
    wb_u1 = plant.abrir_plantilla(f"plantillas/{p1.nombre_plantilla}.xlsm")

    _ = client.post("/modos-plantilla", data={"plant": "plata", "modo": "generar"})
    _ = client.post("/modos-plantilla", data={"plant": "plata", "modo": "guardar"})
    _ = client.post("/almacenar-analisis")

    _ = client_2.post("/preparar-plantilla")
    wb_u2 = plant.abrir_plantilla(f"plantillas/{p2.nombre_plantilla}.xlsm")

    wb_u2.sheets["Plantilla_Plata"].range((4, 3)).value = "Incurrido"
    _ = client_2.post("/modos-plantilla", data={"plant": "plata", "modo": "generar"})
    _ = client_2.post("/modos-plantilla", data={"plant": "plata", "modo": "guardar"})

    wb_u2.sheets["Plantilla_Plata"].range((2, 3)).value = "01_001_A_E"
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
    ) == ["01_001_A_D", "01_001_A_E"]

    info_u2 = pl.read_parquet(
        f"output/resultados/{p2.nombre_plantilla}_{p2.mes_corte}.parquet"
    )

    # Verificamos que, en caso de aperturas guardadas por dos usuarios,
    # se lea solamente el archivo mas reciente
    assert_igual(
        info_wb_res.filter(
            (pl.col("apertura_reservas") == "01_001_A_D")
            & (pl.col("mes_corte") == p2.mes_corte)
        ),
        info_u2.filter(pl.col("apertura_reservas") == "01_001_A_D"),
        "plata_ultimate_bruto",
    )

    wb_u1.close()
    os.remove(f"plantillas/{p1.nombre_plantilla}.xlsm")

    wb_u2.close()
    os.remove(f"plantillas/{p2.nombre_plantilla}.xlsm")

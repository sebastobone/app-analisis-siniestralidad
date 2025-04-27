import os
from datetime import date

import polars as pl
import pytest
import xlwings as xw
from fastapi import status
from fastapi.testclient import TestClient
from src import utils
from src.metodos_plantilla import abrir
from src.metodos_plantilla.guardar_traer.rangos_parametros import (
    obtener_indice_en_rango,
)
from src.models import Parametros
from tests.conftest import (
    agregar_meses_params,
    assert_diferente,
    assert_igual,
    vaciar_directorios_test,
)


@pytest.mark.plantilla
@pytest.mark.integration
def test_actualizar_resultados(
    client: TestClient, client_2: TestClient, rango_meses: tuple[date, date]
):
    vaciar_directorios_test()

    apertura = "01_001_A_D"
    atributo = "bruto"
    apertura_2 = "01_001_A_E"

    params_form_usuario_1 = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    params_form_usuario_2 = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test_u2",
    }

    agregar_meses_params(params_form_usuario_1, rango_meses)
    agregar_meses_params(params_form_usuario_2, rango_meses)

    response1 = client.post("/ingresar-parametros", data=params_form_usuario_1).json()
    response2 = client_2.post("/ingresar-parametros", data=params_form_usuario_2).json()

    _ = client.post("/correr-query-siniestros")
    _ = client.post("/correr-query-primas")
    _ = client.post("/correr-query-expuestos")

    p1 = Parametros.model_validate(response1)
    p2 = Parametros.model_validate(response2)

    # Usuario 1 estima apertura 1
    _ = client.post("/preparar-plantilla")

    wb_u1 = abrir.abrir_plantilla(f"plantillas/{p1.nombre_plantilla}.xlsm")

    _ = client.post(
        "/generar-plantilla",
        data={"apertura": apertura, "atributo": atributo, "plantilla": "plata"},
    )

    _ = client.post(
        "/guardar-apertura",
        data={"apertura": apertura, "atributo": atributo, "plantilla": "plata"},
    )
    _ = client.post("/almacenar-analisis")
    wb_u1.close()

    # Usuario 2 estima apertura 1
    _ = client_2.post("/preparar-plantilla")
    wb_u2 = abrir.abrir_plantilla(f"plantillas/{p2.nombre_plantilla}.xlsm")

    _ = client_2.post(
        "/generar-plantilla",
        data={"apertura": apertura, "atributo": atributo, "plantilla": "plata"},
    )

    hoja_plata = wb_u2.sheets["Plata"]
    celda_metodologia = (
        obtener_indice_en_rango("metodologia", hoja_plata.range("A1:A1000")),
        2,
    )
    hoja_plata.range(celda_metodologia).value = "incurrido"

    _ = client_2.post(
        "/guardar-apertura",
        data={"apertura": apertura, "atributo": atributo, "plantilla": "plata"},
    )

    # Usuario 2 estima apertura 2
    _ = client_2.post(
        "/generar-plantilla",
        data={"apertura": apertura_2, "atributo": atributo, "plantilla": "plata"},
    )
    _ = client_2.post(
        "/guardar-apertura",
        data={"apertura": apertura_2, "atributo": atributo, "plantilla": "plata"},
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
        f"output/resultados/{p1.nombre_plantilla}_{p1.mes_corte}_{p1.tipo_analisis}.parquet"
    )
    info_u2 = pl.read_parquet(
        f"output/resultados/{p2.nombre_plantilla}_{p2.mes_corte}_{p2.tipo_analisis}.parquet"
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
    os.remove("output/resultados.xlsx")

    vaciar_directorios_test()

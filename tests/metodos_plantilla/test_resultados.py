from datetime import date
from pathlib import Path

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
from tests.conftest import assert_diferente, assert_igual, ingresar_parametros


@pytest.mark.plantilla
def test_actualizar_resultados(
    client: TestClient, client_2: TestClient, rango_meses: tuple[date, date]
):
    apertura = "01_001_A_D"
    atributo = "bruto"
    apertura_2 = "01_001_A_E"

    p1 = ingresar_parametros(
        client,
        Parametros(
            negocio="demo",
            mes_inicio=rango_meses[0],
            mes_corte=rango_meses[1],
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test",
        ),
    )
    p2 = ingresar_parametros(
        client_2,
        Parametros(
            negocio="demo",
            mes_inicio=rango_meses[0],
            mes_corte=rango_meses[1],
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test_u2",
        ),
    )

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
    mes_corte_int = utils.date_to_yyyymm(p1.mes_corte)

    info_u1 = pl.read_parquet(
        f"output/resultados/{p1.nombre_plantilla}_{mes_corte_int}_{p1.tipo_analisis}.parquet"
    )
    info_u2 = pl.read_parquet(
        f"output/resultados/{p2.nombre_plantilla}_{mes_corte_int}_{p2.tipo_analisis}.parquet"
    )

    filtro_apertura = pl.col("apertura_reservas") == "01_001_A_D"

    assert_diferente(
        info_u1.filter(filtro_apertura),
        info_u2.filter(filtro_apertura),
        "plata_ultimate_bruto",
    )
    assert_igual(
        info_wb_res.filter((filtro_apertura) & (pl.col("mes_corte") == mes_corte_int)),
        info_u2.filter(filtro_apertura),
        "plata_ultimate_bruto",
    )

    Path(f"plantillas/{p2.nombre_plantilla}.xlsm").unlink()
    wb_res.close()
    Path("output/resultados.xlsx").unlink()

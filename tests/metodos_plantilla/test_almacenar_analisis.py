import os
from datetime import date
from typing import Literal

import polars as pl
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlmodel import Session, select
from src import utils
from src.metodos_plantilla import abrir
from src.models import Parametros
from tests.conftest import assert_igual
from tests.metodos_plantilla.conftest import agregar_meses_params


@pytest.mark.plantilla
@pytest.mark.integration
@pytest.mark.parametrize(
    "params_form, plantillas",
    [
        (
            {
                "negocio": "mock",
                "tipo_analisis": "triangulos",
                "nombre_plantilla": "plantilla_test_triangulos",
            },
            ["frec", "seve"],
        ),
        (
            {
                "negocio": "mock",
                "tipo_analisis": "triangulos",
                "nombre_plantilla": "plantilla_test_triangulos",
            },
            ["plata"],
        ),
        # (
        #     {
        #         "negocio": "mock",
        #         "mes_inicio": "201501",
        #         "mes_corte": "203012",
        #         "tipo_analisis": "triangulos",
        #         "nombre_plantilla": "plantilla_test_triangulos",
        #     },
        #     ["entremes"],
        # ),
    ],
)
def test_almacenar_analisis(
    client: TestClient,
    test_session: Session,
    params_form: dict[str, str],
    plantillas: list[Literal["frec", "seve", "plata", "entremes"]],
    rango_meses: tuple[date, date],
):
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form)
    p = test_session.exec(select(Parametros)).all()[0]

    _ = client.post("/preparar-plantilla")
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    for plantilla in plantillas:
        _ = client.post(
            "/modos-plantilla", data={"plant": plantilla, "modo": "generar"}
        )
        _ = client.post(
            "/modos-plantilla", data={"plant": plantilla, "modo": "guardar"}
        )

    response = client.post("/almacenar-analisis")
    assert response.status_code == status.HTTP_200_OK
    assert os.path.exists(
        f"output/resultados/{p.nombre_plantilla}_{p.mes_corte}.parquet"
    )

    info_plantilla = utils.sheet_to_dataframe(wb, "Aux_Totales").collect()
    info_guardada = pl.read_parquet(
        f"output/resultados/{p.nombre_plantilla}_{p.mes_corte}.parquet"
    ).filter(pl.col("atipico") == 0)

    assert_igual(info_plantilla, info_guardada, "plata_ultimate_bruto")

    wb.close()
    os.remove(f"plantillas/{p.nombre_plantilla}.xlsm")
    os.remove(f"output/resultados/{p.nombre_plantilla}_{p.mes_corte}.parquet")

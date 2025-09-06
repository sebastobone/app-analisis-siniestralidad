from datetime import date

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlmodel import Session, select
from src.models import Parametros


@pytest.mark.fast
def test_generar_base(client: TestClient):
    response = client.get("/")
    assert response.status_code == status.HTTP_200_OK


@pytest.mark.fast
def test_ingresar_parametros(client: TestClient, test_session: Session):
    params_form = {
        "negocio": "demo",
        "mes_inicio": "201001",
        "mes_corte": "203012",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }

    # Dos veces para verificar que se sobreescribe
    response = client.post("/ingresar-parametros", params=params_form)
    response = client.post("/ingresar-parametros", params=params_form)

    assert response.status_code == status.HTTP_200_OK

    params_in_db = test_session.exec(select(Parametros)).all()
    assert len(params_in_db) == 1
    assert params_in_db[0].negocio == params_form["negocio"]
    assert params_in_db[0].mes_inicio == date(2010, 1, 1)


@pytest.mark.fast
@pytest.mark.parametrize(
    "mes_inicio, mes_corte, mensaje",
    [
        ("lorem", "ipsum", "Las fechas deben estar en formato YYYYMM"),
        ("201013", "203012", "El mes debe estar comprendido entre 1 y 12."),
    ],
)
def test_ingresar_parametros_malos(
    client: TestClient, mes_inicio: str, mes_corte: str, mensaje: str
):
    params_form = {
        "negocio": "autonomia",
        "mes_inicio": mes_inicio,
        "mes_corte": mes_corte,
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "plantilla1",
    }
    with pytest.raises(ValidationError) as exc:
        _ = client.post("/ingresar-parametros", params=params_form)
    assert mensaje in str(exc.value)

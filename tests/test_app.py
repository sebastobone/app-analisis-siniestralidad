import pytest
from fastapi import status
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlmodel import Session, select
from src.models import Parametros


@pytest.mark.unit
def test_generar_base(client: TestClient):
    response = client.get("/")
    assert response.status_code == status.HTTP_200_OK


@pytest.mark.unit
def test_ingresar_parametros(
    client: TestClient, test_session: Session, params_form: dict[str, str]
):
    # Dos veces para verificar que se sobreescribe
    response = client.post("/ingresar-parametros", data=params_form)
    response = client.post("/ingresar-parametros", data=params_form)

    assert response.status_code == status.HTTP_200_OK

    params_in_db = test_session.exec(select(Parametros)).all()
    assert len(params_in_db) == 1
    assert params_in_db[0].negocio == params_form["negocio"]
    assert params_in_db[0].mes_inicio == int(params_form["mes_inicio"])


@pytest.mark.unit
def test_ingresar_parametros_malos(client: TestClient, params_form: dict[str, str]):
    params_malos = params_form.copy()
    params_malos["mes_inicio"] = "lorem"
    params_malos["mes_corte"] = "ipsum"
    params_malos["aproximar_reaseguro"] = "sit"

    with pytest.raises(ValidationError):
        client.post("/ingresar-parametros", data=params_malos)

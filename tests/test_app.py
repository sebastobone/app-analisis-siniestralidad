from unittest.mock import patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlmodel import Session, select
from src.models import Parametros


def test_generar_base(client: TestClient):
    response = client.get("/")
    assert response.status_code == status.HTTP_200_OK


def test_ingresar_parametros(
    client: TestClient, test_session: Session, params_form: dict[str, str]
):
    response = client.post(
        "/ingresar-parametros",
        data=params_form,
    )

    assert response.status_code == status.HTTP_200_OK

    params_in_db = test_session.exec(select(Parametros)).all()
    assert len(params_in_db) == 1
    assert params_in_db[0].negocio == params_form["negocio"]
    assert params_in_db[0].mes_inicio == int(params_form["mes_inicio"])


def test_ingresar_parametros_malos(client: TestClient, params_form: dict[str, str]):
    params_malos = params_form.copy()
    params_malos["mes_inicio"] = "lorem"
    params_malos["mes_corte"] = "ipsum"
    params_malos["aproximar_reaseguro"] = "sit"

    with pytest.raises(ValidationError):
        client.post("/ingresar-parametros", data=params_malos)


def test_correr_query_siniestros(
    client: TestClient, test_session: Session, params: Parametros
):
    test_session.add(params)
    test_session.commit()

    with patch("src.main.correr_query_siniestros") as mock_query:
        response = client.post("/correr-query-siniestros")

        assert response.status_code == status.HTTP_200_OK

        mock_query.assert_called_once_with(params)

        params_in_db = test_session.exec(select(Parametros)).all()
        assert len(params_in_db) == 1


def test_correr_query_primas(
    client: TestClient, test_session: Session, params: Parametros
):
    test_session.add(params)
    test_session.commit()

    with patch("src.main.correr_query_primas") as mock_query:
        response = client.post("/correr-query-primas")

        assert response.status_code == status.HTTP_200_OK

        mock_query.assert_called_once_with(params)

        params_in_db = test_session.exec(select(Parametros)).all()
        assert len(params_in_db) == 1


def test_correr_query_expuestos(
    client: TestClient, test_session: Session, params: Parametros
):
    test_session.add(params)
    test_session.commit()

    with patch("src.main.correr_query_expuestos") as mock_query:
        response = client.post("/correr-query-expuestos")

        assert response.status_code == status.HTTP_200_OK

        mock_query.assert_called_once_with(params)

        params_in_db = test_session.exec(select(Parametros)).all()
        assert len(params_in_db) == 1

import pytest
from unittest.mock import patch
from fastapi import status
from fastapi.testclient import TestClient
from src.app import app, Parametros, get_session
from sqlmodel import Session, create_engine, SQLModel, select
from sqlmodel.pool import StaticPool


@pytest.fixture
def test_session():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture
def client(test_session: Session):
    def get_test_session():
        return test_session

    app.dependency_overrides[get_session] = get_test_session

    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


@pytest.fixture
def params_form() -> dict[str, str]:
    return {
        "negocio": "autonomia",
        "mes_inicio": "202301",
        "mes_corte": "202312",
        "tipo_analisis": "triangulos",
        "aproximar_reaseguro": "0",
        "nombre_plantilla": "plantilla",
    }


@pytest.fixture
def params(params_form: dict[str, str]) -> Parametros:
    return Parametros(
        negocio=params_form["negocio"],
        mes_inicio=int(params_form["mes_inicio"]),
        mes_corte=int(params_form["mes_corte"]),
        tipo_analisis=params_form["tipo_analisis"],
        aproximar_reaseguro=bool(params_form["aproximar_reaseguro"]),
        nombre_plantilla=params_form["nombre_plantilla"],
        session_id="test-session-id",
    )


def test_generar_base(client: TestClient):
    response = client.get("/")
    assert response.status_code == 200


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


def test_ingresar_parametros_malos(client: TestClient):
    params_malos = {
        "negocio": "autonomia",
        "mes_inicio": "lorem",
        "mes_corte": "ipsum",
        "tipo_analisis": "1",
        "aproximar_reaseguro": "0",
        "nombre_plantilla": "plantilla",
    }

    response = client.post("/ingresar-parametros", data=params_malos)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


def test_correr_query_siniestros(
    client: TestClient, test_session: Session, params: Parametros
):
    test_session.add(params)
    test_session.commit()

    with patch("src.main.correr_query_siniestros") as mock_query:
        response = client.post(
            "/correr-query-siniestros", cookies={"session_id": str(params.session_id)}
        )

        assert response.status_code == status.HTTP_200_OK

        mock_query.assert_called_once_with(
            params.negocio,
            params.mes_inicio,
            params.mes_corte,
            params.tipo_analisis,
            params.aproximar_reaseguro,
        )

        params_in_db = test_session.exec(select(Parametros)).all()
        assert len(params_in_db) == 1

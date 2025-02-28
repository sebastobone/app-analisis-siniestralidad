import os
import sys
from datetime import date

import numpy as np
import polars as pl
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool
from src import utils
from src.app import app, get_session

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))


@pytest.fixture
def rango_meses() -> tuple[date, date]:
    mes_inicio = date(np.random.randint(2010, 2019), np.random.randint(1, 12), 1)
    mes_corte = date(np.random.randint(2020, 2029), np.random.randint(1, 12), 1)
    return mes_inicio, mes_corte


@pytest.fixture
def mock_siniestros(rango_meses: tuple[date, date]) -> pl.LazyFrame:
    num_rows = 100000
    return pl.LazyFrame(
        {
            "codigo_op": np.random.choice(["01", "02"], size=num_rows),
            "codigo_ramo_op": np.random.choice(["001", "002", "003"], size=num_rows),
            "apertura_1": np.random.choice(["A", "B", "C"], size=num_rows),
            "apertura_2": np.random.choice(["D", "E", "F"], size=num_rows),
            "atipico": np.random.choice([0, 1], size=num_rows, p=[0.95, 0.05]),
            "fecha_siniestro": np.random.choice(
                pl.date_range(
                    rango_meses[0], rango_meses[1], interval="1mo", eager=True
                ),
                size=num_rows,
            ),
            "fecha_registro": np.random.choice(
                pl.date_range(
                    rango_meses[0], rango_meses[1], interval="1mo", eager=True
                ),
                size=num_rows,
            ),
            "pago_bruto": np.random.random(size=num_rows) * 1e8,
            "pago_retenido": np.random.random(size=num_rows) * 1e8,
            "aviso_bruto": np.random.random(size=num_rows) * 1e7,
            "aviso_retenido": np.random.random(size=num_rows) * 1e7,
            "conteo_pago": np.random.randint(0, 100, size=num_rows),
            "conteo_incurrido": np.random.randint(0, 110, size=num_rows),
            "conteo_desistido": np.random.randint(0, 10, size=num_rows),
        }
    ).with_columns(utils.crear_columna_apertura_reservas("mock"))


@pytest.fixture
def mock_primas(rango_meses: tuple[date, date]) -> pl.LazyFrame:
    num_rows = 10000
    return pl.LazyFrame(
        {
            "codigo_op": np.random.choice(["01", "02"], size=num_rows),
            "codigo_ramo_op": np.random.choice(["001", "002", "003"], size=num_rows),
            "apertura_1": np.random.choice(["A", "B", "C"], size=num_rows),
            "apertura_2": np.random.choice(["D", "E", "F"], size=num_rows),
            "fecha_registro": np.random.choice(
                pl.date_range(
                    rango_meses[0], rango_meses[1], interval="1mo", eager=True
                ),
                size=num_rows,
            ),
            "prima_bruta": np.random.random(size=num_rows) * 1e8,
            "prima_retenida": np.random.random(size=num_rows) * 1e8,
            "prima_bruta_devengada": np.random.random(size=num_rows) * 1e8,
            "prima_retenida_devengada": np.random.random(size=num_rows) * 1e8,
        }
    ).with_columns(utils.crear_columna_apertura_reservas("mock"))


@pytest.fixture
def mock_expuestos(rango_meses: tuple[date, date]) -> pl.LazyFrame:
    num_rows = 10000
    return (
        pl.LazyFrame(
            {
                "codigo_op": np.random.choice(["01", "02"], size=num_rows),
                "codigo_ramo_op": np.random.choice(
                    ["001", "002", "003"], size=num_rows
                ),
                "apertura_1": np.random.choice(["A", "B", "C"], size=num_rows),
                "apertura_2": np.random.choice(["D", "E", "F"], size=num_rows),
                "fecha_registro": np.random.choice(
                    pl.date_range(
                        rango_meses[0], rango_meses[1], interval="1mo", eager=True
                    ),
                    size=num_rows,
                ),
                "expuestos": np.random.random(size=num_rows) * 1e6,
                "vigentes": np.random.random(size=num_rows) * 1e6,
            }
        )
        .with_columns(utils.crear_columna_apertura_reservas("mock"))
        .group_by(
            [
                "apertura_reservas",
                "codigo_op",
                "codigo_ramo_op",
                "apertura_1",
                "apertura_2",
                "fecha_registro",
            ]
        )
        .mean()
    )


@pytest.fixture
def bases_ficticias(
    mock_siniestros: pl.LazyFrame,
    mock_primas: pl.LazyFrame,
    mock_expuestos: pl.LazyFrame,
) -> dict[str, pl.LazyFrame]:
    return {
        "siniestros": mock_siniestros,
        "primas": mock_primas,
        "expuestos": mock_expuestos,
    }


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

    client = TestClient(app, cookies={"session_id": "test-usuario-1"})
    yield client
    app.dependency_overrides.clear()


@pytest.fixture
def client_2(test_session: Session):
    def get_test_session():
        return test_session

    app.dependency_overrides[get_session] = get_test_session

    client = TestClient(app, cookies={"session_id": "test-usuario-2"})
    yield client
    app.dependency_overrides.clear()


def assert_igual(
    df1: pl.DataFrame, df2: pl.DataFrame, col1: str, col2: str | None = None
) -> None:
    if not col2:
        col2 = col1
    assert abs(df1.get_column(col1).sum() - df2.get_column(col2).sum()) < 100


def borrar_archivos(directorio: str) -> None:
    for file in os.listdir(directorio):
        if file != ".gitkeep":
            os.remove(directorio)

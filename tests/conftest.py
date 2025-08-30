import os
import sys
from datetime import date
from pathlib import Path
from typing import Literal

import numpy as np
import polars as pl
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool
from src import utils
from src.app import app, get_session
from src.controles_informacion.sap import consolidar_sap

from tests.configuracion import configuracion

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

CREDENCIALES_TERADATA = {
    "host": configuracion.teradata_host,
    "user": configuracion.teradata_user,
    "password": configuracion.teradata_password,
}


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
            "codigo_op": np.random.choice(["01"], size=num_rows),
            "codigo_ramo_op": np.random.choice(["001", "002"], size=num_rows),
            "apertura_1": np.random.choice(["A", "B"], size=num_rows),
            "apertura_2": np.random.choice(["D", "E"], size=num_rows),
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
            "pago_retenido": np.random.random(size=num_rows) * 1e6,
            "aviso_bruto": np.random.random(size=num_rows) * 1e7,
            "aviso_retenido": np.random.random(size=num_rows) * 1e5,
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
            "codigo_op": np.random.choice(["01"], size=num_rows),
            "codigo_ramo_op": np.random.choice(["001", "002"], size=num_rows),
            "apertura_1": np.random.choice(["A", "B"], size=num_rows),
            "apertura_2": np.random.choice(["D", "E"], size=num_rows),
            "fecha_registro": np.random.choice(
                pl.date_range(
                    rango_meses[0], rango_meses[1], interval="1mo", eager=True
                ),
                size=num_rows,
            ),
            "prima_bruta": np.random.random(size=num_rows) * 1e8,
            "prima_retenida": np.random.random(size=num_rows) * 1e7,
            "prima_bruta_devengada": np.random.random(size=num_rows) * 1e8,
            "prima_retenida_devengada": np.random.random(size=num_rows) * 1e7,
        }
    ).with_columns(utils.crear_columna_apertura_reservas("mock"))


@pytest.fixture
def mock_expuestos(rango_meses: tuple[date, date]) -> pl.LazyFrame:
    num_rows = 10000
    return (
        pl.LazyFrame(
            {
                "codigo_op": np.random.choice(["01"], size=num_rows),
                "codigo_ramo_op": np.random.choice(["001", "002"], size=num_rows),
                "apertura_1": np.random.choice(["A", "B"], size=num_rows),
                "apertura_2": np.random.choice(["D", "E"], size=num_rows),
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


def assert_diferente(
    df1: pl.DataFrame, df2: pl.DataFrame, col1: str, col2: str | None = None
) -> None:
    if not col2:
        col2 = col1
    assert not abs(df1.get_column(col1).sum() - df2.get_column(col2).sum()) < 100


def vaciar_directorio(directorio_path: str) -> None:
    directorio = Path(directorio_path)
    for file in directorio.iterdir():
        if file.is_file() and file.name != ".gitkeep":
            file.unlink()


def vaciar_directorios_test() -> None:
    vaciar_directorio("data/raw")
    vaciar_directorio("data/processed")
    vaciar_directorio("data/db")
    vaciar_directorio("output/resultados")
    vaciar_directorio("output")
    vaciar_directorio("data/controles_informacion")
    vaciar_directorio("data/controles_informacion/pre_cuadre_contable")
    vaciar_directorio("data/controles_informacion/post_cuadre_contable")
    vaciar_directorio("data/controles_informacion/post_ajustes_fraude")


def agregar_meses_params(params_form: dict[str, str], rango_meses: tuple[date, date]):
    params_form.update(
        {
            "mes_inicio": str(utils.date_to_yyyymm(rango_meses[0])),
            "mes_corte": str(utils.date_to_yyyymm(rango_meses[1])),
        }
    )


def correr_queries(client: TestClient) -> None:
    _ = client.post("/correr-query-siniestros", data=CREDENCIALES_TERADATA)
    _ = client.post("/correr-query-primas", data=CREDENCIALES_TERADATA)
    _ = client.post("/correr-query-expuestos", data=CREDENCIALES_TERADATA)


async def obtener_sap_negocio(
    negocio: str,
    file: Literal["siniestros", "primas"],
    columnas_cantidades: list[str],
    mes_corte: int,
) -> pl.DataFrame:
    lista_ramos = (
        pl.read_excel(
            f"data/segmentacion_{negocio}.xlsx",
            sheet_name=f"Aperturas_{file.capitalize()}",
        )
        .get_column("codigo_ramo_op")
        .unique()
        .to_list()
    )
    return (await consolidar_sap(negocio, columnas_cantidades, mes_corte)).filter(
        pl.col("codigo_ramo_op").is_in(lista_ramos)
    )


async def validar_cuadre(
    negocio: str,
    file: Literal["siniestros", "primas"],
    columnas_cantidades: list[str],
    mes_corte: int,
) -> None:
    meses_cuadre = pl.read_excel(
        f"data/segmentacion_{negocio}.xlsx", sheet_name=f"Meses_cuadre_{file}"
    ).with_columns(
        [pl.col(f"cuadrar_{col}").cast(pl.Boolean) for col in columnas_cantidades]
    )

    base_post_cuadre = pl.read_parquet(f"data/raw/{file}.parquet").join(
        meses_cuadre, on="fecha_registro"
    )

    sap = (
        await obtener_sap_negocio(negocio, file, columnas_cantidades, mes_corte)
    ).join(meses_cuadre, on="fecha_registro")

    base_cuadrada = base_post_cuadre.with_columns(
        [pl.col(col) * pl.col(f"cuadrar_{col}") for col in columnas_cantidades]
    )
    base_no_cuadrada = base_post_cuadre.with_columns(
        [pl.col(col) * ~pl.col(f"cuadrar_{col}") for col in columnas_cantidades]
    )
    sap_cuadres = sap.with_columns(
        [pl.col(col) * pl.col(f"cuadrar_{col}") for col in columnas_cantidades]
    )
    sap_no_cuadres = sap.with_columns(
        [pl.col(col) * ~pl.col(f"cuadrar_{col}") for col in columnas_cantidades]
    )

    for col in columnas_cantidades:
        assert_igual(base_cuadrada, sap_cuadres, col)
        assert_diferente(base_no_cuadrada, sap_no_cuadres, col)

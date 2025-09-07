import io
import os
import sys
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine, text
from sqlmodel.pool import StaticPool
from src import constantes as ct
from src import utils
from src.app import app
from src.controles_informacion.sap import consolidar_sap
from src.dependencias import get_session
from src.models import Parametros

from tests.configuracion import configuracion

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

CREDENCIALES_TERADATA = {
    "host": configuracion.teradata_host,
    "user": configuracion.teradata_user,
    "password": configuracion.teradata_password,
}

CONTENT_TYPES = {
    "csv": "text/csv",
    "txt": "text/plain",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "parquet": "application/vnd.apache.parquet",
}


@pytest.fixture
def rango_meses() -> tuple[date, date]:
    mes_inicio = date(np.random.randint(2010, 2019), np.random.randint(1, 12), 1)
    mes_corte = date(np.random.randint(2020, 2029), np.random.randint(1, 12), 1)
    return mes_inicio, mes_corte


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


@pytest.fixture(autouse=True)
def limpiar_directorios():
    vaciar_directorios_test()
    yield
    vaciar_directorios_test()


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


def vaciar_directorios_test() -> None:
    utils.vaciar_directorio("data/raw")
    utils.vaciar_directorio("data/demo")
    utils.vaciar_directorio("data/consolidado")
    utils.vaciar_directorio("data/pre_cuadre_contable")
    utils.vaciar_directorio("data/post_cuadre_contable")
    utils.vaciar_directorio("data/processed")
    utils.vaciar_directorio("data/db")
    utils.vaciar_directorio("output/resultados")
    utils.vaciar_directorio("output")
    utils.vaciar_directorio("data/controles_informacion")
    utils.vaciar_directorio("data/controles_informacion/pre_cuadre_contable")
    utils.vaciar_directorio("data/controles_informacion/post_cuadre_contable")
    utils.vaciar_directorio("data/controles_informacion/post_ajustes_fraude")
    utils.vaciar_directorio("data/carga_manual/siniestros")
    utils.vaciar_directorio("data/carga_manual/primas")
    utils.vaciar_directorio("data/carga_manual/expuestos")

    archivo_segmentacion_manual = Path("data/segmentacion_test.xlsx")
    if archivo_segmentacion_manual.exists():
        archivo_segmentacion_manual.unlink()

    # Como borramos todos los archivos, debemos borrar tambien los metadatos
    engine = create_engine(
        "sqlite:///data/database.db", connect_args={"check_same_thread": False}
    )
    with engine.connect() as connection:
        connection.execute(text("DROP TABLE IF EXISTS metadatacantidades"))
        connection.commit()


def correr_queries(client: TestClient) -> None:
    _ = client.post("/correr-query-siniestros", data=CREDENCIALES_TERADATA)
    _ = client.post("/correr-query-primas", data=CREDENCIALES_TERADATA)
    _ = client.post("/correr-query-expuestos", data=CREDENCIALES_TERADATA)


async def obtener_sap_negocio(
    negocio: str,
    file: ct.CANTIDADES_CUADRE,
    columnas_cantidades: list[str],
    mes_corte: date,
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
    file: ct.CANTIDADES_CUADRE,
    columnas_cantidades: list[str],
    mes_corte: date,
) -> None:
    meses_cuadre = pl.read_excel(
        f"data/segmentacion_{negocio}.xlsx", sheet_name=f"Meses_cuadre_{file}"
    ).with_columns(
        [pl.col(f"cuadrar_{col}").cast(pl.Boolean) for col in columnas_cantidades]
    )

    base_post_cuadre = pl.read_parquet(
        f"data/post_cuadre_contable/{file}.parquet"
    ).join(meses_cuadre, on="fecha_registro")

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


def ingresar_parametros(
    client: TestClient,
    p: Parametros,
    files: Mapping[str, tuple[str, io.BytesIO | io.BufferedReader, str]] | None = None,
) -> Parametros:
    params = {
        "negocio": p.negocio,
        "tipo_analisis": p.tipo_analisis,
        "nombre_plantilla": p.nombre_plantilla,
    }
    params.update(
        {
            "mes_inicio": str(utils.date_to_yyyymm(p.mes_inicio)),
            "mes_corte": str(utils.date_to_yyyymm(p.mes_corte)),
        }
    )
    response = client.post("/ingresar-parametros", params=params, files=files).json()
    return Parametros.model_validate(response["parametros"])

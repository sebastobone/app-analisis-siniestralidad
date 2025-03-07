from typing import Literal

import polars as pl
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine, select
from sqlmodel.pool import StaticPool
from src import utils
from src.app import app, get_session
from src.models import Parametros
from src.procesamiento.autonomia import aprox_reaseguro, segmentaciones
from tests.conftest import assert_igual

DICT_KEYS = Literal["sini_bruto", "sini_cedido", "siniestros", "primas", "expuestos"]
FILTRO_083 = (pl.col("codigo_ramo_op") == "083") & (pl.col("codigo_op") == "02")


def separar_meses_anteriores(
    df: pl.DataFrame, mes_corte: int
) -> tuple[pl.DataFrame, pl.DataFrame]:
    mes_corte_dt = utils.yyyymm_to_date(mes_corte)
    df_ant = df.filter(pl.col("fecha_registro").dt.month_start() < mes_corte_dt)
    df_ult = df.filter(pl.col("fecha_registro").dt.month_start() == mes_corte_dt)
    return df_ant, df_ult


def verificar_segmentaciones(
    df: pl.DataFrame, columna: str, valores_esperados: list[str]
):
    valores_reales = df.get_column(columna).unique().to_list()
    assert sorted(set(valores_reales)) == sorted(set(valores_esperados))


@pytest.fixture(scope="session")
def test_session_autonomia():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture(scope="session")
def client_autonomia(test_session_autonomia: Session):
    def get_test_session():
        return test_session_autonomia

    app.dependency_overrides[get_session] = get_test_session

    client = TestClient(app, cookies={"session_id": "test-autonomia"})
    yield client
    app.dependency_overrides.clear()


@pytest.fixture(scope="session")
def info_autonomia(
    client_autonomia: TestClient, test_session_autonomia: Session
) -> tuple[dict[str, pl.DataFrame], Parametros]:
    data = {
        "negocio": "autonomia",
        "mes_inicio": "202401",
        "mes_corte": "202412",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "plantilla_autonomia",
        "aproximar_reaseguro": "True",
    }

    _ = client_autonomia.post("/ingresar-parametros", data=data)
    p = test_session_autonomia.exec(select(Parametros)).all()[0]

    _ = client_autonomia.post("/correr-query-siniestros")
    _ = client_autonomia.post("/correr-query-primas")
    _ = client_autonomia.post("/correr-query-expuestos")

    info = {
        "siniestros": pl.read_parquet("data/raw/siniestros.parquet").with_columns(
            pago_cedido=pl.col("pago_bruto") - pl.col("pago_retenido"),
            aviso_cedido=pl.col("aviso_bruto") - pl.col("aviso_retenido"),
        ),
        "primas": pl.read_parquet("data/raw/primas.parquet"),
        "expuestos": pl.read_parquet("data/raw/expuestos.parquet"),
    }

    _ = client_autonomia.post("/generar-controles")

    return info, p


# @pytest.mark.autonomia
# @pytest.mark.integration
# @pytest.mark.teradata
# @pytest.mark.parametrize("col", ["pago_bruto", "aviso_bruto"])
# def test_base_incurrido_bruto(
#     info_autonomia: tuple[dict[DICT_KEYS, pl.DataFrame], Parametros],
#     col: Literal["pago_bruto", "aviso_bruto"],
# ):
#     info, _ = info_autonomia
#     assert_igual(info["sini_bruto"], info["siniestros"], col)


# @pytest.mark.autonomia
# @pytest.mark.integration
# @pytest.mark.teradata
# @pytest.mark.parametrize("col", ["pago_cedido", "aviso_cedido"])
# def test_base_incurrido_cedido(
#     info_autonomia: tuple[dict[DICT_KEYS, pl.DataFrame], Parametros],
#     col: Literal["pago_cedido", "aviso_cedido"],
# ):
#     info, p = info_autonomia
#     assert_igual(
#         separar_meses_anteriores(info["sini_cedido"], p.mes_corte)[0],
#         separar_meses_anteriores(info["siniestros"], p.mes_corte)[0],
#         col,
#     )


@pytest.mark.autonomia
@pytest.mark.integration
@pytest.mark.teradata
@pytest.mark.parametrize("col", ["pago_cedido", "aviso_cedido"])
def test_aprox_reaseguro(
    info_autonomia: tuple[dict[DICT_KEYS, pl.DataFrame], Parametros],
    col: Literal["pago_cedido", "aviso_cedido"],
):
    info, p = info_autonomia
    segm = segmentaciones.segm()
    inc_atip = aprox_reaseguro.procesar_incurridos_cedidos_atipicos(segm).collect()

    sinis_ult = separar_meses_anteriores(info["siniestros"], p.mes_corte)[1]

    assert_igual(inc_atip, sinis_ult.filter(pl.col("atipico") == 1), col)
    assert_igual(
        sinis_ult.filter(~FILTRO_083),
        segm["add_s_SAP_Sinis_Ced"].filter(~FILTRO_083),
        col,
    )


# @pytest.mark.autonomia
# @pytest.mark.integration
# @pytest.mark.teradata
# @pytest.mark.parametrize("query", ["siniestros"])
# def test_segmentaciones(
#     info_autonomia: tuple[dict[DICT_KEYS, pl.DataFrame], Parametros],
#     query: Literal["siniestros", "primas", "expuestos"],
# ):
#     info, _ = info_autonomia
#     segm = segmentaciones.segm()

#     lista_ramos = ["025", "069", "081", "083",
# "084", "086", "095", "096", "181", "AAV"]
#     lista_amparos = ["RESTO"]
#     lista_canales = ["Resto"]
#     for hoja, df in segm.items():
#         dfl = pl.DataFrame(utils.lowercase_columns(df))
#         if "Canal" in hoja:
#             lista_canales += dfl.get_column("apertura_canal_desc").unique().to_list()
#         if "Amparos" in hoja:
#             lista_amparos += dfl.get_column("apertura_amparo_desc").unique().to_list()

#     verificar_segmentaciones(info[query], "codigo_ramo_op", lista_ramos)
#     verificar_segmentaciones(info[query], "apertura_canal_desc", lista_canales)

#     if query != "primas":
#         verificar_segmentaciones(info[query], "apertura_amparo_desc", lista_amparos)

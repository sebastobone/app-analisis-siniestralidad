from datetime import date

import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import constantes as ct
from src.controles_informacion import sap
from src.informacion.carga_manual import crear_excel
from src.informacion.mocks import generar_mock
from src.models import Parametros
from tests.conftest import (
    CONTENT_TYPES,
    assert_diferente,
    assert_igual,
    crear_parquet,
    ingresar_parametros,
)


@pytest.fixture(autouse=True)
def hojas_segmentacion() -> dict[str, pl.DataFrame]:
    hojas = pl.read_excel("data/segmentacion_demo.xlsx", sheet_id=0)
    hojas["Meses_cuadre_siniestros"] = hojas["Meses_cuadre_siniestros"].with_columns(
        cuadrar_pago_bruto=1, cuadrar_aviso_retenido=1
    )
    hojas["Meses_cuadre_primas"] = hojas["Meses_cuadre_primas"].with_columns(
        cuadrar_prima_bruta=1, cuadrar_prima_retenida_devengada=1
    )
    return hojas


@pytest.fixture(autouse=True)
def parametros(
    client: TestClient,
    rango_meses: tuple[date, date],
    hojas_segmentacion: dict[str, pl.DataFrame],
):
    return ingresar_parametros(
        client,
        Parametros(
            negocio="test",
            mes_inicio=rango_meses[0],
            mes_corte=rango_meses[1],
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test",
        ),
        {
            "archivo_segmentacion": (
                "segmentacion_test.xlsx",
                crear_excel(hojas_segmentacion),
                CONTENT_TYPES["xlsx"],
            )
        },
    )


@pytest.fixture(autouse=True)
def cargar_archivos(client: TestClient, parametros: Parametros):
    rango_meses = parametros.mes_inicio, parametros.mes_corte

    df_siniestros = generar_mock(rango_meses, "siniestros", 1000)
    df_primas = generar_mock(rango_meses, "primas", 500)
    df_expuestos = generar_mock(rango_meses, "expuestos", 500)

    client.post(
        "/cargar-archivos",
        files=[
            (
                "siniestros",
                (
                    "siniestros.parquet",
                    crear_parquet(df_siniestros),
                    CONTENT_TYPES["parquet"],
                ),
            ),
            (
                "primas",
                ("primas.parquet", crear_parquet(df_primas), CONTENT_TYPES["parquet"]),
            ),
            (
                "expuestos",
                (
                    "expuestos.parquet",
                    crear_parquet(df_expuestos),
                    CONTENT_TYPES["parquet"],
                ),
            ),
        ],
    )
    return df_siniestros, df_primas, df_expuestos


@pytest.fixture(autouse=True)
def generar_controles(client: TestClient):
    client.post(
        "/generar-controles",
        json={
            "cuadrar_siniestros": True,
            "cuadrar_primas": True,
            "archivos_siniestros": ["data/carga_manual/siniestros/siniestros.parquet"],
            "archivos_primas": ["data/carga_manual/primas/primas.parquet"],
            "archivos_expuestos": ["data/carga_manual/expuestos/expuestos.parquet"],
        },
    )


async def obtener_sap(
    df: pl.DataFrame, cantidades: list[str], rango_meses: tuple[date, date]
) -> pl.DataFrame:
    periodos_cuadrados = df.select(
        ["codigo_op", "codigo_ramo_op", "fecha_registro"]
    ).unique()
    return (await sap.consolidar_sap("test", cantidades, rango_meses[1])).join(
        periodos_cuadrados, on=["codigo_op", "codigo_ramo_op", "fecha_registro"]
    )


@pytest.mark.asyncio
@pytest.mark.fast
async def test_cuadre_contable(rango_meses: tuple[date, date]) -> None:
    df_cuadrado_siniestros = pl.read_parquet(
        "data/post_cuadre_contable/siniestros.parquet"
    )
    df_sap_siniestros = await obtener_sap(
        df_cuadrado_siniestros, ct.COLUMNAS_SINIESTROS_CUADRE, rango_meses
    )

    assert_igual(df_cuadrado_siniestros, df_sap_siniestros, "pago_bruto")
    assert_diferente(df_cuadrado_siniestros, df_sap_siniestros, "pago_retenido")
    assert_diferente(df_cuadrado_siniestros, df_sap_siniestros, "aviso_bruto")
    assert_igual(df_cuadrado_siniestros, df_sap_siniestros, "aviso_retenido")

    df_cuadrado_primas = pl.read_parquet("data/post_cuadre_contable/primas.parquet")
    df_sap_primas = await obtener_sap(
        df_cuadrado_primas, list(ct.VALORES["primas"].keys()), rango_meses
    )

    assert_igual(df_cuadrado_primas, df_sap_primas, "prima_bruta")
    assert_diferente(df_cuadrado_primas, df_sap_primas, "prima_retenida")
    assert_diferente(df_cuadrado_primas, df_sap_primas, "prima_bruta_devengada")
    assert_igual(df_cuadrado_primas, df_sap_primas, "prima_retenida_devengada")

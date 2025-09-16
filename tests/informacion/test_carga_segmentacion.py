from datetime import date

import polars as pl
import pytest
from fastapi.testclient import TestClient
from src.informacion.carga_manual import crear_excel
from src.models import Parametros
from tests.conftest import CONTENT_TYPES, ingresar_parametros


def validar_excepciones(
    client: TestClient,
    rango_meses: tuple[date, date],
    hojas: dict[str, pl.DataFrame],
    mensajes: list[str],
) -> None:
    with pytest.raises(ValueError) as exc:
        ingresar_parametros(
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
                    crear_excel(hojas),
                    CONTENT_TYPES["xlsx"],
                )
            },
        )
    for mensaje in mensajes:
        assert mensaje in str(exc.value)


@pytest.mark.fast
def test_hojas_faltantes(client: TestClient, rango_meses: tuple[date, date]):
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame({"a": 1}),
        "Aperturas_Primas": pl.DataFrame({"b": 1}),
    }
    validar_excepciones(
        client,
        rango_meses,
        hojas,
        [
            "Faltan las siguientes hojas en el archivo de segmentacion: ",
            "{'Aperturas_Expuestos'}",
        ],
    )


@pytest.mark.fast
def test_columnas_faltantes(client: TestClient, rango_meses: tuple[date, date]):
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_040"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["040"],
                "periodicidad_ocurrencia": ["Mensual"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
    }
    validar_excepciones(
        client,
        rango_meses,
        hojas,
        ["tipo_indexacion_severidad", "Aperturas_Siniestros"],
    )


@pytest.mark.fast
def test_valores_nulos(client: TestClient, rango_meses: tuple[date, date]):
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_040"],
                "codigo_op": ["01"],
                "codigo_ramo_op": [None],
                "periodicidad_ocurrencia": ["Mensual"],
                "tipo_indexacion_severidad": ["Ninguna"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
    }
    validar_excepciones(client, rango_meses, hojas, ["valores nulos"])


@pytest.mark.fast
def test_aperturas_duplicadas(client: TestClient, rango_meses: tuple[date, date]):
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_040", "01_040"],
                "codigo_op": ["01", "01"],
                "codigo_ramo_op": ["040", "040"],
                "periodicidad_ocurrencia": ["Mensual", "Mensual"],
                "tipo_indexacion_severidad": ["Ninguna", "Ninguna"],
                "medida_indexacion_severidad": ["Ninguna", "Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
    }
    validar_excepciones(client, rango_meses, hojas, ["aperturas duplicadas"])


@pytest.mark.fast
def test_periodicidades_invalidas(client: TestClient, rango_meses: tuple[date, date]):
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_040"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["040"],
                "periodicidad_ocurrencia": ["Otro"],
                "tipo_indexacion_severidad": ["Ninguna"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
    }
    validar_excepciones(
        client, rango_meses, hojas, ["valores invalidos", "periodicidad_ocurrencia"]
    )


@pytest.mark.fast
def test_tipos_indexacion_invalidos(client: TestClient, rango_meses: tuple[date, date]):
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_040"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["040"],
                "periodicidad_ocurrencia": ["Mensual"],
                "tipo_indexacion_severidad": ["Por fecha de atencion"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
    }
    validar_excepciones(
        client,
        rango_meses,
        hojas,
        ["valores invalidos", "tipo_indexacion_severidad"],
    )


@pytest.mark.fast
def test_medidas_indexacion_invalidas(
    client: TestClient, rango_meses: tuple[date, date]
):
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_040"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["040"],
                "periodicidad_ocurrencia": ["Mensual"],
                "tipo_indexacion_severidad": ["Por fecha de ocurrencia"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
    }
    validar_excepciones(
        client,
        rango_meses,
        hojas,
        ["medida_indexacion_severidad", "no puede ser"],
    )


@pytest.mark.fast
def test_variables_apertura_sobrantes(
    client: TestClient, rango_meses: tuple[date, date]
):
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_040"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["040"],
                "periodicidad_ocurrencia": ["Mensual"],
                "tipo_indexacion_severidad": ["Ninguna"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"], "apertura_1": ["A"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
    }
    validar_excepciones(
        client,
        rango_meses,
        hojas,
        ["Sobrantes:", "apertura_1", "Aperturas_Primas"],
    )


@pytest.mark.fast
def test_cruces_nulos_aperturas(client: TestClient, rango_meses: tuple[date, date]):
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_040_A", "01_041_A"],
                "codigo_op": ["01", "01"],
                "codigo_ramo_op": ["040", "041"],
                "apertura_1": ["A", "A"],
                "periodicidad_ocurrencia": ["Mensual", "Mensual"],
                "tipo_indexacion_severidad": ["Ninguna", "Ninguna"],
                "medida_indexacion_severidad": ["Ninguna", "Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["040"]}
        ),
    }
    validar_excepciones(
        client,
        rango_meses,
        hojas,
        ["no cruzan", "Aperturas_Primas"],
    )


@pytest.mark.fast
def test_archivo_correcto(client: TestClient, rango_meses: tuple[date, date]):
    ingresar_parametros(
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
                open("data/segmentacion_demo.xlsx", "rb"),
                CONTENT_TYPES["xlsx"],
            )
        },
    )

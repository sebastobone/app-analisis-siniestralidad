import io
from datetime import date

import polars as pl
import pytest
import xlsxwriter
from fastapi.testclient import TestClient
from tests.conftest import CONTENT_TYPES, ingresar_parametros


def crear_excel(hojas: dict[str, pl.DataFrame]) -> io.BytesIO:
    excel_buffer = io.BytesIO()

    with xlsxwriter.Workbook(excel_buffer) as writer:
        for hoja in list(hojas.keys()):
            hojas[hoja].write_excel(writer, worksheet=hoja)

    excel_buffer.seek(0)
    return excel_buffer


@pytest.mark.unit
def test_hojas_faltantes(client: TestClient, rango_meses: tuple[date, date]):
    ingresar_parametros(client, rango_meses, "test")
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame({"a": 1}),
        "Aperturas_Primas": pl.DataFrame({"b": 1}),
    }
    with pytest.raises(ValueError):
        _ = client.post(
            "/cargar-archivos",
            files={
                "segmentacion": (
                    "segmentacion_test.xlsx",
                    crear_excel(hojas),
                    CONTENT_TYPES["xlsx"],
                )
            },
        )


@pytest.mark.unit
def test_columnas_faltantes(client: TestClient, rango_meses: tuple[date, date]):
    ingresar_parametros(client, rango_meses, "test")
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["001"],
                "periodicidad_ocurrencia": ["Mensual"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files={
                "segmentacion": (
                    "segmentacion_test.xlsx",
                    crear_excel(hojas),
                    CONTENT_TYPES["xlsx"],
                )
            },
        )
    assert "tipo_indexacion_severidad" in str(exc.value)
    assert "Aperturas_Siniestros" in str(exc.value)


@pytest.mark.unit
def test_aperturas_duplicadas(client: TestClient, rango_meses: tuple[date, date]):
    ingresar_parametros(client, rango_meses, "test")
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001", "01_001"],
                "codigo_op": ["01", "01"],
                "codigo_ramo_op": ["001", "001"],
                "periodicidad_ocurrencia": ["Mensual", "Mensual"],
                "tipo_indexacion_severidad": ["Ninguna", "Ninguna"],
                "medida_indexacion_severidad": ["Ninguna", "Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files={
                "segmentacion": (
                    "segmentacion_test.xlsx",
                    crear_excel(hojas),
                    CONTENT_TYPES["xlsx"],
                )
            },
        )
    assert "aperturas duplicadas" in str(exc.value)


@pytest.mark.unit
def test_periodicidades_invalidas(client: TestClient, rango_meses: tuple[date, date]):
    ingresar_parametros(client, rango_meses, "test")
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["001"],
                "periodicidad_ocurrencia": ["Otro"],
                "tipo_indexacion_severidad": ["Ninguna"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files={
                "segmentacion": (
                    "segmentacion_test.xlsx",
                    crear_excel(hojas),
                    CONTENT_TYPES["xlsx"],
                )
            },
        )
    assert "valores invalidos" in str(exc.value)
    assert "periodicidad_ocurrencia" in str(exc.value)


@pytest.mark.unit
def test_tipos_indexacion_invalidos(client: TestClient, rango_meses: tuple[date, date]):
    ingresar_parametros(client, rango_meses, "test")
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["001"],
                "periodicidad_ocurrencia": ["Mensual"],
                "tipo_indexacion_severidad": ["Por fecha de atencion"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files={
                "segmentacion": (
                    "segmentacion_test.xlsx",
                    crear_excel(hojas),
                    CONTENT_TYPES["xlsx"],
                )
            },
        )
    assert "valores invalidos" in str(exc.value)
    assert "tipo_indexacion_severidad" in str(exc.value)


@pytest.mark.unit
def test_medidas_indexacion_invalidas(
    client: TestClient, rango_meses: tuple[date, date]
):
    ingresar_parametros(client, rango_meses, "test")
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["001"],
                "periodicidad_ocurrencia": ["Mensual"],
                "tipo_indexacion_severidad": ["Por fecha de ocurrencia"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files={
                "segmentacion": (
                    "segmentacion_test.xlsx",
                    crear_excel(hojas),
                    CONTENT_TYPES["xlsx"],
                )
            },
        )
    assert "medida_indexacion_severidad" in str(exc.value)
    assert "no puede ser" in str(exc.value)


@pytest.mark.unit
def test_variables_apertura_sobrantes(
    client: TestClient, rango_meses: tuple[date, date]
):
    ingresar_parametros(client, rango_meses, "test")
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["001"],
                "periodicidad_ocurrencia": ["Mensual"],
                "tipo_indexacion_severidad": ["Ninguna"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"], "apertura_1": ["A"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files={
                "segmentacion": (
                    "segmentacion_test.xlsx",
                    crear_excel(hojas),
                    CONTENT_TYPES["xlsx"],
                )
            },
        )
    assert "Sobrantes:" in str(exc.value)
    assert "apertura_1" in str(exc.value)
    assert "Aperturas_Primas" in str(exc.value)


@pytest.mark.unit
def test_cruces_nulos_aperturas(client: TestClient, rango_meses: tuple[date, date]):
    ingresar_parametros(client, rango_meses, "test")
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001_A", "01_002_A"],
                "codigo_op": ["01", "01"],
                "codigo_ramo_op": ["001", "002"],
                "apertura_1": ["A", "A"],
                "periodicidad_ocurrencia": ["Mensual", "Mensual"],
                "tipo_indexacion_severidad": ["Ninguna", "Ninguna"],
                "medida_indexacion_severidad": ["Ninguna", "Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files={
                "segmentacion": (
                    "segmentacion_test.xlsx",
                    crear_excel(hojas),
                    CONTENT_TYPES["xlsx"],
                )
            },
        )
    assert "no cruzan" in str(exc.value)
    assert "Aperturas_Primas" in str(exc.value)


@pytest.mark.unit
def test_archivo_correcto(client: TestClient, rango_meses: tuple[date, date]):
    ingresar_parametros(client, rango_meses)
    _ = client.post(
        "/cargar-archivos",
        files={
            "segmentacion": (
                "segmentacion_demo.xlsx",
                open("data/segmentacion_demo.xlsx", "rb"),
                CONTENT_TYPES["xlsx"],
            )
        },
    )

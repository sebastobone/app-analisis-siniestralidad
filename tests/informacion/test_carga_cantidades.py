import io
from datetime import date
from pathlib import Path
from typing import Literal

import polars as pl
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from tests.conftest import (
    CONTENT_TYPES,
    ingresar_parametros,
    vaciar_directorios_test,
)
from tests.informacion.test_carga_segmentacion import crear_excel

CARGA_SEGMENTACION = {
    "archivo_segmentacion": (
        "segmentacion_test.xlsx",
        crear_excel(
            {
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
                    {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
                ),
                "Aperturas_Expuestos": pl.DataFrame(
                    {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
                ),
            }
        ),
        CONTENT_TYPES["xlsx"],
    ),
}

PRIMAS_BASICO = pl.DataFrame(
    {
        "codigo_op": ["01"],
        "codigo_ramo_op": ["001"],
        "fecha_registro": ["2024-01-01"],
        "prima_bruta": [1],
        "prima_retenida": [1],
        "prima_bruta_devengada": [1],
        "prima_retenida_devengada": [1],
    }
)


def crear_csv(df: pl.DataFrame, separador: str = ",") -> io.BytesIO:
    str_buffer = io.StringIO()
    df.write_csv(str_buffer, separator=separador)
    csv_buffer = io.BytesIO(str_buffer.getvalue().encode())
    csv_buffer.seek(0)
    return csv_buffer


def crear_parquet(df: pl.DataFrame) -> io.BytesIO:
    parquet_buffer = io.BytesIO()
    df.write_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    return parquet_buffer


def validar_carga(filename: str) -> None:
    extension = filename.split(".")[-1]
    filename_guardado = filename.replace(extension, "parquet")

    assert Path(f"data/carga_manual/primas/{filename_guardado}").exists()
    df_cargado = pl.read_parquet(f"data/carga_manual/primas/{filename_guardado}")
    assert df_cargado.shape[1] > 1
    assert df_cargado.get_column("codigo_op").item(0) == "01"
    assert df_cargado.get_column("fecha_registro").dtype.is_temporal()
    assert df_cargado.get_column("prima_bruta").dtype.is_numeric()


@pytest.mark.fast
@pytest.mark.parametrize("separador", [";", ",", "\t", "|"])
def test_cargar_multiples(
    client: TestClient,
    rango_meses: tuple[date, date],
    separador: Literal[";", ",", "\t", "|"],
):
    vaciar_directorios_test()
    _ = ingresar_parametros(client, rango_meses, "test", CARGA_SEGMENTACION)

    df = PRIMAS_BASICO
    hojas = {"Primas": PRIMAS_BASICO}

    _ = client.post(
        "/cargar-archivos",
        files=[
            (
                "primas",
                ("primas_csv.csv", crear_csv(df, separador), CONTENT_TYPES["csv"]),
            ),
            (
                "primas",
                ("primas_txt.txt", crear_csv(df, separador), CONTENT_TYPES["txt"]),
            ),
            (
                "primas",
                ("primas_xlsx.xlsx", crear_excel(hojas), CONTENT_TYPES["xlsx"]),
            ),
            (
                "primas",
                ("primas_parquet.parquet", crear_parquet(df), CONTENT_TYPES["parquet"]),
            ),
        ],
    )

    validar_carga("primas_csv.csv")
    validar_carga("primas_txt.txt")
    validar_carga("primas_xlsx.xlsx")
    validar_carga("primas_parquet.parquet")

    vaciar_directorios_test()


@pytest.mark.fast
def test_excel_varias_hojas(client: TestClient, rango_meses: tuple[date, date]):
    vaciar_directorios_test()
    _ = ingresar_parametros(client, rango_meses, "test", CARGA_SEGMENTACION)

    hojas = {"Primas": PRIMAS_BASICO, "Hoja_extra": pl.DataFrame({"codigo_op": ["01"]})}

    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files=[
                ("primas", ("primas.xlsx", crear_excel(hojas), CONTENT_TYPES["xlsx"])),
            ],
        )
    assert "tiene mas de una hoja" in str(exc.value)

    vaciar_directorios_test()


@pytest.mark.fast
def test_columnas_faltantes(client: TestClient, rango_meses: tuple[date, date]):
    vaciar_directorios_test()
    _ = ingresar_parametros(client, rango_meses, "test", CARGA_SEGMENTACION)

    df = PRIMAS_BASICO.drop("codigo_op")

    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files=[
                ("primas", ("primas.csv", crear_csv(df), CONTENT_TYPES["csv"])),
            ],
        )
    assert "faltan las siguientes columnas" in str(exc.value)

    vaciar_directorios_test()


@pytest.mark.fast
def test_valores_nulos(client: TestClient, rango_meses: tuple[date, date]):
    vaciar_directorios_test()
    _ = ingresar_parametros(client, rango_meses, "test", CARGA_SEGMENTACION)

    df = PRIMAS_BASICO.vstack(PRIMAS_BASICO.with_columns(codigo_op=pl.lit(None)))

    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files=[
                ("primas", ("primas.csv", crear_csv(df), CONTENT_TYPES["csv"])),
            ],
        )
    assert "valores nulos" in str(exc.value)

    vaciar_directorios_test()


@pytest.mark.fast
def test_aperturas_faltantes(client: TestClient, rango_meses: tuple[date, date]):
    vaciar_directorios_test()
    _ = ingresar_parametros(client, rango_meses, "test", CARGA_SEGMENTACION)

    df = PRIMAS_BASICO.vstack(PRIMAS_BASICO.with_columns(codigo_op=pl.lit("02")))

    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files=[
                ("primas", ("primas.csv", crear_csv(df), CONTENT_TYPES["csv"])),
            ],
        )
    assert "Agregue estas aperturas" in str(exc.value)

    vaciar_directorios_test()


@pytest.mark.fast
@pytest.mark.parametrize(
    "columna_mod",
    [
        pl.lit(202401).alias("fecha_registro"),
        pl.lit(2.2).alias("conteo_pago"),
        pl.lit("abc").alias("pago_bruto"),
    ],
)
def test_tipos_datos_malos(
    client: TestClient, rango_meses: tuple[date, date], columna_mod: pl.Expr
):
    vaciar_directorios_test()
    _ = ingresar_parametros(client, rango_meses, "test", CARGA_SEGMENTACION)

    # En csv, txt, xlsx se validan los tipos de datos al leer el archivo
    df = PRIMAS_BASICO.with_columns(columna_mod)
    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files=[
                ("siniestros", ("siniestros.csv", crear_csv(df), CONTENT_TYPES["csv"])),
            ],
        )
    assert "error al leer el archivo" in str(exc.value)
    assert "could not parse" in str(exc.value)

    # En parquet se validan los tipos de datos antes de agrupar y guardar
    df_parquet = PRIMAS_BASICO.with_columns(prima_bruta=pl.lit("abc"))
    with pytest.raises(ValueError) as exc:
        _ = client.post(
            "/cargar-archivos",
            files=[
                (
                    "primas",
                    (
                        "primas.parquet",
                        crear_parquet(df_parquet),
                        CONTENT_TYPES["parquet"],
                    ),
                ),
            ],
        )
    assert "error al transformar los tipos de datos" in str(exc.value)
    assert "conversion from `str` to `f64` failed" in str(exc.value)

    vaciar_directorios_test()


@pytest.mark.fast
def test_agrupar_columnas_relevantes(
    client: TestClient, rango_meses: tuple[date, date]
):
    vaciar_directorios_test()
    _ = ingresar_parametros(client, rango_meses, "test", CARGA_SEGMENTACION)

    df = PRIMAS_BASICO.with_columns(descuento=pl.lit("Si"))

    _ = client.post(
        "/cargar-archivos",
        files=[
            ("primas", ("primas.csv", crear_csv(df), CONTENT_TYPES["csv"])),
        ],
    )

    df_cargado = pl.read_parquet("data/carga_manual/primas/primas.parquet")
    assert "descuento" not in df_cargado.collect_schema().names()

    vaciar_directorios_test()


@pytest.mark.fast
def test_eliminar_archivos(client: TestClient):
    vaciar_directorios_test()

    df = pl.DataFrame({"codigo_op": ["01"], "fecha_registro": ["2024-01-01"]})
    paths: list[Path] = []
    for cantidad in ["siniestros", "primas", "expuestos"]:
        path = Path(f"data/carga_manual/{cantidad}/{cantidad}.parquet")
        paths.append(path)
        df.write_parquet(path)

    _ = client.post("/eliminar-archivos-cargados")

    for path in paths:
        assert not path.exists()

    vaciar_directorios_test()


@pytest.mark.fast
def test_descargar_ejemplos(client: TestClient):
    response = client.get("/descargar-ejemplos-cantidades")
    assert response.status_code == status.HTTP_200_OK

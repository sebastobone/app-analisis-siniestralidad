import io
from datetime import date
from pathlib import Path
from typing import Literal

import polars as pl
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from src import constantes as ct
from src.informacion.carga_manual import crear_excel
from src.informacion.mocks import generar_mock
from src.models import Parametros
from tests.conftest import CONTENT_TYPES, assert_igual, ingresar_parametros

SINIESTROS_BASICO = pl.DataFrame(
    {
        "codigo_op": ["01"],
        "codigo_ramo_op": ["001"],
        "apertura_1": ["A"],
        "apertura_2": ["D"],
        "atipico": [0],
        "fecha_siniestro": [date(2024, 1, 1)],
        "fecha_registro": [date(2024, 1, 1)],
        "pago_bruto": [1.0],
        "pago_retenido": [1.0],
        "aviso_bruto": [1.0],
        "aviso_retenido": [1.0],
        "conteo_pago": [1],
        "conteo_incurrido": [1],
        "conteo_desistido": [1],
    }
)


@pytest.fixture(autouse=True)
def params(client: TestClient, rango_meses: tuple[date, date]) -> Parametros:
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
                open("data/segmentacion_demo.xlsx", "rb"),
                CONTENT_TYPES["xlsx"],
            ),
        },
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


def validar_carga(
    filename: str, rango_meses: tuple[date, date], df: pl.DataFrame
) -> None:
    extension = filename.split(".")[-1]
    filename_guardado = filename.replace(extension, "parquet")

    assert Path(f"data/carga_manual/siniestros/{filename_guardado}").exists()
    df_cargado = pl.read_parquet(f"data/carga_manual/siniestros/{filename_guardado}")

    assert df_cargado.shape[1] > 1
    assert df_cargado.get_column("codigo_op").item(0) == "01"
    assert df_cargado.get_column("fecha_registro").dtype.is_temporal()
    assert df_cargado.get_column("pago_bruto").dtype.is_float()
    assert df_cargado.get_column("fecha_siniestro").is_between(*rango_meses).all()
    assert df_cargado.get_column("fecha_registro").is_between(*rango_meses).all()

    mes_corte = pl.date(rango_meses[1].year, rango_meses[1].month, 1).dt.month_end()

    assert_igual(
        df.filter(
            (pl.col("fecha_siniestro") <= mes_corte)
            & (pl.col("fecha_registro") <= mes_corte)
        ),
        df_cargado,
        "pago_bruto",
    )


@pytest.mark.fast
@pytest.mark.parametrize("separador", [";", ",", "\t", "|"])
def test_cargar_multiples(
    client: TestClient,
    rango_meses: tuple[date, date],
    separador: Literal[";", ",", "\t", "|"],
):
    df = generar_mock(rango_meses, "siniestros", 1000)
    hojas = {"Siniestros": df}

    _ = client.post(
        "/cargar-archivos",
        files=[
            (
                "siniestros",
                ("siniestros_csv.csv", crear_csv(df, separador), CONTENT_TYPES["csv"]),
            ),
            (
                "siniestros",
                ("siniestros_txt.txt", crear_csv(df, separador), CONTENT_TYPES["txt"]),
            ),
            (
                "siniestros",
                ("siniestros_xlsx.xlsx", crear_excel(hojas), CONTENT_TYPES["xlsx"]),
            ),
            (
                "siniestros",
                (
                    "siniestros_parquet.parquet",
                    crear_parquet(df),
                    CONTENT_TYPES["parquet"],
                ),
            ),
        ],
    )

    validar_carga("siniestros_csv.csv", rango_meses, df)
    validar_carga("siniestros_txt.txt", rango_meses, df)
    validar_carga("siniestros_xlsx.xlsx", rango_meses, df)
    validar_carga("siniestros_parquet.parquet", rango_meses, df)


@pytest.mark.fast
def test_excel_varias_hojas(client: TestClient):
    hojas = {
        "Siniestros": SINIESTROS_BASICO,
        "Hoja_extra": pl.DataFrame({"codigo_op": ["01"]}),
    }

    with pytest.raises(ValueError, match="tiene mas de una hoja"):
        _ = client.post(
            "/cargar-archivos",
            files=[
                (
                    "siniestros",
                    ("siniestros.xlsx", crear_excel(hojas), CONTENT_TYPES["xlsx"]),
                ),
            ],
        )


@pytest.mark.fast
def test_columnas_faltantes(client: TestClient):
    df = SINIESTROS_BASICO.drop("codigo_op")

    with pytest.raises(ValueError, match="faltan las siguientes columnas"):
        _ = client.post(
            "/cargar-archivos",
            files=[
                ("siniestros", ("siniestros.csv", crear_csv(df), CONTENT_TYPES["csv"])),
            ],
        )


@pytest.mark.fast
def test_valores_nulos(client: TestClient):
    df = SINIESTROS_BASICO.vstack(
        SINIESTROS_BASICO.with_columns(codigo_op=pl.lit(None))
    )

    with pytest.raises(ValueError, match="valores nulos"):
        _ = client.post(
            "/cargar-archivos",
            files=[
                ("siniestros", ("siniestros.csv", crear_csv(df), CONTENT_TYPES["csv"])),
            ],
        )


@pytest.mark.fast
def test_aperturas_faltantes(client: TestClient):
    df = SINIESTROS_BASICO.vstack(
        SINIESTROS_BASICO.with_columns(codigo_op=pl.lit("02"))
    )

    with pytest.raises(ValueError, match="Agregue estas aperturas"):
        _ = client.post(
            "/cargar-archivos",
            files=[
                ("siniestros", ("siniestros.csv", crear_csv(df), CONTENT_TYPES["csv"])),
            ],
        )


@pytest.mark.fast
@pytest.mark.parametrize(
    "columna_mod",
    [
        pl.lit(202401).alias("fecha_registro"),
        pl.lit(2.2).alias("conteo_pago"),
        pl.lit("abc").alias("pago_bruto"),
    ],
)
def test_tipos_datos_malos(client: TestClient, columna_mod: pl.Expr):
    # En csv, txt, xlsx se validan los tipos de datos al leer el archivo
    df = SINIESTROS_BASICO.with_columns(columna_mod)
    with pytest.raises(ValueError, match="error al leer el archivo"):
        _ = client.post(
            "/cargar-archivos",
            files=[
                ("siniestros", ("siniestros.csv", crear_csv(df), CONTENT_TYPES["csv"])),
            ],
        )

    # En parquet se validan los tipos de datos antes de agrupar y guardar
    df_parquet = SINIESTROS_BASICO.with_columns(pago_bruto=pl.lit("abc"))
    with pytest.raises(ValueError, match="error al transformar los tipos de datos"):
        _ = client.post(
            "/cargar-archivos",
            files=[
                (
                    "siniestros",
                    (
                        "siniestros.parquet",
                        crear_parquet(df_parquet),
                        CONTENT_TYPES["parquet"],
                    ),
                ),
            ],
        )


@pytest.mark.fast
def test_agrupar_columnas_relevantes(
    client: TestClient, rango_meses: tuple[date, date]
):
    df = generar_mock(rango_meses, "siniestros", 1000).with_columns(pj=pl.lit("Si"))

    _ = client.post(
        "/cargar-archivos",
        files=[
            ("siniestros", ("siniestros.csv", crear_csv(df), CONTENT_TYPES["csv"])),
        ],
    )

    df_cargado = pl.read_parquet("data/carga_manual/siniestros/siniestros.parquet")
    assert "pj" not in df_cargado.collect_schema().names()


@pytest.mark.fast
def test_eliminar_archivos(client: TestClient):
    df = pl.DataFrame({"codigo_op": ["01"], "fecha_registro": ["2024-01-01"]})
    paths: list[Path] = []
    for cantidad in ct.LISTA_CANTIDADES:
        path = Path(f"data/carga_manual/{cantidad}/{cantidad}.parquet")
        paths.append(path)
        df.write_parquet(path)

    _ = client.post("/eliminar-archivos-cargados")

    for path in paths:
        assert not path.exists()


@pytest.mark.fast
def test_descargar_ejemplos(client: TestClient):
    response = client.get("/descargar-ejemplos-cantidades")
    assert response.status_code == status.HTTP_200_OK

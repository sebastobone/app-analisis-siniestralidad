from datetime import date
from pathlib import Path

import polars as pl
import pytest
import xlwings as xw
from fastapi import status
from fastapi.testclient import TestClient
from src import constantes as ct
from src import utils
from src.metodos_plantilla import abrir
from src.metodos_plantilla.guardar_traer.rangos_parametros import (
    obtener_indice_en_rango,
)
from src.models import Parametros
from tests.conftest import assert_igual, ingresar_parametros


@pytest.mark.plantilla
def test_preparar_triangulos(client: TestClient, rango_meses: tuple[date, date]):
    p = ingresar_parametros(
        client,
        Parametros(
            negocio="demo",
            mes_inicio=rango_meses[0],
            mes_corte=rango_meses[1],
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test",
        ),
    )

    wb_test = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    response = client.post("/preparar-plantilla")

    assert response.status_code == status.HTTP_200_OK
    assert Path(f"plantillas/{p.nombre_plantilla}.xlsm").exists()

    assert not wb_test.sheets["Entremes"].visible
    assert wb_test.sheets["Frecuencia"].visible
    assert wb_test.sheets["Severidad"].visible
    assert wb_test.sheets["Plata"].visible

    periodicidades = utils.obtener_aperturas("demo", "siniestros").select(
        "apertura_reservas", "periodicidad_ocurrencia"
    )

    base_siniestros_original = (
        pl.read_parquet("data/processed/base_triangulos.parquet")
        .filter(pl.col("diagonal") == 1)
        .join(
            periodicidades,
            on=["apertura_reservas", "periodicidad_ocurrencia"],
            how="inner",
        )
    )
    base_tipicos_original = base_siniestros_original.filter(pl.col("atipico") == 0)
    base_atipicos_original = base_siniestros_original.filter(pl.col("atipico") == 1)

    base_tipicos_plantilla = utils.sheet_to_dataframe(wb_test, "Resumen")
    base_atipicos_plantilla = utils.sheet_to_dataframe(wb_test, "Atipicos")

    assert base_tipicos_original.shape[0] == base_tipicos_plantilla.shape[0]
    for columna in ct.COLUMNAS_QTYS:
        assert_igual(base_tipicos_original, base_tipicos_plantilla, columna)

    assert base_atipicos_original.shape[0] == base_atipicos_plantilla.shape[0]
    for columna in ct.COLUMNAS_QTYS:
        assert_igual(base_atipicos_original, base_atipicos_plantilla, columna)


@pytest.mark.fast
def test_preparar_entremes_sin_resultados_anteriores(
    client: TestClient, rango_meses: tuple[date, date]
):
    with pytest.raises(ValueError, match="No se encontraron resultados anteriores"):
        _ = ingresar_parametros(
            client,
            Parametros(
                negocio="demo",
                mes_inicio=rango_meses[0],
                mes_corte=rango_meses[1],
                tipo_analisis="entremes",
                nombre_plantilla="wb_test",
            ),
        )


@pytest.mark.plantilla
def test_preparar_entremes(client: TestClient, rango_meses: tuple[date, date]):
    _ = ingresar_parametros(
        client,
        Parametros(
            negocio="demo",
            mes_inicio=rango_meses[0],
            mes_corte=date(2024, 12, 1),
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test",
        ),
    )

    _ = client.post("/preparar-plantilla")
    _ = client.post(
        "/guardar-todo",
        data={"apertura": "01_001_A_D", "atributo": "bruto", "plantilla": "plata"},
    )

    _ = client.post("/almacenar-analisis")

    p = ingresar_parametros(
        client,
        Parametros(
            negocio="demo",
            mes_inicio=rango_meses[0],
            mes_corte=date(2025, 1, 1),
            tipo_analisis="entremes",
            nombre_plantilla="wb_test",
        ),
    )
    wb_test = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    _ = client.post(
        "/preparar-plantilla",
        data={
            "referencia_actuarial": "triangulos",
            "referencia_contable": "triangulos",
        },
    )

    assert wb_test.sheets["Entremes"].visible
    assert wb_test.sheets["Frecuencia"].visible
    assert not wb_test.sheets["Severidad"].visible
    assert not wb_test.sheets["Plata"].visible

    validar_formulas_no_textuales(client, wb_test)
    validar_cifras_entremes(wb_test)

    _ = client.post("/almacenar-analisis")

    for siguiente_mes in [date(2025, 2, 1), date(2025, 3, 1)]:
        p = ingresar_parametros(
            client,
            Parametros(
                negocio="demo",
                mes_inicio=rango_meses[0],
                mes_corte=siguiente_mes,
                tipo_analisis="entremes",
                nombre_plantilla="wb_test",
            ),
        )
        _ = client.post("/preparar-plantilla")
        _ = client.post("/almacenar-analisis")

    p = ingresar_parametros(
        client,
        Parametros(
            negocio="demo",
            mes_inicio=rango_meses[0],
            mes_corte=date(2025, 3, 1),
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test",
        ),
    )

    _ = client.post("/preparar-plantilla")
    _ = client.post("/guardar-todo")
    _ = client.post("/almacenar-analisis")

    p = ingresar_parametros(
        client,
        Parametros(
            negocio="demo",
            mes_inicio=rango_meses[0],
            mes_corte=date(2025, 4, 1),
            tipo_analisis="entremes",
            nombre_plantilla="wb_test",
        ),
    )

    _ = client.post(
        "/preparar-plantilla",
        data={"referencia_actuarial": "triangulos", "referencia_contable": "entremes"},
    )

    _ = client.post("/almacenar-analisis")


def validar_formulas_no_textuales(client: TestClient, wb_test: xw.Book) -> None:
    """
    Validamos que no se peguen formulas en texto al preparar otra vez la plantilla.
    """
    _ = client.post(
        "/preparar-plantilla",
        data={
            "referencia_actuarial": "triangulos",
            "referencia_contable": "triangulos",
        },
    )

    col_bf = obtener_indice_en_rango(
        "pct_sue_bornhuetter_ferguson_bruto", wb_test.sheets["Entremes"].range("1:1")
    )
    assert isinstance(wb_test.sheets["Entremes"].cells(2, col_bf).value, float)


def validar_cifras_entremes(wb_test: xw.Book) -> None:
    periodicidades = utils.obtener_aperturas("demo", "siniestros").select(
        "apertura_reservas", "periodicidad_ocurrencia"
    )

    base_siniestros_original = (
        pl.read_parquet("data/processed/base_triangulos.parquet")
        .join(
            periodicidades,
            on=["apertura_reservas", "periodicidad_ocurrencia"],
            how="inner",
        )
        .filter(pl.col("diagonal") == 1)
        .filter(
            pl.col("periodo_ocurrencia")
            != pl.col("periodo_ocurrencia").max().over("apertura_reservas")
        )
        .select(
            [
                "apertura_reservas",
                "atipico",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
            ]
            + ct.COLUMNAS_QTYS
        )
        .vstack(
            pl.read_parquet("data/processed/base_ultima_ocurrencia.parquet")
            .join(
                periodicidades.rename(
                    {"periodicidad_ocurrencia": "periodicidad_triangulo"}
                ),
                on=["apertura_reservas", "periodicidad_triangulo"],
                how="inner",
            )
            .drop("periodicidad_triangulo")
        )
    )

    base_tipicos_original = base_siniestros_original.filter(pl.col("atipico") == 0)
    base_atipicos_original = base_siniestros_original.filter(pl.col("atipico") == 1)

    base_tipicos_plantilla = utils.sheet_to_dataframe(wb_test, "Resumen")
    base_atipicos_plantilla = utils.sheet_to_dataframe(wb_test, "Atipicos")

    assert base_tipicos_original.shape[0] == base_tipicos_plantilla.shape[0]
    for columna in ct.COLUMNAS_QTYS:
        assert_igual(base_tipicos_original, base_tipicos_plantilla, columna)

    assert base_atipicos_original.shape[0] == base_atipicos_plantilla.shape[0]
    for columna in ct.COLUMNAS_QTYS:
        assert_igual(base_atipicos_original, base_atipicos_plantilla, columna)

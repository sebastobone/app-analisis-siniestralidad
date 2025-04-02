import os
from datetime import date
from typing import Literal

import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import constantes as ct
from src import utils
from src.metodos_plantilla import abrir, actualizar, generar, preparar
from src.procesamiento import base_siniestros
from tests.conftest import agregar_meses_params, vaciar_directorio


@pytest.mark.integration
@pytest.mark.parametrize(
    "tipo_analisis, periodicidad_ocurrencia",
    [
        ("triangulos", "Mensual"),
        ("triangulos", "Trimestral"),
        ("triangulos", "Semestral"),
        ("triangulos", "Anual"),
        ("entremes", "Trimestral"),
        ("entremes", "Semestral"),
        ("entremes", "Anual"),
    ],
)
def test_forma_triangulo(
    tipo_analisis: Literal["triangulos", "entremes"],
    periodicidad_ocurrencia: Literal["Mensual", "Trimestral", "Semestral", "Anual"],
    rango_meses: tuple[date, date],
):
    mock_siniestros = utils.generar_mock_siniestros(rango_meses)
    base_triangulos, _, _ = base_siniestros.generar_bases_siniestros(
        mock_siniestros.lazy(), tipo_analisis, *rango_meses
    )

    df = generar.crear_triangulo_base_plantilla(
        base_triangulos.lazy(),
        "01_001_A_D",
        "bruto",
        pl.DataFrame(
            {
                "apertura_reservas": ["01_001_A_D"],
                "periodicidad_ocurrencia": [periodicidad_ocurrencia],
            }
        ),
        ["pago", "incurrido"],
    )

    if tipo_analisis == "triangulos":
        assert df.shape[0] == df.shape[1] // 2
    elif tipo_analisis == "entremes":
        assert (
            df.shape[0] * ct.PERIODICIDADES[periodicidad_ocurrencia] >= df.shape[1] // 2
        )


@pytest.mark.integration
def test_plantilla_no_preparada(client: TestClient, rango_meses: tuple[date, date]):
    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test_no_prep",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form)
    wb = abrir.abrir_plantilla(f"plantillas/{params_form['nombre_plantilla']}.xlsm")

    _ = client.post("/correr-query-siniestros")
    _ = client.post("/correr-query-primas")
    _ = client.post("/correr-query-expuestos")

    with pytest.raises(preparar.PlantillaNoPreparadaError):
        _ = client.post(
            "/generar-plantilla",
            data={"apertura": "01_001_A_D", "atributo": "bruto", "plantilla": "plata"},
        )

    vaciar_directorio("data/raw")
    wb.close()
    os.remove(f"plantillas/{params_form['nombre_plantilla']}.xlsm")


def test_generar_severidad(client: TestClient, rango_meses: tuple[date, date]):
    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form).json()
    wb = abrir.abrir_plantilla(f"plantillas/{params_form['nombre_plantilla']}.xlsm")

    _ = client.post("/correr-query-siniestros")
    _ = client.post("/correr-query-primas")
    _ = client.post("/correr-query-expuestos")

    _ = client.post("/preparar-plantilla")

    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_001_A_D", "atributo": "bruto", "plantilla": "severidad"},
    )

    apertura_en_frecuencia = actualizar.obtener_apertura_actual(wb, "frecuencia")
    apertura_en_severidad = actualizar.obtener_apertura_actual(wb, "severidad")
    assert apertura_en_frecuencia == apertura_en_severidad

    vaciar_directorio("data/raw")
    vaciar_directorio("data/processed")

import os
from datetime import date
from typing import Literal

import polars as pl
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlmodel import Session, select
from src import constantes as ct
from src.metodos_plantilla import abrir, generar
from src.models import Parametros
from src.procesamiento import base_siniestros
from tests.metodos_plantilla.conftest import agregar_meses_params


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
    mock_siniestros: pl.LazyFrame,
    rango_meses: tuple[date, date],
):
    _, _, _ = base_siniestros.generar_bases_siniestros(
        mock_siniestros, tipo_analisis, *rango_meses
    )

    df = generar.crear_triangulo_base_plantilla(
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


@pytest.mark.plantilla
@pytest.mark.integration
@pytest.mark.parametrize(
    "params_form",
    [
        {"negocio": "mock", "tipo_analisis": "triangulos", "nombre_plantilla": "test1"},
        # {"negocio": "mock", "tipo_analisis": "entremes", "nombre_plantilla": "test2"},
    ],
)
def test_generar_plantilla(
    client: TestClient,
    test_session: Session,
    params_form: dict[str, str],
    rango_meses: tuple[date, date],
):
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form)
    p = test_session.exec(select(Parametros)).all()[0]

    _ = client.post("/preparar-plantilla")
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    if p.tipo_analisis == "triangulos":
        plantillas = ["frec", "seve", "plata"]
    elif p.tipo_analisis == "entremes":
        plantillas = ["entremes"]

    for plantilla in plantillas:
        response = client.post(
            "/modos-plantilla", data={"plant": plantilla, "modo": "generar"}
        )

        assert response.status_code == status.HTTP_200_OK
        assert os.path.exists(f"plantillas/{p.nombre_plantilla}.xlsm")

        plantilla_name = f"Plantilla_{plantilla.capitalize()}"
        assert wb.sheets[plantilla_name].range((2, 2)).value == "Apertura"
        assert wb.sheets[plantilla_name].range((2, 3)).value == "01_001_A_D"
        assert wb.sheets[plantilla_name].range((3, 2)).value == "Atributo"
        assert wb.sheets[plantilla_name].range((3, 3)).value == "Bruto"

    wb.close()
    os.remove(f"plantillas/{p.nombre_plantilla}.xlsm")

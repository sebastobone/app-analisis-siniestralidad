from datetime import date
from typing import Literal

import polars as pl
import pytest
from src import constantes as ct
from src import utils
from src.metodos_plantilla import generar
from src.procesamiento import base_siniestros
from tests.conftest import vaciar_directorio


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
    base_triangulos.write_parquet("data/processed/base_triangulos.parquet")

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

    vaciar_directorio("data/raw")
    vaciar_directorio("data/processed")

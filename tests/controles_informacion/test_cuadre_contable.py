from datetime import date
from unittest.mock import patch

import numpy as np
import polars as pl
import pytest
from src import utils
from src.controles_informacion import cuadre_contable, sap
from src.controles_informacion import generacion as ctrl
from src.informacion.mocks import generar_mock
from tests.conftest import assert_diferente, assert_igual
from tests.controles_informacion.test_generacion import mock_hoja_afo


@pytest.mark.asyncio
@pytest.mark.fast
@pytest.mark.parametrize("qty", ["pago_bruto", "aviso_bruto"])
async def test_cuadre_contable_soat(rango_meses: tuple[date, date], qty: str) -> None:
    with patch("src.controles_informacion.sap.pl.read_excel") as mock_read_excel:
        mock_read_excel.return_value = mock_hoja_afo(rango_meses[1], "pago_bruto")

        df_sap = (
            (
                await sap.consolidar_sap(
                    "soat", ["pago_bruto", "pago_retenido"], rango_meses[1]
                )
            )
            .with_columns(
                aviso_bruto=pl.col("pago_bruto") * np.random.random(),
                aviso_retenido=pl.col("pago_retenido") * np.random.random(),
            )
            .filter((pl.col("codigo_ramo_op") == "001") & (pl.col("codigo_op") == "01"))
        )

    mock_siniestros = generar_mock(rango_meses, "siniestros")

    mock_soat = mock_siniestros.filter(
        (pl.col("codigo_ramo_op") == "001") & (pl.col("codigo_op") == "01")
    )

    qtys = ["pago_bruto", "pago_retenido", "aviso_bruto", "aviso_retenido"]
    df_tera = ctrl.agrupar_tera(
        mock_soat, ["codigo_op", "codigo_ramo_op", "fecha_registro"], qtys
    )

    dif_sap_vs_tera = await ctrl.comparar_sap_tera(
        df_tera, df_sap, rango_meses[1], qtys
    )

    meses_prueba = (
        mock_soat.group_by("fecha_registro")
        .agg(pl.sum("pago_bruto"))
        .sort("pago_bruto", descending=True)
        .get_column("fecha_registro")
        .to_list()[:2]
    )

    meses_a_cuadrar = pl.DataFrame(
        {
            "fecha_registro": [meses_prueba[0], meses_prueba[1]],
            "cuadrar_pago_bruto": [1, 1],
            "cuadrar_pago_retenido": [1, 1],
            "cuadrar_aviso_bruto": [1, 1],
            "cuadrar_aviso_retenido": [1, 1],
        }
    )

    with patch(
        "src.controles_informacion.cuadre_contable.obtener_aperturas_para_asignar_diferencia"
    ) as mock_apertura_dif_soat:
        mock_apertura_dif_soat.return_value = pl.DataFrame(
            {
                "codigo_op": ["01"],
                "codigo_ramo_op": ["001"],
                "apertura_1": ["A"],
                "apertura_2": ["D"],
            }
        ).with_columns(utils.crear_columna_apertura_reservas("demo", "siniestros"))

        with patch("src.controles_informacion.cuadre_contable.guardar_archivos"):
            df_cuadre = await cuadre_contable.realizar_cuadre_contable(
                "demo", "siniestros", mock_soat, dif_sap_vs_tera, meses_a_cuadrar
            )

    cifra_sap_meses_a_cuadrar = df_sap.filter(
        pl.col("fecha_registro").is_in([meses_prueba[0], meses_prueba[1]])
    )
    cifra_final_meses_a_cuadrar = df_cuadre.filter(
        pl.col("fecha_registro").is_in([meses_prueba[0], meses_prueba[1]])
    )

    cifra_sap_meses_sin_cuadre = df_sap.filter(
        ~pl.col("fecha_registro").is_in([meses_prueba[0], meses_prueba[1]])
    )
    cifra_final_meses_sin_cuadre = df_cuadre.filter(
        ~pl.col("fecha_registro").is_in([meses_prueba[0], meses_prueba[1]])
    )

    assert_igual(cifra_sap_meses_a_cuadrar, cifra_final_meses_a_cuadrar, qty)
    assert_diferente(cifra_sap_meses_sin_cuadre, cifra_final_meses_sin_cuadre, qty)

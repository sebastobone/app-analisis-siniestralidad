from datetime import date
from unittest.mock import patch

import numpy as np
import polars as pl
import pytest
from src import utils
from src.controles_informacion import cuadre_contable, sap
from src.controles_informacion import generacion as ctrl
from tests.conftest import assert_diferente, assert_igual
from tests.controles_informacion.test_generacion import mock_hoja_afo


@pytest.mark.asyncio
@pytest.mark.parametrize("qty", ["pago_bruto", "aviso_bruto"])
async def test_cuadre_contable_soat(rango_meses: tuple[date, date], qty: str) -> None:
    mes_inicio, mes_corte = rango_meses
    mes_corte_int = utils.date_to_yyyymm(mes_corte)

    with patch("src.controles_informacion.sap.pl.read_excel") as mock_read_excel:
        mock_read_excel.return_value = mock_hoja_afo(mes_corte_int, "pago_bruto")

        df_sap = (
            (
                await sap.consolidar_sap(
                    ["Generales"], ["pago_bruto", "pago_retenido"], mes_corte_int
                )
            )
            .with_columns(
                aviso_bruto=pl.col("pago_bruto") * np.random.random(),
                aviso_retenido=pl.col("pago_retenido") * np.random.random(),
            )
            .filter((pl.col("codigo_ramo_op") == "001") & (pl.col("codigo_op") == "01"))
        )

    mock_siniestros = utils.generar_mock_siniestros(rango_meses)

    mock_soat = mock_siniestros.filter(
        (pl.col("codigo_ramo_op") == "001") & (pl.col("codigo_op") == "01")
    )

    qtys = ["pago_bruto", "pago_retenido", "aviso_bruto", "aviso_retenido"]
    df_tera = ctrl.agrupar_tera(
        mock_soat, ["codigo_op", "codigo_ramo_op", "fecha_registro"], qtys
    )

    dif_sap_vs_tera = await ctrl.comparar_sap_tera(df_tera, df_sap, mes_corte_int, qtys)

    meses_a_cuadrar = pl.DataFrame({"fecha_registro": [mes_inicio, mes_corte]})

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
        ).with_columns(utils.crear_columna_apertura_reservas("demo"))

        with patch("src.controles_informacion.cuadre_contable.guardar_archivos"):
            df_cuadre = await cuadre_contable.realizar_cuadre_contable(
                "demo", "siniestros", mock_soat, dif_sap_vs_tera, meses_a_cuadrar
            )

    cifra_sap_meses_a_cuadrar = df_sap.filter(
        pl.col("fecha_registro").is_in([mes_inicio, mes_corte])
    )
    cifra_final_meses_a_cuadrar = df_cuadre.filter(
        pl.col("fecha_registro").is_in([mes_inicio, mes_corte])
    )

    cifra_sap_meses_sin_cuadre = df_sap.filter(
        ~pl.col("fecha_registro").is_in([mes_inicio, mes_corte])
    )
    cifra_final_meses_sin_cuadre = df_cuadre.filter(
        ~pl.col("fecha_registro").is_in([mes_inicio, mes_corte])
    )

    assert_igual(cifra_sap_meses_a_cuadrar, cifra_final_meses_a_cuadrar, qty)
    assert_diferente(cifra_sap_meses_sin_cuadre, cifra_final_meses_sin_cuadre, qty)

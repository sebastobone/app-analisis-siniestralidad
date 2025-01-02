import os
from datetime import date
from typing import Literal
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from src import constantes as ct
from src import utils
from src import plantilla as plant
from src.procesamiento import base_primas_expuestos, base_siniestros


def generar_bases_mock(
    mock_siniestros: pl.LazyFrame,
    mock_expuestos: pl.LazyFrame,
    mock_primas: pl.LazyFrame,
    tipo_analisis: Literal["triangulos", "entremes"],
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    with patch("src.procesamiento.base_siniestros.guardar_archivos"):
        base_triangulos, base_ult_ocurr, base_atipicos = (
            base_siniestros.generar_bases_siniestros(
                mock_siniestros, tipo_analisis, date(2018, 12, 1), date(2023, 12, 1)
            )
        )

    base_expuestos = base_primas_expuestos.generar_base_primas_expuestos(
        mock_expuestos, "expuestos", "mock"
    )
    base_primas = base_primas_expuestos.generar_base_primas_expuestos(
        mock_primas, "primas", "mock"
    )

    return base_triangulos, base_ult_ocurr, base_atipicos, base_expuestos, base_primas


@pytest.mark.plantilla
@pytest.mark.parametrize(
    "mes_corte, tipo_analisis, plantillas",
    [
        (202312, "triangulos", ["frec", "seve", "plata"]),
        (202312, "entremes", ["entremes"]),
    ],
)
@patch("src.metodos_plantilla.insumos.df_primas")
@patch("src.metodos_plantilla.insumos.df_expuestos")
@patch("src.metodos_plantilla.insumos.df_atipicos")
@patch("src.metodos_plantilla.insumos.df_ult_ocurr")
@patch("src.metodos_plantilla.insumos.df_triangulos")
@patch("src.plantilla.tablas_resumen.df_aperturas")
def test_preparar_y_generar_plantilla(
    mock_df_aperturas: MagicMock,
    mock_df_triangulos: MagicMock,
    mock_df_ult_ocurr: MagicMock,
    mock_df_atipicos: MagicMock,
    mock_df_expuestos: MagicMock,
    mock_df_primas: MagicMock,
    mock_siniestros: pl.LazyFrame,
    mock_expuestos: pl.LazyFrame,
    mock_primas: pl.LazyFrame,
    mes_corte: int,
    tipo_analisis: Literal["triangulos", "entremes"],
    plantillas: list[Literal["frec", "seve", "plata", "entremes"]],
):
    base_triangulos, base_ult_ocurr, base_atipicos, base_expuestos, base_primas = (
        generar_bases_mock(mock_siniestros, mock_expuestos, mock_primas, tipo_analisis)
    )

    mock_df_aperturas.return_value = mock_siniestros
    mock_df_triangulos.return_value = base_triangulos.lazy()
    mock_df_ult_ocurr.return_value = base_ult_ocurr.lazy()
    mock_df_atipicos.return_value = base_atipicos.lazy()
    mock_df_expuestos.return_value = base_expuestos.lazy()
    mock_df_primas.return_value = base_primas.lazy()

    if os.path.exists("tests/mock_plantilla.xlsm"):
        os.remove("tests/mock_plantilla.xlsm")

    wb = plant.abrir_plantilla("tests/mock_plantilla.xlsm")
    plant.preparar_plantilla(wb, mes_corte, tipo_analisis, "mock")

    assert wb.sheets["Main"]["A4"].value == "Mes corte"
    assert wb.sheets["Main"]["B4"].value == mes_corte
    assert wb.sheets["Main"]["A5"].value == "Mes anterior"
    assert wb.sheets["Main"]["B5"].value == (
        mes_corte - 1 if mes_corte % 100 != 1 else ((mes_corte // 100) - 1) * 100 + 12
    )

    if tipo_analisis == "triangulos":
        assert not wb.sheets["Plantilla_Entremes"].visible
        assert wb.sheets["Plantilla_Frec"].visible
        assert wb.sheets["Plantilla_Seve"].visible
        assert wb.sheets["Plantilla_Plata"].visible
    elif tipo_analisis == "entremes":
        assert wb.sheets["Plantilla_Entremes"].visible
        assert not wb.sheets["Plantilla_Frec"].visible
        assert not wb.sheets["Plantilla_Seve"].visible
        assert not wb.sheets["Plantilla_Plata"].visible

    assert "aperturas" in [table.name for table in wb.sheets["Main"].tables]
    assert "periodicidades" in [table.name for table in wb.sheets["Main"].tables]

    df_original = base_triangulos.filter(
        (pl.col("periodicidad_ocurrencia") == "Trimestral") & (pl.col("diagonal") == 1)
    ).select(
        [
            "apertura_reservas",
            "periodicidad_ocurrencia",
            "periodo_ocurrencia",
        ]
        + ct.COLUMNAS_QTYS
    )

    if tipo_analisis == "entremes":
        df_original = df_original.filter(
            pl.col("periodo_ocurrencia")
            != pl.col("periodo_ocurrencia").max().over("apertura_reservas")
        ).vstack(
            base_ult_ocurr.filter(
                pl.col("periodicidad_triangulo") == "Trimestral"
            ).drop("periodicidad_triangulo")
        )

    df_plantilla = utils.sheet_to_dataframe(wb, "Aux_Totales").collect()

    assert df_original.shape[0] == df_plantilla.shape[0]
    assert (
        abs(
            df_original.get_column("pago_bruto").sum()
            - df_plantilla.get_column("pago_bruto").sum()
        )
        < 100
    )

    for plantilla in plantillas:
        plant.generar_plantilla(wb, plantilla, "01_001_A_D", "bruto", mes_corte)

    wb.close()


@pytest.mark.plantilla
@pytest.mark.parametrize(
    "mes_corte, tipo_analisis, plantillas, rangos_adicionales",
    [
        (
            202312,
            "triangulos",
            ["frec", "seve", "plata"],
            {
                "frec": ["BASE", "INDICADOR", "COMENTARIOS"],
                "seve": [
                    "BASE",
                    "INDICADOR",
                    "COMENTARIOS",
                    "TIPO_INDEXACION",
                    "MEDIDA_INDEXACION",
                ],
                "plata": ["BASE", "INDICADOR", "COMENTARIOS"],
            },
        ),
        (
            202312,
            "entremes",
            ["entremes"],
            {
                "entremes": [
                    "FREC_SEV_ULTIMA_OCURRENCIA",
                    "VARIABLE_DESPEJADA",
                    "COMENTARIOS",
                    "FACTOR_COMPLETITUD",
                    "PCT_SUE_BF",
                    "VELOCIDAD_BF",
                    "PCT_SUE_NUEVO",
                    "AJUSTE_PARCIAL",
                    "COMENTARIOS_AJUSTE",
                ]
            },
        ),
    ],
)
@patch("src.metodos_plantilla.insumos.df_primas")
@patch("src.metodos_plantilla.insumos.df_expuestos")
@patch("src.metodos_plantilla.insumos.df_atipicos")
@patch("src.metodos_plantilla.insumos.df_ult_ocurr")
@patch("src.metodos_plantilla.insumos.df_triangulos")
@patch("src.plantilla.tablas_resumen.df_aperturas")
def test_guardar_traer(
    mock_df_aperturas: MagicMock,
    mock_df_triangulos: MagicMock,
    mock_df_ult_ocurr: MagicMock,
    mock_df_atipicos: MagicMock,
    mock_df_expuestos: MagicMock,
    mock_df_primas: MagicMock,
    mock_siniestros: pl.LazyFrame,
    mock_expuestos: pl.LazyFrame,
    mock_primas: pl.LazyFrame,
    mes_corte: int,
    tipo_analisis: Literal["triangulos", "entremes"],
    plantillas: list[Literal["frec", "seve", "plata", "entremes"]],
    rangos_adicionales: dict[Literal["frec", "seve", "plata", "entremes"], list[str]],
):
    base_triangulos, base_ult_ocurr, base_atipicos, base_expuestos, base_primas = (
        generar_bases_mock(mock_siniestros, mock_expuestos, mock_primas, tipo_analisis)
    )

    mock_df_aperturas.return_value = mock_siniestros
    mock_df_triangulos.return_value = base_triangulos.lazy()
    mock_df_ult_ocurr.return_value = base_ult_ocurr.lazy()
    mock_df_atipicos.return_value = base_atipicos.lazy()
    mock_df_expuestos.return_value = base_expuestos.lazy()
    mock_df_primas.return_value = base_primas.lazy()

    if os.path.exists("tests/mock_plantilla.xlsm"):
        os.remove("tests/mock_plantilla.xlsm")

    wb = plant.abrir_plantilla("tests/mock_plantilla.xlsm")
    plant.preparar_plantilla(wb, mes_corte, tipo_analisis, "mock")

    for plantilla in plantillas:
        plant.generar_plantilla(wb, plantilla, "01_001_A_D", "bruto", mes_corte)

    map_plantillas = {
        "frec": "Plantilla_Frec",
        "seve": "Plantilla_Seve",
        "plata": "Plantilla_Plata",
        "entremes": "Plantilla_Entremes",
    }

    rangos_comunes = [
        "MET_PAGO_INCURRIDO",
        "EXCLUSIONES",
        "VENTANAS",
        "FACTORES_SELECCIONADOS",
        "ULTIMATE",
        "METODOLOGIA",
    ]

    for plantilla in plantillas:
        archivos_guardados = [
            f"01_001_A_D_bruto_{map_plantillas[plantilla]}_{nombre_rango}"
            for nombre_rango in rangos_comunes + rangos_adicionales[plantilla]
        ]

        with pytest.raises(FileNotFoundError):
            plant.guardar_traer_fn(
                wb, "traer", plantilla, "01_001_A_D", "bruto", mes_corte
            )

        with patch(
            "src.plantilla.guardar_traer.pl.DataFrame.write_csv"
        ) as mock_guardar:
            plant.guardar_traer_fn(
                wb, "guardar", plantilla, "01_001_A_D", "bruto", mes_corte
            )
            for archivo in archivos_guardados:
                mock_guardar.assert_any_call(f"data/db/{archivo}.csv", separator="\t")

        with patch("src.plantilla.guardar_traer.pl.read_csv") as mock_leer:
            mock_leer.return_value = pl.DataFrame(
                [("ABCDEFG", "HIJKLMN"), ("ABCDEFG", "HIJKLMN")]
            )
            plant.guardar_traer_fn(
                wb, "traer", plantilla, "01_001_A_D", "bruto", mes_corte
            )
            for archivo in archivos_guardados:
                mock_leer.assert_any_call(f"data/db/{archivo}.csv", separator="\t")

    wb.close()

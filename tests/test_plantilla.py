import os
from datetime import date
from typing import Literal
from unittest.mock import patch

import polars as pl
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlmodel import Session, select
from src import constantes as ct
from src import plantilla as plant
from src import utils
from src.metodos_plantilla import base_plantillas
from src.models import Parametros
from src.procesamiento import base_siniestros

from tests.conftest import assert_igual


@pytest.fixture(autouse=True)
def guardar_bases_ficticias(
    mock_siniestros: pl.LazyFrame,
    mock_primas: pl.LazyFrame,
    mock_expuestos: pl.LazyFrame,
) -> None:
    mock_siniestros.collect().write_parquet("data/raw/siniestros.parquet")
    mock_primas.collect().write_parquet("data/raw/primas.parquet")
    mock_expuestos.collect().write_parquet("data/raw/expuestos.parquet")


def agregar_meses_params(
    params_form: dict[str, str], mes_inicio: date, mes_corte: date
):
    params_form.update(
        {
            "mes_corte": str(utils.date_to_yyyymm(mes_corte)),
            "mes_inicio": str(utils.date_to_yyyymm(mes_inicio)),
        }
    )


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
    mes_inicio: date,
    mes_corte: date,
):
    _, _, _ = base_siniestros.generar_bases_siniestros(
        mock_siniestros, tipo_analisis, mes_inicio, mes_corte
    )

    df = base_plantillas.base_plantillas(
        "01_001_A_D",
        "bruto",
        [["01_001_A_D", periodicidad_ocurrencia]],
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
        {"negocio": "mock", "tipo_analisis": "entremes", "nombre_plantilla": "test2"},
    ],
)
def test_preparar_plantilla(
    client: TestClient,
    test_session: Session,
    params_form: dict[str, str],
    mes_inicio: date,
    mes_corte: date,
):
    agregar_meses_params(params_form, mes_inicio, mes_corte)

    _ = client.post("/ingresar-parametros", data=params_form)
    p = test_session.exec(select(Parametros)).all()[0]

    response = client.post("/preparar-plantilla")

    assert response.status_code == status.HTTP_200_OK
    assert os.path.exists(f"plantillas/{p.nombre_plantilla}.xlsm")

    wb = plant.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    assert wb.sheets["Main"]["A4"].value == "Mes corte"
    assert wb.sheets["Main"]["B4"].value == p.mes_corte
    assert wb.sheets["Main"]["A5"].value == "Mes anterior"
    assert wb.sheets["Main"]["B5"].value == (
        p.mes_corte - 1
        if p.mes_corte % 100 != 1
        else ((p.mes_corte // 100) - 1) * 100 + 12
    )

    if p.tipo_analisis == "triangulos":
        assert not wb.sheets["Plantilla_Entremes"].visible
        assert wb.sheets["Plantilla_Frec"].visible
        assert wb.sheets["Plantilla_Seve"].visible
        assert wb.sheets["Plantilla_Plata"].visible
    elif p.tipo_analisis == "entremes":
        assert wb.sheets["Plantilla_Entremes"].visible
        assert not wb.sheets["Plantilla_Frec"].visible
        assert not wb.sheets["Plantilla_Seve"].visible
        assert not wb.sheets["Plantilla_Plata"].visible

    assert "aperturas" in [table.name for table in wb.sheets["Main"].tables]
    assert "periodicidades" in [table.name for table in wb.sheets["Main"].tables]

    base_triangulos = pl.read_parquet("data/processed/base_triangulos.parquet")

    df_original = base_triangulos.filter(
        (pl.col("periodicidad_ocurrencia") == "Trimestral") & (pl.col("diagonal") == 1)
    ).select(
        ["apertura_reservas", "periodicidad_ocurrencia", "periodo_ocurrencia"]
        + ct.COLUMNAS_QTYS
    )

    if p.tipo_analisis == "entremes":
        base_ult_ocurr = pl.read_parquet(
            "data/processed/base_ultima_ocurrencia.parquet"
        )
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
    assert_igual(df_original, df_plantilla, "pago_bruto")

    wb.close()
    os.remove(f"plantillas/{p.nombre_plantilla}.xlsm")


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
    mes_inicio: date,
    mes_corte: date,
):
    agregar_meses_params(params_form, mes_inicio, mes_corte)

    _ = client.post("/ingresar-parametros", data=params_form)
    p = test_session.exec(select(Parametros)).all()[0]

    _ = client.post("/preparar-plantilla")
    wb = plant.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

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


@pytest.mark.plantilla
@pytest.mark.integration
@pytest.mark.parametrize(
    "params_form, rangos_adicionales",
    [
        (
            {
                "negocio": "mock",
                "tipo_analisis": "triangulos",
                "nombre_plantilla": "plantilla_test_triangulos",
            },
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
        # (
        #     {
        #         "negocio": "mock",
        #         "mes_inicio": "201501",
        #         "mes_corte": "203012",
        #         "tipo_analisis": "entremes",
        #         "nombre_plantilla": "plantilla_test_entremes",
        #     },
        #     ["entremes"],
        #     {
        #         "entremes": [
        #             "FREC_SEV_ULTIMA_OCURRENCIA",
        #             "VARIABLE_DESPEJADA",
        #             "COMENTARIOS",
        #             "FACTOR_COMPLETITUD",
        #             "PCT_SUE_BF",
        #             "VELOCIDAD_BF",
        #             "PCT_SUE_NUEVO",
        #             "AJUSTE_PARCIAL",
        #             "COMENTARIOS_AJUSTE",
        #         ]
        #     },
        # ),
    ],
)
def test_guardar_traer(
    client: TestClient,
    test_session: Session,
    params_form: dict[str, str],
    rangos_adicionales: dict[str, list[str]],
    mes_inicio: date,
    mes_corte: date,
):
    agregar_meses_params(params_form, mes_inicio, mes_corte)

    _ = client.post("/ingresar-parametros", data=params_form)
    p = test_session.exec(select(Parametros)).all()[0]

    _ = client.post("/preparar-plantilla")
    wb = plant.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    rangos_comunes = [
        "MET_PAGO_INCURRIDO",
        "EXCLUSIONES",
        "VENTANAS",
        "FACTORES_SELECCIONADOS",
        "ULTIMATE",
        "METODOLOGIA",
    ]

    if p.tipo_analisis == "triangulos":
        plantillas = ["frec", "seve", "plata"]
    elif p.tipo_analisis == "entremes":
        plantillas = ["entremes"]

    for plantilla in plantillas:
        _ = client.post(
            "/modos-plantilla", data={"plant": plantilla, "modo": "generar"}
        )

        plantilla_name = f"Plantilla_{plantilla.capitalize()}"

        apertura = str(wb.sheets[plantilla_name]["C2"].value)
        atributo = str(wb.sheets[plantilla_name]["C3"].value).lower()

        archivos_guardados = [
            f"{apertura}_{atributo}_Plantilla_{plantilla.capitalize()}_{nombre_rango}"
            for nombre_rango in rangos_comunes + rangos_adicionales[plantilla]
        ]

        with pytest.raises(FileNotFoundError):
            _ = client.post(
                "/modos-plantilla", data={"plant": plantilla, "modo": "traer"}
            )

        with patch(
            "src.plantilla.guardar_traer.pl.DataFrame.write_csv"
        ) as mock_guardar:
            response = client.post(
                "/modos-plantilla", data={"plant": plantilla, "modo": "guardar"}
            )
            assert response.status_code == status.HTTP_200_OK
            for archivo in archivos_guardados:
                mock_guardar.assert_any_call(f"data/db/{archivo}.csv", separator="\t")

        with patch("src.plantilla.guardar_traer.pl.read_csv") as mock_leer:
            mock_leer.return_value = pl.DataFrame(
                [("ABCDEFG", "HIJKLMN"), ("ABCDEFG", "HIJKLMN")]
            )
            response = client.post(
                "/modos-plantilla", data={"plant": plantilla, "modo": "traer"}
            )
            assert response.status_code == status.HTTP_200_OK
            for archivo in archivos_guardados:
                mock_leer.assert_any_call(f"data/db/{archivo}.csv", separator="\t")

    wb.close()
    os.remove(f"plantillas/{p.nombre_plantilla}.xlsm")


@pytest.mark.plantilla
@pytest.mark.integration
@pytest.mark.parametrize(
    "params_form, plantillas",
    [
        (
            {
                "negocio": "mock",
                "tipo_analisis": "triangulos",
                "nombre_plantilla": "plantilla_test_triangulos",
            },
            ["frec", "seve"],
        ),
        (
            {
                "negocio": "mock",
                "tipo_analisis": "triangulos",
                "nombre_plantilla": "plantilla_test_triangulos",
            },
            ["plata"],
        ),
        # (
        #     {
        #         "negocio": "mock",
        #         "mes_inicio": "201501",
        #         "mes_corte": "203012",
        #         "tipo_analisis": "triangulos",
        #         "nombre_plantilla": "plantilla_test_triangulos",
        #     },
        #     ["entremes"],
        # ),
    ],
)
def test_almacenar_analisis(
    client: TestClient,
    test_session: Session,
    params_form: dict[str, str],
    plantillas: list[Literal["frec", "seve", "plata", "entremes"]],
    mes_inicio: date,
    mes_corte: date,
):
    agregar_meses_params(params_form, mes_inicio, mes_corte)

    _ = client.post("/ingresar-parametros", data=params_form)
    p = test_session.exec(select(Parametros)).all()[0]

    _ = client.post("/preparar-plantilla")
    wb = plant.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    for plantilla in plantillas:
        _ = client.post(
            "/modos-plantilla", data={"plant": plantilla, "modo": "generar"}
        )
        _ = client.post(
            "/modos-plantilla", data={"plant": plantilla, "modo": "guardar"}
        )

    response = client.post("/almacenar-analisis")
    assert response.status_code == status.HTTP_200_OK
    assert os.path.exists(
        f"output/resultados/{p.nombre_plantilla}_{p.mes_corte}.parquet"
    )

    info_plantilla = utils.sheet_to_dataframe(wb, "Aux_Totales").collect()
    info_guardada = pl.read_parquet(
        f"output/resultados/{p.nombre_plantilla}_{p.mes_corte}.parquet"
    ).filter(pl.col("atipico") == 0)

    assert_igual(info_plantilla, info_guardada, "plata_ultimate_bruto")

    wb.close()
    os.remove(f"plantillas/{p.nombre_plantilla}.xlsm")

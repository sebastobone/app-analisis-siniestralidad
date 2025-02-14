import os
from datetime import date
from unittest.mock import patch

import polars as pl
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlmodel import Session, select
from src.metodos_plantilla import abrir
from src.models import Parametros
from tests.metodos_plantilla.conftest import agregar_meses_params


def limpiar_datos_guardados() -> None:
    for file in os.listdir("data/db"):
        if "01_001_A_D" in file:
            os.remove(f"data/db/{file}")


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
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

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

        limpiar_datos_guardados()
        with pytest.raises(FileNotFoundError):
            _ = client.post(
                "/modos-plantilla", data={"plant": plantilla, "modo": "traer"}
            )

        with patch(
            "src.metodos_plantilla.guardar_traer.guardar_apertura.pl.DataFrame.write_csv"
        ) as mock_guardar:
            response = client.post(
                "/modos-plantilla", data={"plant": plantilla, "modo": "guardar"}
            )
            assert response.status_code == status.HTTP_200_OK
            for archivo in archivos_guardados:
                mock_guardar.assert_any_call(f"data/db/{archivo}.csv", separator="\t")

        with patch(
            "src.metodos_plantilla.guardar_traer.traer_apertura.pl.read_csv"
        ) as mock_leer:
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

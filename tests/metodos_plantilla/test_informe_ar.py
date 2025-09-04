from datetime import date

import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import utils
from src.metodos_plantilla import abrir
from src.metodos_plantilla.resultados import concatenar_archivos_resultados
from src.models import Parametros
from tests.conftest import (
    agregar_meses_params,
    assert_igual,
    correr_queries,
    vaciar_directorios_test,
)


@pytest.mark.plantilla
def test_generar_informe_ar(client: TestClient, rango_meses: tuple[date, date]):
    vaciar_directorios_test()

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }

    agregar_meses_params(params_form, rango_meses)

    response = client.post("/ingresar-parametros", params=params_form).json()
    p = Parametros.model_validate(response)

    correr_queries(client)

    _ = client.post("/preparar-plantilla")
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_001_A_D", "atributo": "bruto", "plantilla": "plata"},
    )
    _ = client.post(
        "/guardar-apertura",
        data={"apertura": "01_001_A_D", "atributo": "bruto", "plantilla": "plata"},
    )

    _ = client.post("/almacenar-analisis")
    _ = client.post("/generar-informe-ar")

    mes_corte = utils.date_to_yyyymm(rango_meses[1])

    ar = pl.read_excel(f"output/informe_ar_demo_{mes_corte}.xlsx")
    resultado = concatenar_archivos_resultados()

    columnas = [
        ["sue_actuarial", "plata_ultimate"],
        ["sue_contable", "plata_ultimate_contable"],
        ["ibnr_actuarial", "ibnr"],
        ["ibnr_contable", "ibnr_contable"],
    ]

    for cols in columnas:
        for atributo in ["bruto", "retenido"]:
            assert_igual(
                ar.filter(pl.col("atributo") == atributo),
                resultado,
                cols[0],
                f"{cols[1]}_{atributo}",
            )

    for col in ["pago", "aviso"]:
        for atributo in ["bruto", "retenido"]:
            for tipo_siniestro in [["tipicos", 0], ["atipicos", 1]]:
                assert_igual(
                    ar.filter(pl.col("atributo") == atributo),
                    resultado.filter(pl.col("atipico") == tipo_siniestro[1]),
                    f"{col}_{tipo_siniestro[0]}",
                    f"{col}_{atributo}",
                )

    wb.close()

    vaciar_directorios_test()

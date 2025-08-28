import os
from datetime import date

import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import utils
from src.metodos_plantilla import abrir, actualizar
from tests.conftest import agregar_meses_params, correr_queries, vaciar_directorios_test

ERROR_MEDIDA_NO_ENCONTRADA = utils.limpiar_espacios_log(
    """
    La medida de indexacion "SMMLV" no se encuentra en la hoja
    Indexaciones. Por favor, verifique que la medida este escrita
    correctamente y que exista en la hoja.
    """
)

ERROR_FRECUENCIA_NO_ENCONTRADA = utils.limpiar_espacios_log(
    """
    No se han guardado resultados de frecuencia para la apertura
    01_002_A_E. Para generar la plantilla de severidad
    con indexacion por fecha de movimiento, se necesitan resultados
    almacenados de frecuencia.
    """
)

MEDIDAS = pl.DataFrame(
    pl.date_range(pl.date(2000, 1, 1), pl.date(2050, 12, 1), "1mo", eager=True).alias(
        "periodo"
    )
).with_columns(
    periodo=pl.col("periodo").dt.year() * 100 + pl.col("periodo").dt.month(),
    SMMLV=pl.lit(1000000),
    IPC=pl.lit(100),
)


def test_indexacion_ocurrencia(client: TestClient, rango_meses: tuple[date, date]):
    vaciar_directorios_test()

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form).json()
    wb = abrir.abrir_plantilla(f"plantillas/{params_form['nombre_plantilla']}.xlsm")

    correr_queries(client)

    _ = client.post("/preparar-plantilla")

    wb.sheets["Indexaciones"].clear()
    with pytest.raises(ValueError, match=ERROR_MEDIDA_NO_ENCONTRADA):
        _ = client.post(
            "/generar-plantilla",
            data={
                "apertura": "01_002_A_D",
                "atributo": "bruto",
                "plantilla": "severidad",
            },
        )

    wb.sheets["Indexaciones"].cells(1, 1).value = MEDIDAS
    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_002_A_D", "atributo": "bruto", "plantilla": "severidad"},
    )

    _ = client.post(
        "/guardar-apertura",
        data={"apertura": "01_002_A_D", "atributo": "bruto", "plantilla": "severidad"},
    )

    assert os.path.exists(
        "data/db/wb_test.xlsm_01_002_A_D_bruto_Severidad_UNIDAD_INDEXACION.parquet"
    )

    _ = client.post(
        "/traer-apertura",
        data={"apertura": "01_002_A_D", "atributo": "bruto", "plantilla": "severidad"},
    )

    vaciar_directorios_test()


def test_indexacion_movimiento(client: TestClient, rango_meses: tuple[date, date]):
    vaciar_directorios_test()

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form).json()
    wb = abrir.abrir_plantilla(f"plantillas/{params_form['nombre_plantilla']}.xlsm")

    correr_queries(client)

    _ = client.post("/preparar-plantilla")

    wb.sheets["Indexaciones"].clear()
    wb.sheets["Indexaciones"].cells(1, 1).value = MEDIDAS

    with pytest.raises(FileNotFoundError, match=ERROR_FRECUENCIA_NO_ENCONTRADA):
        _ = client.post(
            "/generar-plantilla",
            data={
                "apertura": "01_002_A_E",
                "atributo": "bruto",
                "plantilla": "severidad",
            },
        )

    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_002_A_E", "atributo": "bruto", "plantilla": "frecuencia"},
    )
    _ = client.post(
        "/guardar-apertura",
        data={"apertura": "01_002_A_E", "atributo": "bruto", "plantilla": "frecuencia"},
    )

    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_002_A_E", "atributo": "bruto", "plantilla": "severidad"},
    )

    _ = client.post(
        "/guardar-apertura",
        data={"apertura": "01_002_A_E", "atributo": "bruto", "plantilla": "severidad"},
    )

    assert os.path.exists(
        "data/db/wb_test.xlsm_01_002_A_E_bruto_Severidad_UNIDAD_INDEXACION.parquet"
    )

    _ = client.post(
        "/traer-apertura",
        data={"apertura": "01_002_A_E", "atributo": "bruto", "plantilla": "severidad"},
    )

    vaciar_directorios_test()


def test_actualizar_indexacion_diferente(
    client: TestClient, rango_meses: tuple[date, date]
):
    vaciar_directorios_test()

    params_form = {
        "negocio": "demo",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    agregar_meses_params(params_form, rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form).json()
    wb = abrir.abrir_plantilla(f"plantillas/{params_form['nombre_plantilla']}.xlsm")

    correr_queries(client)

    _ = client.post("/preparar-plantilla")

    wb.sheets["Indexaciones"].clear()
    wb.sheets["Indexaciones"].cells(1, 1).value = MEDIDAS

    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_002_A_E", "atributo": "bruto", "plantilla": "frecuencia"},
    )
    _ = client.post(
        "/guardar-apertura",
        data={"apertura": "01_002_A_E", "atributo": "bruto", "plantilla": "frecuencia"},
    )

    _ = client.post(
        "/generar-plantilla",
        data={"apertura": "01_002_A_D", "atributo": "bruto", "plantilla": "severidad"},
    )

    with pytest.raises(actualizar.IndexacionDiferenteError):
        _ = client.post(
            "/actualizar-plantilla",
            data={
                "apertura": "01_002_A_E",
                "atributo": "bruto",
                "plantilla": "severidad",
            },
        )

    vaciar_directorios_test()

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import utils
from src.metodos_plantilla import abrir, actualizar
from src.models import Parametros
from tests.conftest import ingresar_parametros

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
    01_041_A_E. Para generar o actualizar la plantilla de severidad
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


@pytest.fixture(autouse=True)
def params(client: TestClient, rango_meses: tuple[date, date]) -> Parametros:
    return ingresar_parametros(
        client,
        Parametros(
            negocio="demo",
            mes_inicio=rango_meses[0],
            mes_corte=rango_meses[1],
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test",
        ),
    )


@pytest.mark.plantilla
def test_indexacion_ocurrencia(client: TestClient, params: Parametros):
    wb = abrir.abrir_plantilla(f"plantillas/{params.nombre_plantilla}.xlsm")

    client.post("/preparar-plantilla")

    wb.sheets["Indexaciones"].clear()
    with pytest.raises(ValueError, match=ERROR_MEDIDA_NO_ENCONTRADA):
        client.post(
            "/generar-plantilla",
            data={
                "apertura": "01_041_A_D",
                "atributo": "bruto",
                "plantilla": "severidad",
            },
        )

    wb.sheets["Indexaciones"].cells(1, 1).value = MEDIDAS
    client.post(
        "/generar-plantilla",
        data={"apertura": "01_041_A_D", "atributo": "bruto", "plantilla": "severidad"},
    )
    client.post(
        "/guardar-apertura",
        data={"apertura": "01_041_A_D", "atributo": "bruto", "plantilla": "severidad"},
    )

    assert Path(
        "data/db/wb_test.xlsm_01_041_A_D_bruto_Severidad_UNIDAD_INDEXACION.parquet"
    ).exists()

    client.post(
        "/traer-apertura",
        data={"apertura": "01_041_A_D", "atributo": "bruto", "plantilla": "severidad"},
    )


@pytest.mark.plantilla
def test_indexacion_movimiento(client: TestClient, rango_meses: tuple[date, date]):
    """
    Tenemos que hacer este test en una plantilla aparte, dado el requerimiento
    de informacion de frecuencia en el tipo de indexacion por fecha de movimiento.
    Cuando los tests hacen cleanup, los archivos almacenados se borran, entonces
    usar esta misma plantilla en otros tests va a generar errores cuando se intente
    preparar, pues se intenta actualizar la plantilla de severidad con informacion
    inexistente.
    """
    p = ingresar_parametros(
        client,
        Parametros(
            negocio="demo",
            mes_inicio=rango_meses[0],
            mes_corte=rango_meses[1],
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test_indexacion_movimiento",
        ),
    )

    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    client.post("/preparar-plantilla")

    wb.sheets["Indexaciones"].clear()
    wb.sheets["Indexaciones"].cells(1, 1).value = MEDIDAS

    with pytest.raises(FileNotFoundError, match=ERROR_FRECUENCIA_NO_ENCONTRADA):
        client.post(
            "/generar-plantilla",
            data={
                "apertura": "01_041_A_E",
                "atributo": "bruto",
                "plantilla": "severidad",
            },
        )

    client.post(
        "/generar-plantilla",
        data={"apertura": "01_041_A_E", "atributo": "bruto", "plantilla": "frecuencia"},
    )
    client.post(
        "/guardar-apertura",
        data={"apertura": "01_041_A_E", "atributo": "bruto", "plantilla": "frecuencia"},
    )

    client.post(
        "/generar-plantilla",
        data={"apertura": "01_041_A_E", "atributo": "bruto", "plantilla": "severidad"},
    )
    client.post(
        "/guardar-apertura",
        data={"apertura": "01_041_A_E", "atributo": "bruto", "plantilla": "severidad"},
    )

    assert Path(
        f"data/db/{p.nombre_plantilla}.xlsm_01_041_A_E_bruto_Severidad_UNIDAD_INDEXACION.parquet"
    ).exists()

    client.post(
        "/traer-apertura",
        data={"apertura": "01_041_A_E", "atributo": "bruto", "plantilla": "severidad"},
    )

    wb.close()
    Path(f"plantillas/{p.nombre_plantilla}.xlsm").unlink()


@pytest.mark.plantilla
def test_actualizar_indexacion_diferente(client: TestClient, params: Parametros):
    wb = abrir.abrir_plantilla(f"plantillas/{params.nombre_plantilla}.xlsm")

    client.post("/preparar-plantilla")

    wb.sheets["Indexaciones"].clear()
    wb.sheets["Indexaciones"].cells(1, 1).value = MEDIDAS

    client.post(
        "/generar-plantilla",
        data={"apertura": "01_041_A_E", "atributo": "bruto", "plantilla": "frecuencia"},
    )
    client.post(
        "/guardar-apertura",
        data={"apertura": "01_041_A_E", "atributo": "bruto", "plantilla": "frecuencia"},
    )

    client.post(
        "/generar-plantilla",
        data={"apertura": "01_041_A_D", "atributo": "bruto", "plantilla": "severidad"},
    )

    with pytest.raises(actualizar.IndexacionDiferenteError):
        client.post(
            "/actualizar-plantilla",
            data={
                "apertura": "01_041_A_E",
                "atributo": "bruto",
                "plantilla": "severidad",
            },
        )

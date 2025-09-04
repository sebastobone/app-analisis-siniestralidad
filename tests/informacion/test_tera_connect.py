from datetime import timedelta

import polars as pl
import pytest
from src import constantes as ct
from src import utils
from src.informacion import tera_connect
from src.models import Parametros
from src.validation import adds


@pytest.fixture
def params() -> Parametros:
    params_form = {
        "negocio": "autonomia",
        "mes_inicio": "201001",
        "mes_corte": "203012",
        "tipo_analisis": "triangulos",
        "nombre_plantilla": "wb_test",
    }
    params = Parametros(**params_form, session_id="test-session-id")
    return Parametros.model_validate(params)


@pytest.mark.fast
def test_reemplazar_parametros_queries(params: Parametros):
    mock_query = """
        SELECT
            {mes_primera_ocurrencia}
            , {mes_corte}
            , {fecha_primera_ocurrencia}
            , {fecha_mes_corte}
        FROM TABLE1
    """

    correct_result = f"""
        SELECT
            {params.mes_inicio}
            , {params.mes_corte}
            , {utils.yyyymm_to_date(params.mes_inicio)}
            , {utils.yyyymm_to_date(params.mes_corte)}
        FROM TABLE1
    """  # noqa: S608

    test = tera_connect._reemplazar_parametros_queries(mock_query, params)

    assert test == correct_result


@pytest.mark.fast
def test_crear_particiones_fechas(params: Parametros):
    test = tera_connect._crear_particiones_fechas(params.mes_inicio, params.mes_corte)

    mes_inicio_date = utils.yyyymm_to_date(params.mes_inicio)
    mes_inicio_next = mes_inicio_date.replace(day=28) + timedelta(days=4)
    mes_inicio_last_day = mes_inicio_next - timedelta(days=mes_inicio_next.day)

    assert test[0] == (utils.yyyymm_to_date(params.mes_inicio), mes_inicio_last_day)


@pytest.mark.asyncio
@pytest.mark.fast
@pytest.mark.parametrize("cantidad", ["siniestros", "primas", "expuestos"])
async def test_cargar_segmentaciones(cantidad: ct.CANTIDADES):
    df = pl.read_excel("data/segmentacion_demo.xlsx", sheet_id=0)
    hojas_segm = [i for i in list(df.keys()) if str(i).startswith("add")]

    result = await tera_connect._obtener_segmentaciones("demo", cantidad)

    assert len(hojas_segm) == len(result)


@pytest.mark.asyncio
@pytest.mark.fast
async def test_nombres_adds():
    with pytest.raises(ValueError):
        await adds.validar_nombre_hojas_segmentacion(["add_d_Siniestros"])

    with pytest.raises(ValueError):
        await adds.validar_nombre_hojas_segmentacion(["add_Siniestros"])

    await adds.validar_nombre_hojas_segmentacion(["add_s_Siniestros"])


@pytest.mark.asyncio
@pytest.mark.fast
async def test_suficiencia_adds():
    queries = [
        "INSERT INTO POLIZAS VALUES (?, ?)",
        "INSERT INTO SUCURSALES VALUES (?, ?)",
        "INSERT INTO CANALES VALUES (?, ?)",
    ]

    with pytest.raises(ValueError):
        await adds.validar_numero_segmentaciones("_", "demo", queries, [pl.DataFrame()])

    await adds.validar_numero_segmentaciones("_", "demo", queries, [pl.DataFrame()] * 3)


@pytest.mark.asyncio
@pytest.mark.fast
async def test_numero_columnas_add():
    mock_query = "INSERT INTO table VALUES (?, ?)"
    mock_add_malo = pl.DataFrame({"datos": [1, 1, 2]})
    mock_add_bueno = pl.DataFrame({"datos": [1, 1, 2], "otro": [1, 1, 2]})

    with pytest.raises(ValueError):
        await adds.validar_numero_columnas_segmentacion(mock_query, mock_add_malo)

    await adds.validar_numero_columnas_segmentacion(mock_query, mock_add_bueno)

from datetime import date, timedelta
from typing import Literal

import polars as pl
import pytest
from src import utils
from src.extraccion import tera_connect
from src.models import Parametros


@pytest.fixture
def params() -> Parametros:
    params_form = {
        "negocio": "autonomia",
        "mes_inicio": "201001",
        "mes_corte": "203012",
        "tipo_analisis": "triangulos",
        "aproximar_reaseguro": "False",
        "nombre_plantilla": "wb_test",
        "add_fraude_soat": "False",
    }
    params = Parametros(**params_form, session_id="test-session-id")
    return Parametros.model_validate(params)


@pytest.mark.unit
def test_determinar_tipo_query():
    assert tera_connect.determinar_tipo_query("path/to/siniestros.sql") == "siniestros"
    assert tera_connect.determinar_tipo_query("path/to/primas.sql") == "primas"
    assert tera_connect.determinar_tipo_query("path/to/expuestos.sql") == "expuestos"


@pytest.mark.unit
def test_reemplazar_parametros_queries(params: Parametros):
    mock_query = """
        SELECT
            {mes_primera_ocurrencia}
            , {mes_corte}
            , {fecha_primera_ocurrencia}
            , {fecha_mes_corte}
            , {aproximar_reaseguro}
        FROM TABLE1
    """

    correct_result = f"""
        SELECT
            {params.mes_inicio}
            , {params.mes_corte}
            , {utils.yyyymm_to_date(params.mes_inicio)}
            , {utils.yyyymm_to_date(params.mes_corte)}
            , {int(params.aproximar_reaseguro)}
        FROM TABLE1
    """  # noqa: S608

    test = tera_connect.reemplazar_parametros_queries(mock_query, params)

    assert test == correct_result


@pytest.mark.unit
def test_crear_particiones_fechas(params: Parametros):
    test = tera_connect.crear_particiones_fechas(params.mes_inicio, params.mes_corte)

    mes_inicio_date = utils.yyyymm_to_date(params.mes_inicio)
    mes_inicio_next = mes_inicio_date.replace(day=28) + timedelta(days=4)
    mes_inicio_last_day = mes_inicio_next - timedelta(days=mes_inicio_next.day)

    assert test[0] == (utils.yyyymm_to_date(params.mes_inicio), mes_inicio_last_day)


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.parametrize("tipo_query", ["siniestros", "primas", "expuestos"])
async def test_cargar_segmentaciones(
    tipo_query: Literal["siniestros", "primas", "expuestos"],
):
    df = pl.read_excel("data/segmentacion_demo.xlsx", sheet_id=0)
    hojas_segm = [i for i in list(df.keys()) if str(i).startswith("add")]

    result = await tera_connect.obtener_segmentaciones(
        "data/segmentacion_demo.xlsx", tipo_query
    )

    assert len(hojas_segm) == len(result)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_check_adds_segmentacion():
    with pytest.raises(ValueError):
        await tera_connect.verificar_nombre_hojas_segmentacion(["add_d_Siniestros"])

    with pytest.raises(ValueError):
        await tera_connect.verificar_nombre_hojas_segmentacion(["add_Siniestros"])

    await tera_connect.verificar_nombre_hojas_segmentacion(["add_s_Siniestros"])


@pytest.mark.asyncio
@pytest.mark.unit
async def test_check_suficiencia_adds():
    mock_query = """
    INSERT INTO POLIZAS VALUES (?, ?);
    INSERT INTO SUCURSALES VALUES (?, ?);
    INSERT INTO CANALES VALUES (?, ?);
    """

    with pytest.raises(ValueError):
        await tera_connect.verificar_numero_segmentaciones(
            "_", mock_query, [pl.DataFrame()]
        )

    await tera_connect.verificar_numero_segmentaciones(
        "_", mock_query, [pl.DataFrame()] * 3
    )


@pytest.mark.asyncio
@pytest.mark.unit
async def test_check_numero_columnas_add():
    mock_query = "INSERT INTO table VALUES (?, ?)"
    mock_add_malo = pl.DataFrame({"datos": [1, 1, 2]})
    mock_add_bueno = pl.DataFrame({"datos": [1, 1, 2], "otro": [1, 1, 2]})

    with pytest.raises(ValueError):
        await tera_connect.verificar_numero_columnas_segmentacion(
            mock_query, mock_add_malo
        )

    await tera_connect.verificar_numero_columnas_segmentacion(
        mock_query, mock_add_bueno
    )


@pytest.mark.asyncio
@pytest.mark.unit
async def test_check_nulls():
    mock_add_malo = pl.DataFrame({"datos": [None, 1, 2, 3, 4, 5]})
    mock_add_bueno = pl.DataFrame({"datos": [1, 1, 2, 3, 4, 5]})

    with pytest.raises(ValueError):
        await tera_connect.verificar_valores_nulos(mock_add_malo)

    await tera_connect.verificar_valores_nulos(mock_add_bueno)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_check_final_info(rango_meses: tuple[date, date]):
    mes_inicio_int = utils.date_to_yyyymm(rango_meses[0])
    mes_corte_int = utils.date_to_yyyymm(rango_meses[1])

    df = utils.generar_mock_siniestros(rango_meses)

    with pytest.raises(ValueError):
        await tera_connect.verificar_resultado(
            "siniestros", df.drop("codigo_op"), "demo", mes_inicio_int, mes_corte_int
        )

    with pytest.raises(ValueError):
        await tera_connect.verificar_resultado(
            "siniestros",
            df.with_columns(pl.col("fecha_siniestro").cast(pl.String)),
            "demo",
            mes_inicio_int,
            mes_corte_int,
        )

    df_fechas_por_fuera = df.slice(0, 2).with_columns(
        fecha_registro=[date(2000, 1, 1), date(2100, 1, 1)],
        fecha_siniestro=[date(2000, 1, 1), date(2100, 1, 1)],
    )

    with pytest.raises(ValueError):
        await tera_connect.verificar_resultado(
            "siniestros",
            df_fechas_por_fuera,
            "demo",
            mes_inicio_int,
            mes_corte_int,
        )

    df_falt = df.slice(0, 2).with_columns(
        [
            pl.lit(None).alias(col)
            for col in utils.obtener_nombres_aperturas("demo", "siniestros")
        ]
    )
    with pytest.raises(ValueError):
        await tera_connect.verificar_resultado(
            "siniestros",
            pl.concat([df, df_falt]),
            "demo",
            mes_inicio_int,
            mes_corte_int,
        )


@pytest.mark.asyncio
@pytest.mark.unit
async def test_verificar_aperturas_faltantes():
    with pytest.raises(ValueError):
        await tera_connect.verificar_aperturas_faltantes(["a", "b"], ["a", "c"])
    await tera_connect.verificar_aperturas_faltantes(["a", "b"], ["a", "b"])
    await tera_connect.verificar_aperturas_sobrantes(["a", "b"], ["a", "b"])
    await tera_connect.verificar_aperturas_sobrantes(["a", "b", "c"], ["a", "b"])

from datetime import date, timedelta
from typing import Literal
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from src import utils
from src.extraccion import tera_connect
from src.models import Parametros


@pytest.fixture
def params(params_form: dict[str, str]) -> Parametros:
    params_form = {
        "negocio": "autonomia",
        "mes_inicio": "201001",
        "mes_corte": "203012",
        "tipo_analisis": "triangulos",
        "aproximar_reaseguro": "False",
        "nombre_plantilla": "plantilla",
        "cuadre_contable_sinis": "False",
        "add_fraude_soat": "False",
        "cuadre_contable_primas": "False",
    }
    params = Parametros(**params_form, session_id="test-session-id")
    return Parametros.model_validate(params)


@pytest.mark.unit
def test_determinar_tipo_query():
    assert tera_connect.determinar_tipo_query("path/to/siniestros.sql") == "siniestros"
    assert tera_connect.determinar_tipo_query("path/to/siniestros_bruto.sql") == "otro"


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
    """

    test = tera_connect.reemplazar_parametros_queries(mock_query, params)

    assert test == correct_result


@pytest.mark.unit
def test_crear_particiones_fechas(params: Parametros):
    test = tera_connect.crear_particiones_fechas(params.mes_inicio, params.mes_corte)

    mes_inicio_date = utils.yyyymm_to_date(params.mes_inicio)
    mes_inicio_next = mes_inicio_date.replace(day=28) + timedelta(days=4)
    mes_inicio_last_day = mes_inicio_next - timedelta(days=mes_inicio_next.day)

    assert test[0] == (utils.yyyymm_to_date(params.mes_inicio), mes_inicio_last_day)


@pytest.mark.unit
@pytest.mark.parametrize(
    "tipo_query, hoja_segm",
    [
        ("siniestros", "add_s_Amparos"),
        ("primas", "add_pe_Canales"),
        ("expuestos", "add_pe_Canales"),
    ],
)
@patch("src.extraccion.tera_connect.pd.ExcelFile")
@patch("src.extraccion.tera_connect.pl.read_excel")
def test_cargar_segmentaciones(
    mock_read_excel: MagicMock,
    mock_excel_file: MagicMock,
    tipo_query: Literal["siniestros", "primas", "expuestos"],
    hoja_segm: str,
):
    mock_excel_file.return_value.sheet_names = [hoja_segm]

    mock_read_excel.return_value = pl.DataFrame({"col1": [1, 2, 3]})

    result = tera_connect.obtener_segmentaciones("test_file.xlsx", tipo_query)

    assert len(result) == 1
    mock_read_excel.assert_called_once_with("test_file.xlsx", sheet_name=hoja_segm)
    mock_excel_file.assert_called_once_with("test_file.xlsx")


@pytest.mark.unit
def test_check_adds_segmentacion():
    with pytest.raises(ValueError):
        tera_connect.verificar_nombre_hojas_segmentacion(["add_d_Siniestros"])

    with pytest.raises(ValueError):
        tera_connect.verificar_nombre_hojas_segmentacion(["add_Siniestros"])

    tera_connect.verificar_nombre_hojas_segmentacion(["add_s_Siniestros"])


@pytest.mark.unit
def test_check_suficiencia_adds():
    mock_query = """
    INSERT INTO POLIZAS VALUES (?, ?);
    INSERT INTO SUCURSALES VALUES (?, ?);
    INSERT INTO CANALES VALUES (?, ?);
    """

    with pytest.raises(ValueError):
        tera_connect.verificar_numero_segmentaciones("_", mock_query, [pl.DataFrame()])

    tera_connect.verificar_numero_segmentaciones("_", mock_query, [pl.DataFrame()] * 3)


@pytest.mark.unit
def test_check_numero_columnas_add():
    mock_query = "INSERT INTO table VALUES (?, ?)"
    mock_add_malo = pl.DataFrame({"datos": [1, 1, 2]})
    mock_add_bueno = pl.DataFrame({"datos": [1, 1, 2], "otro": [1, 1, 2]})

    with pytest.raises(ValueError):
        tera_connect.verificar_numero_columnas_segmentacion(mock_query, mock_add_malo)

    tera_connect.verificar_numero_columnas_segmentacion(mock_query, mock_add_bueno)


@pytest.mark.unit
def test_check_nulls():
    mock_add_malo = pl.DataFrame({"datos": [None, 1, 2, 3, 4, 5]})
    mock_add_bueno = pl.DataFrame({"datos": [1, 1, 2, 3, 4, 5]})

    with pytest.raises(ValueError):
        tera_connect.verificar_valores_nulos(mock_add_malo)

    tera_connect.verificar_valores_nulos(mock_add_bueno)


@pytest.mark.unit
def test_check_final_info(
    mock_siniestros: pl.LazyFrame, mes_inicio: date, mes_corte: date
):
    mes_inicio_int = utils.date_to_yyyymm(mes_inicio)
    mes_corte_int = utils.date_to_yyyymm(mes_corte)

    tipo_query = "siniestros"
    df = mock_siniestros.collect()

    with pytest.raises(ValueError):
        tera_connect.verificar_resultado_siniestros_primas_expuestos(
            tipo_query, df.drop("codigo_op"), "mock", mes_inicio_int, mes_corte_int
        )

    with pytest.raises(ValueError):
        tera_connect.verificar_resultado_siniestros_primas_expuestos(
            tipo_query,
            df.with_columns(pl.col("fecha_siniestro").cast(pl.String)),
            "mock",
            mes_inicio_int,
            mes_corte_int,
        )

    df_fechas_por_fuera = df.slice(0, 2).with_columns(
        fecha_registro=[date(2000, 1, 1), date(2100, 1, 1)],
        fecha_siniestro=[date(2000, 1, 1), date(2100, 1, 1)],
    )

    with pytest.raises(ValueError):
        tera_connect.verificar_resultado_siniestros_primas_expuestos(
            tipo_query,
            df_fechas_por_fuera,
            "mock",
            mes_inicio_int,
            mes_corte_int,
        )

    df_falt = df.slice(0, 2).with_columns(
        [pl.lit(None).alias(col) for col in utils.columnas_aperturas("mock")]
    )
    with pytest.raises(ValueError):
        tera_connect.verificar_resultado_siniestros_primas_expuestos(
            tipo_query, pl.concat([df, df_falt]), "mock", mes_inicio_int, mes_corte_int
        )

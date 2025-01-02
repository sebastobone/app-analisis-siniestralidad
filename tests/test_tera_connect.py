from datetime import timedelta
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from src import constantes as ct
from src import utils
from src.app import Parametros
from src.extraccion import tera_connect


@pytest.mark.unit
def test_tipo_query():
    assert tera_connect.tipo_query("path/to/siniestros.sql") == "siniestros"
    assert tera_connect.tipo_query("path/to/siniestros_bruto.sql") == "otro"


@pytest.mark.unit
def test_preparar_queries(params: Parametros):
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
            , {params.aproximar_reaseguro}
        FROM TABLE1
    """

    test = tera_connect.preparar_queries(
        mock_query, params.mes_inicio, params.mes_corte, params.aproximar_reaseguro
    )

    assert test == correct_result


@pytest.mark.unit
def test_fechas_chunks(params: Parametros):
    test = tera_connect.fechas_chunks(params.mes_inicio, params.mes_corte)
    print(test[0])

    mes_inicio_date = utils.yyyymm_to_date(params.mes_inicio)
    mes_inicio_next = mes_inicio_date.replace(day=28) + timedelta(days=4)
    mes_inicio_last_day = mes_inicio_next - timedelta(days=mes_inicio_next.day)

    assert test[0] == (utils.yyyymm_to_date(params.mes_inicio), mes_inicio_last_day)


@pytest.mark.unit
@patch("src.extraccion.tera_connect.pd.ExcelFile")
@patch("src.extraccion.tera_connect.pl.read_excel")
def test_cargar_segmentaciones(mock_read_excel: MagicMock, mock_excel_file: MagicMock):
    mock_excel_file.return_value.sheet_names = [
        "add_s_Amparos",
        "add_pe_Canales",
    ]

    mock_read_excel.return_value = pl.DataFrame({"col1": [1, 2, 3]})

    result = tera_connect.cargar_segmentaciones("test_file.xlsx", "siniestros")

    assert len(result) == 1
    mock_read_excel.assert_called_once_with(
        "test_file.xlsx", sheet_name="add_s_Amparos"
    )
    mock_excel_file.assert_called_once_with("test_file.xlsx")


@pytest.mark.unit
def test_check_adds_segmentacion():
    with pytest.raises(ValueError):
        tera_connect.check_adds_segmentacion(["add_d_Siniestros"])

    with pytest.raises(ValueError):
        tera_connect.check_adds_segmentacion(["add_Siniestros"])

    tera_connect.check_adds_segmentacion(["add_s_Siniestros"])


@pytest.mark.unit
def test_check_suficiencia_adds():
    mock_query = """
    INSERT INTO POLIZAS VALUES (?, ?);
    INSERT INTO SUCURSALES VALUES (?, ?);
    INSERT INTO CANALES VALUES (?, ?);
    """

    with pytest.raises(ValueError):
        tera_connect.check_suficiencia_adds("_", mock_query, [pl.DataFrame()])

    tera_connect.check_suficiencia_adds("_", mock_query, [pl.DataFrame()] * 3)


@pytest.mark.unit
def test_check_numero_columnas_add():
    mock_query = "INSERT INTO table VALUES (?, ?)"
    mock_add_malo = pl.DataFrame({"datos": [1, 1, 2]})
    mock_add_bueno = pl.DataFrame({"datos": [1, 1, 2], "otro": [1, 1, 2]})

    with pytest.raises(ValueError):
        tera_connect.check_numero_columnas_add(mock_query, mock_add_malo)

    tera_connect.check_numero_columnas_add(mock_query, mock_add_bueno)


@pytest.mark.unit
def test_check_nulls():
    mock_add_malo = pl.DataFrame({"datos": [None, 1, 2, 3, 4, 5]})
    mock_add_bueno = pl.DataFrame({"datos": [1, 1, 2, 3, 4, 5]})

    with pytest.raises(ValueError):
        tera_connect.check_nulls(mock_add_malo)

    tera_connect.check_nulls(mock_add_bueno)


@pytest.mark.unit
def test_check_final_info(mock_siniestros: pl.LazyFrame):
    tipo_query = "siniestros"
    df = mock_siniestros.collect()

    with pytest.raises(ValueError) as exc_info:
        tera_connect.check_final_info(tipo_query, df.drop("codigo_op"), "mock")
    assert str(exc_info.value) == """¡Falta la columna codigo_op!"""

    with pytest.raises(ValueError) as exc_info:
        tera_connect.check_final_info(
            tipo_query,
            df.with_columns(pl.col("fecha_siniestro").cast(pl.String)),
            "mock",
        )

    df_falt = df.slice(0, 2).with_columns(
        [pl.lit(None).alias(col) for col in ct.columnas_aperturas("mock")]
    )
    with pytest.raises(ValueError) as exc_info:
        tera_connect.check_final_info(tipo_query, pl.concat([df, df_falt]), "mock")

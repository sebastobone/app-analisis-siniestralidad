from datetime import timedelta

import polars as pl
import pytest
from src import utils
from src.app import Parametros
from src.extraccion import tera_connect


def test_tipo_query():
    assert tera_connect.tipo_query("path/to/siniestros.sql") == "siniestros"
    assert tera_connect.tipo_query("path/to/siniestros_bruto.sql") == "otro"


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


def test_fechas_chunks(params: Parametros):
    test = tera_connect.fechas_chunks(params.mes_inicio, params.mes_corte)
    print(test[0])

    mes_inicio_date = utils.yyyymm_to_date(params.mes_inicio)
    mes_inicio_next = mes_inicio_date.replace(day=28) + timedelta(days=4)
    mes_inicio_last_day = mes_inicio_next - timedelta(days=mes_inicio_next.day)

    assert test[0] == (utils.yyyymm_to_date(params.mes_inicio), mes_inicio_last_day)


def test_check_adds_segmentacion():
    with pytest.raises(ValueError) as exc_info:
        tera_connect.check_adds_segmentacion(["add_d_Siniestros"])
    print(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        tera_connect.check_adds_segmentacion(["add_Siniestros"])
    print(exc_info.value)

    assert tera_connect.check_adds_segmentacion(["add_s_Siniestros"]) is None


def test_check_suficiencia_adds():
    mock_query = """

    INSERT INTO POLIZAS VALUES (?, ?);
    INSERT INTO SUCURSALES VALUES (?, ?);
    INSERT INTO CANALES VALUES (?, ?);

    """

    with pytest.raises(ValueError) as exc_info:
        tera_connect.check_suficiencia_adds("siniestros", mock_query, ["add_d_Polizas"])
    print(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        tera_connect.check_suficiencia_adds(
            "siniestros", mock_query, ["add_s_Polizas", "add_s_Sucursales"]
        )
    print(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        tera_connect.check_suficiencia_adds(
            "siniestros",
            mock_query,
            ["add_s_Polizas", "add_s_Sucursales", "add_s_Canales", "add_s_Random"],
        )
    print(exc_info.value)

    assert (
        tera_connect.check_suficiencia_adds(
            "siniestros",
            mock_query,
            ["add_s_Polizas", "add_s_Sucursales", "add_s_Canales"],
        )
        is None
    )


def test_check_numero_columnas_add():
    mock_add = pl.DataFrame({"datos": [1, 1, 2, 3, 4, 5]})
    mock_query = "INSERT INTO table VALUES (?, ?)"
    with pytest.raises(ValueError) as exc_info:
        tera_connect.check_numero_columnas_add(mock_query, mock_add)
    print(exc_info.value)

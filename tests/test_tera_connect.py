import pytest
import polars as pl
from src.extraccion import tera_connect


def test_check_adds_segmentacion():
    with pytest.raises(Exception) as exc_info:
        tera_connect.check_adds_segmentacion(["add_d_Siniestros"])
    print(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        tera_connect.check_adds_segmentacion(["add_Siniestros"])
    print(exc_info.value)

    assert tera_connect.check_adds_segmentacion(["add_s_Siniestros"]) is None


def test_check_suficiencia_adds():
    mock_query = """

    INSERT INTO POLIZAS VALUES (?, ?);
    INSERT INTO SUCURSALES VALUES (?, ?);
    INSERT INTO CANALES VALUES (?, ?);

    """

    with pytest.raises(Exception) as exc_info:
        tera_connect.check_suficiencia_adds("siniestros", mock_query, ["add_d_Polizas"])
    print(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        tera_connect.check_suficiencia_adds(
            "siniestros", mock_query, ["add_s_Polizas", "add_s_Sucursales"]
        )
    print(exc_info.value)

    with pytest.raises(Exception) as exc_info:
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
    mock_df = pl.DataFrame({"datos": [1, 1, 2, 3, 4, 5]})
    mock_query = "INSERT INTO table VALUES (?, ?)"
    with pytest.raises(Exception) as exc_info:
        tera_connect.check_numero_columnas_add("siniestros", mock_query, mock_df)
    print(exc_info.value)


def test_check_duplicados():
    mock_df = pl.DataFrame({"datos": [1, 1, 2, 3, 4, 5]})
    with pytest.raises(Exception) as exc_info:
        tera_connect.check_duplicados(mock_df)
    print(exc_info.value)

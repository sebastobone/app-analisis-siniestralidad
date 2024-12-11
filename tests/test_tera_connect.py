import pytest
from src.extraccion.tera_connect import check_suficiencia_adds


def test_check_suficiencia_adds():
    mock_query = """

    INSERT INTO POLIZAS VALUES (?, ?);
    INSERT INTO SUCURSALES VALUES (?, ?);
    INSERT INTO CANALES VALUES (?, ?);

    """

    with pytest.raises(Exception) as exc_info:
        check_suficiencia_adds("siniestros", mock_query, ["add_d_Polizas"])
    print(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        check_suficiencia_adds(
            "siniestros", mock_query, ["add_s_Polizas", "add_s_Sucursales"]
        )
    print(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        check_suficiencia_adds(
            "siniestros",
            mock_query,
            ["add_s_Polizas", "add_s_Sucursales", "add_s_Canales", "add_s_Random"],
        )
    print(exc_info.value)

    assert (
        check_suficiencia_adds(
            "siniestros",
            mock_query,
            ["add_s_Polizas", "add_s_Sucursales", "add_s_Canales"],
        )
        == None
    )
    # check_suficiencia_adds("siniestros", mock_query, ["add_s_Polizas", "add_s_Sucursales", "add_s_Canales"])

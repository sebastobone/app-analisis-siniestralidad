import polars as pl
from unittest.mock import patch
from src.controles_informacion import controles_informacion as ctrl


@patch("src.controles_informacion.controles_informacion.pl.read_excel")
def test_leer_sap(mock_read_excel):
    mock_read_excel.return_value = pl.DataFrame(
        {
            "Ejercicio/Período": ["2024"],
            "Ramo Agr": ["R1"],
            "Resultado total": [100],
        }
    )
    cias = ["Generales"]
    qtys = ["retenido"]
    mes_corte = 202401
    result = ctrl.leer_sap(cias, qtys, mes_corte).collect()
    assert "codigo_op" in result.columns
    assert result.shape[0] > 0

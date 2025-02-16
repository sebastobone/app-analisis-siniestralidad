import os
from datetime import date

import polars as pl
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlmodel import Session, select
from src import constantes as ct
from src import utils
from src.metodos_plantilla import abrir
from src.models import Parametros
from tests.conftest import assert_igual
from tests.metodos_plantilla.conftest import agregar_meses_params


@pytest.mark.plantilla
@pytest.mark.integration
@pytest.mark.parametrize(
    "params_form",
    [
        {"negocio": "mock", "tipo_analisis": "triangulos", "nombre_plantilla": "test1"},
        {"negocio": "mock", "tipo_analisis": "entremes", "nombre_plantilla": "test2"},
    ],
)
def test_preparar_plantilla(
    client: TestClient,
    test_session: Session,
    params_form: dict[str, str],
    rango_meses: tuple[date, date],
):
    agregar_meses_params(params_form, *rango_meses)

    _ = client.post("/ingresar-parametros", data=params_form)
    p = test_session.exec(select(Parametros)).all()[0]

    response = client.post("/preparar-plantilla")

    assert response.status_code == status.HTTP_200_OK
    assert os.path.exists(f"plantillas/{p.nombre_plantilla}.xlsm")

    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    assert wb.sheets["Main"]["A4"].value == "Mes corte"
    assert wb.sheets["Main"]["B4"].value == p.mes_corte
    assert wb.sheets["Main"]["A5"].value == "Mes anterior"
    assert wb.sheets["Main"]["B5"].value == (
        p.mes_corte - 1
        if p.mes_corte % 100 != 1
        else ((p.mes_corte // 100) - 1) * 100 + 12
    )

    if p.tipo_analisis == "triangulos":
        assert not wb.sheets["Plantilla_Entremes"].visible
        assert wb.sheets["Plantilla_Frec"].visible
        assert wb.sheets["Plantilla_Seve"].visible
        assert wb.sheets["Plantilla_Plata"].visible
    elif p.tipo_analisis == "entremes":
        assert wb.sheets["Plantilla_Entremes"].visible
        assert not wb.sheets["Plantilla_Frec"].visible
        assert not wb.sheets["Plantilla_Seve"].visible
        assert not wb.sheets["Plantilla_Plata"].visible

    assert "aperturas" in [table.name for table in wb.sheets["Main"].tables]
    assert "periodicidades" in [table.name for table in wb.sheets["Main"].tables]

    base_triangulos = pl.read_parquet("data/processed/base_triangulos.parquet")

    df_original = base_triangulos.filter(
        (pl.col("periodicidad_ocurrencia") == "Trimestral") & (pl.col("diagonal") == 1)
    ).select(
        ["apertura_reservas", "periodicidad_ocurrencia", "periodo_ocurrencia"]
        + ct.COLUMNAS_QTYS
    )

    if p.tipo_analisis == "entremes":
        base_ult_ocurr = pl.read_parquet(
            "data/processed/base_ultima_ocurrencia.parquet"
        )
        df_original = df_original.filter(
            pl.col("periodo_ocurrencia")
            != pl.col("periodo_ocurrencia").max().over("apertura_reservas")
        ).vstack(
            base_ult_ocurr.filter(
                pl.col("periodicidad_triangulo") == "Trimestral"
            ).drop("periodicidad_triangulo")
        )

    df_plantilla = utils.sheet_to_dataframe(wb, "Aux_Totales").collect()

    assert df_original.shape[0] == df_plantilla.shape[0]
    assert_igual(df_original, df_plantilla, "pago_bruto")

    wb.close()
    os.remove(f"plantillas/{p.nombre_plantilla}.xlsm")

import polars as pl
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, select
from src import utils
from src.controles_informacion.controles_informacion import consolidar_sap
from src.models import Parametros


@pytest.mark.soat
@pytest.mark.integration
@pytest.mark.teradata
def test_info_soat(client: TestClient, test_session: Session):
    params = {
        "negocio": "soat",
        "mes_inicio": "201901",
        "mes_corte": "202401",
        "tipo_analisis": "triangulos",
        "aproximar_reaseguro": "False",
        "nombre_plantilla": "plantilla_test_soat",
        "cuadre_contable_sinis": "True",
        "add_fraude_soat": "True",
        "cuadre_contable_primas": "False",
    }

    _ = client.post("/ingresar-parametros", data=params)
    p = test_session.exec(select(Parametros)).all()[0]

    lista_vehiculos = ["AUTO", "MOTO"]
    lista_canales = ["ASESORES", "CERRADO", "CORBETA", "DIGITAL", "EXITO", "RESTO"]

    for query in ["siniestros", "primas", "expuestos"]:
        _ = client.post(f"/correr-query-{query}")
        df = pl.read_parquet(f"data/raw/{query}.parquet")
        assert (
            sorted(df.get_column("apertura_canal_desc").unique().to_list())
            == lista_canales
        )
        assert (
            sorted(df.get_column("tipo_vehiculo").unique().to_list()) == lista_vehiculos
        )

    _ = client.post("/generar-controles")

    mes_corte_dt = utils.yyyymm_to_date(p.mes_corte)

    df_sinis_pre_cuadre = pl.read_parquet("data/raw/siniestros_pre_cuadre.parquet")
    df_sinis_post_cuadre = pl.read_parquet("data/raw/siniestros_post_cuadre.parquet")
    df_sinis_post_ajustes = pl.read_parquet("data/raw/siniestros.parquet")

    df_ajustes_fraude = (
        pl.read_excel("data/segmentacion_soat.xlsx", sheet_name="Ajustes_Fraude")
        .drop("tipo_ajuste")
        .filter(pl.col("fecha_registro") <= mes_corte_dt)
    )

    sap = consolidar_sap(
        ["Generales"],
        ["pago_bruto", "pago_retenido", "aviso_bruto", "aviso_retenido"],
        p.mes_corte,
    ).filter(
        (pl.col("fecha_registro") == mes_corte_dt) & (pl.col("codigo_ramo_op") == "041")
    )

    for col in ["pago_bruto", "pago_retenido", "aviso_bruto", "aviso_retenido"]:
        assert (
            abs(
                df_sinis_pre_cuadre.filter(pl.col("fecha_registro") != mes_corte_dt)
                .get_column(col)
                .sum()
                - df_sinis_post_cuadre.filter(pl.col("fecha_registro") != mes_corte_dt)
                .get_column(col)
                .sum()
            )
            < 100
        )
        assert (
            abs(
                df_sinis_post_cuadre.filter(pl.col("fecha_registro") == mes_corte_dt)
                .get_column(col)
                .sum()
                - sap.get_column(col).sum()
            )
            < 100
        )
        assert (
            abs(
                df_sinis_post_ajustes.get_column(col).sum()
                - df_sinis_post_cuadre.get_column(col).sum()
                - df_ajustes_fraude.get_column(col).sum()
            )
            < 100
        )

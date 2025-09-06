from datetime import date

import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import utils
from src.models import Parametros

from tests.conftest import assert_igual, correr_queries, ingresar_parametros


def separar_meses_anteriores(
    df: pl.DataFrame, mes_corte: date
) -> tuple[pl.DataFrame, pl.DataFrame]:
    df_ant = df.filter(pl.col("fecha_registro").dt.month_start() < mes_corte)
    df_ult = df.filter(pl.col("fecha_registro").dt.month_start() == mes_corte)
    return df_ant, df_ult


@pytest.mark.autonomia
@pytest.mark.teradata
@pytest.mark.asyncio
async def test_info_autonomia(client: TestClient) -> None:
    p = ingresar_parametros(
        client,
        Parametros(
            negocio="autonomia",
            mes_inicio=date(2024, 1, 1),
            mes_corte=date(2024, 12, 31),
            tipo_analisis="triangulos",
            nombre_plantilla="plantilla_autonomia",
        ),
    )

    correr_queries(client)

    base_siniestros = pl.read_parquet("data/raw/siniestros.parquet").with_columns(
        pago_cedido=pl.col("pago_bruto") - pl.col("pago_retenido"),
        aviso_cedido=pl.col("aviso_bruto") - pl.col("aviso_retenido"),
    )

    _ = client.post("/generar-controles")

    siniestros_cedidos_sap = pl.read_excel(
        "data/segmentacion_autonomia.xlsx", sheet_name="add_s_SAP_Sinis_Ced"
    )
    incurridos_atipicos = utils.lowercase_columns(
        pl.read_excel(
            "data/segmentacion_autonomia.xlsx", sheet_name="add_s_Inc_Ced_Atipicos"
        )
    )

    siniestros_ultimo_mes = separar_meses_anteriores(base_siniestros, p.mes_corte)[1]

    for col in ["pago_cedido", "aviso_cedido"]:
        assert_igual(
            incurridos_atipicos,
            siniestros_ultimo_mes.filter(pl.col("atipico") == 1),
            col,
        )
        assert_igual(siniestros_ultimo_mes, siniestros_cedidos_sap, col)

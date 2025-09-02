import tempfile
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from src import constantes as ct
from src import utils
from src.controles_informacion import generacion as gen
from src.controles_informacion import sap
from src.informacion.mocks import generar_mock


@pytest.mark.fast
@pytest.mark.parametrize(
    "qtys, expected",
    [
        (
            ["prima_bruta", "prima_cedida"],
            set(ct.Valores().model_dump()["primas"].keys()),
        ),
        (
            ["pago_retenido", "aviso_bruto"],
            set(("pago_bruto", "pago_cedido", "aviso_bruto", "aviso_cedido")),
        ),
        (["rpnd_retenido"], set(("rpnd_bruto", "rpnd_cedido"))),
    ],
)
def test_definir_hojas_afo(qtys: list[str], expected: set[str]):
    assert sap.definir_hojas_afo(qtys) == expected


def fechas_sap(mes_corte: int) -> list[date]:
    return pl.date_range(
        utils.yyyymm_to_date(201001),
        utils.yyyymm_to_date(mes_corte),
        interval="1mo",
        eager=True,
    ).to_list()


def mock_hoja_afo(mes_corte: int, qty: str) -> pl.DataFrame:
    fechas = fechas_sap(mes_corte)
    num_rows = len(fechas)

    signo = sap.signo_sap(qty)

    return pl.DataFrame(
        {
            "Ejercicio/Período": [
                f"{ct.NOMBRE_MES[fecha.month]} {fecha.year}" for fecha in fechas
            ],
            "column_1": ["Importe ML" for _ in range(num_rows)],
            sap.columna_ramo_sap(qty): ["COP" for _ in range(num_rows)],
            "001": np.random.random(size=num_rows) * signo * 1e9,
            "002": np.random.random(size=num_rows) * signo * 1e9,
            "003": np.random.random(size=num_rows) * signo * 1e9,
        }
    ).with_columns(pl.sum_horizontal("001", "002", "003").alias("Resultado total"))


@pytest.mark.asyncio
@pytest.mark.fast
@pytest.mark.parametrize("cia", ["Vida", "Generales"])
@pytest.mark.parametrize(
    "qty",
    [
        "pago_bruto",
        "pago_cedido",
        "aviso_bruto",
        "aviso_cedido",
        "prima_bruta",
        "prima_bruta_devengada",
        "prima_retenida",
        "prima_retenida_devengada",
    ],
)
async def test_transformar_hoja_afo(cia: str, qty: str, rango_meses: tuple[date, date]):
    mes_corte_int = utils.date_to_yyyymm(rango_meses[1])
    df_original = mock_hoja_afo(mes_corte_int, qty)
    sum_original = sum(
        [df_original.get_column(column).sum() for column in ["001", "002", "003"]]
    ) * sap.signo_sap(qty)

    df_processed = await sap.transformar_hoja_afo(df_original, cia, qty, mes_corte_int)
    sum_processed = df_processed.get_column(qty).sum()

    assert abs(sum_original - sum_processed) < 100

    hoja_afo_incompleta = df_original.slice(0, df_original.shape[0] - 1)
    with pytest.raises(ValueError):
        await sap.transformar_hoja_afo(hoja_afo_incompleta, cia, qty, mes_corte_int)


@pytest.mark.asyncio
@pytest.mark.fast
@pytest.mark.parametrize(
    "qtys, mes_corte, expected_columns",
    [
        (["prima_bruta", "prima_cedida"], 202201, ["prima_bruta", "prima_cedida"]),
        (["pago_retenido"], 202201, ["pago_retenido"]),
    ],
)
async def test_consolidar_sap(qtys, mes_corte, expected_columns):
    result = await sap.consolidar_sap("autonomia", qtys, mes_corte)
    assert (
        result.collect_schema().names()
        == ["codigo_op", "codigo_ramo_op", "fecha_registro"] + expected_columns
    )
    assert result.shape[0] > 0


@pytest.mark.asyncio
@pytest.mark.fast
async def test_verificar_existencia_afos():
    with pytest.raises(FileNotFoundError):
        await gen.verificar_existencia_afos("demo")


@pytest.mark.asyncio
@pytest.mark.fast
async def test_restablecer_archivos_queries():
    dates = (202001, 202412)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)

        siniestros = generar_mock(*dates, "siniestros")
        primas = generar_mock(*dates, "primas")
        expuestos = generar_mock(*dates, "expuestos")

        await gen.restablecer_salidas_queries(tmp_dir)

        for sufijo in ["", "_teradata"]:
            siniestros.write_parquet(tmp_dir_path / f"siniestros{sufijo}.parquet")
            siniestros.write_parquet(tmp_dir_path / f"siniestros{sufijo}.csv")
            primas.write_parquet(tmp_dir_path / f"primas{sufijo}.parquet")
            primas.write_parquet(tmp_dir_path / f"primas{sufijo}.csv")
            expuestos.write_parquet(tmp_dir_path / f"expuestos{sufijo}.parquet")
            expuestos.write_parquet(tmp_dir_path / f"expuestos{sufijo}.csv")

        await gen.restablecer_salidas_queries(tmp_dir)

        archivos_guardados = sorted([file.name for file in tmp_dir_path.iterdir()])
        assert archivos_guardados == sorted(gen.ARCHIVOS_PERMANENTES)

        for base in ["siniestros", "primas"]:
            for suffix in ["_pre_cuadre", "_post_cuadre"]:
                for ext in [".parquet", ".csv"]:
                    siniestros.write_parquet(tmp_dir_path / f"{base}{suffix}{ext}")

        await gen.restablecer_salidas_queries(tmp_dir)

        archivos_guardados = sorted([file.name for file in tmp_dir_path.iterdir()])
        assert archivos_guardados == sorted(gen.ARCHIVOS_PERMANENTES)

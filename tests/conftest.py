import os
import sys
from datetime import date

import numpy as np
import polars as pl
import pytest
from src import utils
from src.app import Parametros

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))


@pytest.fixture
def mock_siniestros(params: Parametros) -> pl.LazyFrame:
    num_rows = 100000
    return pl.LazyFrame(
        {
            "codigo_op": np.random.choice(["01", "02"], size=num_rows),
            "codigo_ramo_op": np.random.choice(["001", "002", "003"], size=num_rows),
            "ramo_desc": ["" for i in range(num_rows)],
            "apertura_1": np.random.choice(["A", "B", "C"], size=num_rows),
            "apertura_2": np.random.choice(["D", "E", "F"], size=num_rows),
            "atipico": np.random.choice([0, 1], size=num_rows, p=[0.95, 0.05]),
            "fecha_siniestro": np.random.choice(
                pl.date_range(
                    utils.yyyymm_to_date(params.mes_inicio),
                    date(2030, 1, 1),
                    interval="1mo",
                    eager=True,
                ),
                size=num_rows,
            ),
            "fecha_registro": np.random.choice(
                pl.date_range(
                    utils.yyyymm_to_date(params.mes_inicio),
                    date(2030, 1, 1),
                    interval="1mo",
                    eager=True,
                ),
                size=num_rows,
            ),
            "pago_bruto": np.random.random(size=num_rows) * 1e8,
            "pago_retenido": np.random.random(size=num_rows) * 1e8,
            "aviso_bruto": np.random.random(size=num_rows) * 1e7,
            "aviso_retenido": np.random.random(size=num_rows) * 1e7,
            "conteo_pago": np.random.randint(0, 100, size=num_rows),
            "conteo_incurrido": np.random.randint(0, 110, size=num_rows),
            "conteo_desistido": np.random.randint(0, 10, size=num_rows),
        }
    ).with_columns(
        utils.col_apertura_reservas("mock"),
        ramo_desc=pl.when(pl.col("codigo_ramo_op") == "001")
        .then(pl.lit("RAMO1"))
        .when(pl.col("codigo_ramo_op") == "002")
        .then(pl.lit("RAMO2"))
        .otherwise(pl.lit("RAMO3")),
    )


@pytest.fixture
def mock_primas(params: Parametros) -> pl.LazyFrame:
    num_rows = 10000
    return pl.LazyFrame(
        {
            "codigo_op": np.random.choice(["01", "02"], size=num_rows),
            "codigo_ramo_op": np.random.choice(["001", "002", "003"], size=num_rows),
            "ramo_desc": np.random.choice(["RAMO1", "RAMO2", "RAMO3"], size=num_rows),
            "apertura_1": np.random.choice(["A", "B", "C"], size=num_rows),
            "apertura_2": np.random.choice(["D", "E", "F"], size=num_rows),
            "fecha_registro": np.random.choice(
                pl.date_range(
                    utils.yyyymm_to_date(params.mes_inicio),
                    date(2030, 1, 1),
                    interval="1mo",
                    eager=True,
                ),
                size=num_rows,
            ),
            "prima_bruta": np.random.random(size=num_rows) * 1e8,
            "prima_retenida": np.random.random(size=num_rows) * 1e8,
            "prima_bruta_devengada": np.random.random(size=num_rows) * 1e8,
            "prima_retenida_devengada": np.random.random(size=num_rows) * 1e8,
        }
    ).with_columns(
        utils.col_apertura_reservas("mock"),
        ramo_desc=pl.when(pl.col("codigo_ramo_op") == "001")
        .then(pl.lit("RAMO1"))
        .when(pl.col("codigo_ramo_op") == "002")
        .then(pl.lit("RAMO2"))
        .otherwise(pl.lit("RAMO3")),
    )


@pytest.fixture
def mock_expuestos(params: Parametros) -> pl.LazyFrame:
    num_rows = 10000
    return (
        pl.LazyFrame(
            {
                "codigo_op": np.random.choice(["01", "02"], size=num_rows),
                "codigo_ramo_op": np.random.choice(
                    ["001", "002", "003"], size=num_rows
                ),
                "ramo_desc": np.random.choice(
                    ["RAMO1", "RAMO2", "RAMO3"], size=num_rows
                ),
                "apertura_1": np.random.choice(["A", "B", "C"], size=num_rows),
                "apertura_2": np.random.choice(["D", "E", "F"], size=num_rows),
                "fecha_registro": np.random.choice(
                    pl.date_range(
                        utils.yyyymm_to_date(params.mes_inicio),
                        date(2030, 1, 1),
                        interval="1mo",
                        eager=True,
                    ),
                    size=num_rows,
                ),
                "expuestos": np.random.random(size=num_rows) * 1e6,
                "vigentes": np.random.random(size=num_rows) * 1e6,
            }
        )
        .with_columns(
            utils.col_apertura_reservas("mock"),
            ramo_desc=pl.when(pl.col("codigo_ramo_op") == "001")
            .then(pl.lit("RAMO1"))
            .when(pl.col("codigo_ramo_op") == "002")
            .then(pl.lit("RAMO2"))
            .otherwise(pl.lit("RAMO3")),
        )
        .group_by(
            [
                "apertura_reservas",
                "codigo_op",
                "codigo_ramo_op",
                "ramo_desc",
                "apertura_1",
                "apertura_2",
                "fecha_registro",
            ]
        )
        .mean()
    )


@pytest.fixture
def params_form() -> dict[str, str]:
    return {
        "negocio": "autonomia",
        "mes_inicio": "201812",
        "mes_corte": "202312",
        "tipo_analisis": "triangulos",
        "aproximar_reaseguro": "False",
        "nombre_plantilla": "plantilla",
        "cuadre_contable_sinis": "False",
        "add_fraude_soat": "False",
        "cuadre_contable_primas": "False",
    }


@pytest.fixture
def params(params_form: dict[str, str]) -> Parametros:
    params = Parametros(**params_form, session_id="test-session-id")
    return Parametros.model_validate(params)

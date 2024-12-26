import sys
import os
import pytest
import polars as pl
import numpy as np
from src import utils
from datetime import date, timedelta
from src.app import Parametros

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))


@pytest.fixture
def mock_siniestros() -> pl.LazyFrame:
    num_rows = 100000
    return pl.LazyFrame(
        {
            "codigo_op": np.random.choice(["01", "02"], size=num_rows),
            "codigo_ramo_op": np.random.choice(["001", "002", "003"], size=num_rows),
            "ramo_desc": np.random.choice(["RAMO1", "RAMO2", "RAMO3"], size=num_rows),
            "apertura_1": np.random.choice(["A", "B", "C"], size=num_rows),
            "apertura_2": np.random.choice(["D", "E", "F"], size=num_rows),
            "atipico": np.random.choice([0, 1], size=num_rows, p=[0.95, 0.05]),
            "fecha_siniestro": [
                date(2010, 1, 1) + timedelta(np.random.randint(low=0, high=365 * 20))
                for _ in range(num_rows)
            ],
            "fecha_registro": [
                date(2010, 1, 1) + timedelta(np.random.randint(low=0, high=365 * 20))
                for _ in range(num_rows)
            ],
            "pago_bruto": np.random.random(size=num_rows) * 1e8,
            "pago_retenido": np.random.random(size=num_rows) * 1e8,
            "aviso_bruto": np.random.random(size=num_rows) * 1e8,
            "aviso_retenido": np.random.random(size=num_rows) * 1e8,
            "conteo_pago": np.random.randint(0, 100, size=num_rows),
            "conteo_incurrido": np.random.randint(0, 100, size=num_rows),
            "conteo_desistido": np.random.randint(0, 100, size=num_rows),
        }
    ).with_columns(apertura_reservas=utils.col_apertura_reservas("mock"))


@pytest.fixture
def mock_primas() -> pl.LazyFrame:
    num_rows = 10000
    return pl.LazyFrame(
        {
            "codigo_op": np.random.choice(["01", "02"], size=num_rows),
            "codigo_ramo_op": np.random.choice(["001", "002", "003"], size=num_rows),
            "ramo_desc": np.random.choice(["RAMO1", "RAMO2", "RAMO3"], size=num_rows),
            "apertura_1": np.random.choice(["A", "B", "C"], size=num_rows),
            "apertura_2": np.random.choice(["D", "E", "F"], size=num_rows),
            "fecha_registro": [
                date(2010, 1, 1) + timedelta(np.random.randint(low=0, high=365 * 20))
                for _ in range(num_rows)
            ],
            "prima_bruta": np.random.random(size=num_rows) * 1e8,
            "prima_retenida": np.random.random(size=num_rows) * 1e8,
            "prima_bruta_devengada": np.random.random(size=num_rows) * 1e8,
            "prima_retenida_devengada": np.random.random(size=num_rows) * 1e8,
        }
    ).with_columns(apertura_reservas=utils.col_apertura_reservas("mock"))


@pytest.fixture
def mock_expuestos() -> pl.LazyFrame:
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
                "fecha_registro": [
                    date(2010, 1, 1)
                    + timedelta(np.random.randint(low=0, high=365 * 20))
                    for _ in range(num_rows)
                ],
                "expuestos": np.random.random(size=num_rows) * 1e6,
                "vigentes": np.random.random(size=num_rows) * 1e6,
            }
        )
        .with_columns(
            pl.col("fecha_registro").dt.month_end(),
            apertura_reservas=utils.col_apertura_reservas("mock"),
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
        "mes_inicio": "202301",
        "mes_corte": "202312",
        "tipo_analisis": "triangulos",
        "aproximar_reaseguro": "0",
        "nombre_plantilla": "plantilla",
        "cuadre_contable_sinis": "0",
        "add_fraude_soat": "0",
        "cuadre_contable_primas": "0",
    }


# No se le puede pasar el diccionario desenvuelto (**params_form)
# porque, al ser un modelo SQLModel y no un modelo de Pydantic, no
# hace la conversion de los tipos automaticamente.
@pytest.fixture
def params(params_form: dict[str, str]) -> Parametros:
    return Parametros(
        negocio=params_form["negocio"],
        mes_inicio=int(params_form["mes_inicio"]),
        mes_corte=int(params_form["mes_corte"]),
        tipo_analisis=params_form["tipo_analisis"],
        aproximar_reaseguro=bool(params_form["aproximar_reaseguro"]),
        nombre_plantilla=params_form["nombre_plantilla"],
        cuadre_contable_sinis=bool(params_form["cuadre_contable_sinis"]),
        add_fraude_soat=bool(params_form["add_fraude_soat"]),
        cuadre_contable_primas=bool(params_form["cuadre_contable_primas"]),
        session_id="test-session-id",
    )

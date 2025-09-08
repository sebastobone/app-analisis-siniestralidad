from datetime import date
from typing import Any

import polars as pl
import pytest
from fastapi.testclient import TestClient
from src import constantes as ct
from src.models import Parametros
from src.validation.segmentacion import validar_aptitud_cuadre_contable
from tests.conftest import CONTENT_TYPES, ingresar_parametros


@pytest.fixture()
def params(client: TestClient, rango_meses: tuple[date, date]) -> Parametros:
    return ingresar_parametros(
        client,
        Parametros(
            negocio="test",
            mes_inicio=rango_meses[0],
            mes_corte=rango_meses[1],
            tipo_analisis="triangulos",
            nombre_plantilla="wb_test",
        ),
        {
            "archivo_segmentacion": (
                "segmentacion_test.xlsx",
                open("data/segmentacion_demo.xlsx", "rb"),
                CONTENT_TYPES["xlsx"],
            ),
        },
    )


@pytest.fixture()
def hojas() -> dict[str, pl.DataFrame]:
    return pl.read_excel("data/segmentacion_demo.xlsx", sheet_id=0)


@pytest.mark.fast
@pytest.mark.parametrize(
    "cantidad, hoja_faltante",
    [("siniestros", "Meses_cuadre_siniestros"), ("primas", "Cuadre_Primas")],
)
def test_hojas_faltantes(
    params: Parametros,
    hojas: dict[str, pl.DataFrame],
    cantidad: ct.CANTIDADES_CUADRE,
    hoja_faltante: str,
):
    _ = hojas.pop(hoja_faltante)
    with pytest.raises(ValueError, match=hoja_faltante):
        validar_aptitud_cuadre_contable(hojas, params, cantidad)


@pytest.mark.fast
@pytest.mark.parametrize("cantidad", ["siniestros", "primas"])
def test_columnas_faltantes(
    params: Parametros, hojas: dict[str, pl.DataFrame], cantidad: ct.CANTIDADES_CUADRE
):
    hojas[f"Cuadre_{cantidad.capitalize()}"] = hojas[
        f"Cuadre_{cantidad.capitalize()}"
    ].drop("codigo_ramo_op")
    with pytest.raises(ValueError, match="debe contener las columnas de aperturas"):
        validar_aptitud_cuadre_contable(hojas, params, cantidad)


@pytest.mark.fast
@pytest.mark.parametrize("cantidad", ["siniestros", "primas"])
def test_aperturas_duplicadas(
    params: Parametros, hojas: dict[str, pl.DataFrame], cantidad: ct.CANTIDADES_CUADRE
):
    hojas[f"Cuadre_{cantidad.capitalize()}"] = hojas[
        f"Cuadre_{cantidad.capitalize()}"
    ].vstack(hojas[f"Cuadre_{cantidad.capitalize()}"])
    with pytest.raises(ValueError, match="aperturas duplicadas"):
        validar_aptitud_cuadre_contable(hojas, params, cantidad)


@pytest.mark.fast
@pytest.mark.parametrize("cantidad", ["siniestros", "primas"])
def test_mismatch_aperturas(
    params: Parametros, hojas: dict[str, pl.DataFrame], cantidad: ct.CANTIDADES_CUADRE
):
    hojas[f"Cuadre_{cantidad.capitalize()}"] = hojas[
        f"Cuadre_{cantidad.capitalize()}"
    ].vstack(
        hojas[f"Cuadre_{cantidad.capitalize()}"].with_columns(apertura_1=pl.lit("X"))
    )
    with pytest.raises(ValueError, match="No tiene las mismas aperturas"):
        validar_aptitud_cuadre_contable(hojas, params, cantidad)


@pytest.mark.fast
@pytest.mark.parametrize(
    "cantidad, columna_faltante",
    [("siniestros", "cuadrar_pago_bruto"), ("primas", "cuadrar_prima_retenida")],
)
def test_columnas_faltantes_meses(
    params: Parametros,
    hojas: dict[str, pl.DataFrame],
    cantidad: ct.CANTIDADES_CUADRE,
    columna_faltante: str,
):
    hojas[f"Meses_cuadre_{cantidad}"] = hojas[f"Meses_cuadre_{cantidad}"].drop(
        columna_faltante
    )
    with pytest.raises(ValueError, match="faltan las siguientes columnas"):
        validar_aptitud_cuadre_contable(hojas, params, cantidad)


@pytest.mark.fast
@pytest.mark.parametrize("cantidad", ["siniestros", "primas"])
def test_fechas_duplicadas(
    params: Parametros, hojas: dict[str, pl.DataFrame], cantidad: ct.CANTIDADES_CUADRE
):
    hojas[f"Meses_cuadre_{cantidad}"] = hojas[f"Meses_cuadre_{cantidad}"].vstack(
        hojas[f"Meses_cuadre_{cantidad}"]
    )
    with pytest.raises(ValueError, match="fechas duplicadas"):
        validar_aptitud_cuadre_contable(hojas, params, cantidad)


@pytest.mark.fast
@pytest.mark.parametrize("cantidad", ["siniestros", "primas"])
def test_fechas_inesperadas(
    params: Parametros, hojas: dict[str, pl.DataFrame], cantidad: ct.CANTIDADES_CUADRE
):
    hojas[f"Meses_cuadre_{cantidad}"] = hojas[f"Meses_cuadre_{cantidad}"].with_columns(
        pl.col("fecha_registro").dt.month_end()
    )
    with pytest.raises(ValueError, match="faltan las siguientes fechas"):
        validar_aptitud_cuadre_contable(hojas, params, cantidad)


@pytest.mark.fast
@pytest.mark.parametrize(
    "cantidad, valor, col",
    [
        ("siniestros", 2, "cuadrar_pago_bruto"),
        ("siniestros", -1, "cuadrar_aviso_retenido"),
        ("primas", 2, "cuadrar_prima_bruta"),
        ("primas", -1, "cuadrar_prima_bruta_devengada"),
    ],
)
def test_valores_por_fuera(
    params: Parametros,
    hojas: dict[str, pl.DataFrame],
    cantidad: ct.CANTIDADES_CUADRE,
    valor: int,
    col: str,
):
    hojas[f"Meses_cuadre_{cantidad}"] = hojas[f"Meses_cuadre_{cantidad}"].with_columns(
        pl.lit(valor).alias(col)
    )
    with pytest.raises(ValueError, match="contiene valores"):
        validar_aptitud_cuadre_contable(hojas, params, cantidad)


@pytest.mark.fast
@pytest.mark.parametrize(
    "cantidad, columna, valor",
    [
        ("siniestros", "cuadrar_pago_bruto", 0.5),
        ("siniestros", "fecha_registro", 42),
        ("primas", "cuadrar_prima_bruta", 0.5),
        ("primas", "fecha_registro", 42),
    ],
)
def test_tipos_datos_malos(
    params: Parametros,
    hojas: dict[str, pl.DataFrame],
    cantidad: ct.CANTIDADES_CUADRE,
    columna: str,
    valor: Any,
):
    hojas[f"Meses_cuadre_{cantidad}"] = hojas[f"Meses_cuadre_{cantidad}"].with_columns(
        pl.lit(valor).alias(columna)
    )
    with pytest.raises(ValueError, match=columna):
        validar_aptitud_cuadre_contable(hojas, params, cantidad)

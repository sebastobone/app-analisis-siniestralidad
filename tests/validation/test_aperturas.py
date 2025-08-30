import polars as pl
import pytest
from src.validation import aperturas


@pytest.mark.unit
def test_hojas_faltantes():
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(),
        "Aperturas_Primas": pl.DataFrame(),
    }
    with pytest.raises(ValueError):
        aperturas.validar_aperturas(hojas)


@pytest.mark.unit
def test_columnas_faltantes():
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["001"],
                "periodicidad_ocurrencia": ["Mensual"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        aperturas.validar_aperturas(hojas)
    assert "tipo_indexacion_severidad" in str(exc.value)
    assert "Aperturas_Siniestros" in str(exc.value)


@pytest.mark.unit
def test_aperturas_duplicadas():
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001", "01_001"],
                "codigo_op": ["01", "01"],
                "codigo_ramo_op": ["001", "001"],
                "periodicidad_ocurrencia": ["Mensual", "Mensual"],
                "tipo_indexacion_severidad": ["Ninguna", "Ninguna"],
                "medida_indexacion_severidad": ["Ninguna", "Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        aperturas.validar_aperturas(hojas)
    assert "duplicados" in str(exc.value)


@pytest.mark.unit
def test_periodicidades_invalidas():
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["001"],
                "periodicidad_ocurrencia": ["Otro"],
                "tipo_indexacion_severidad": ["Ninguna"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        aperturas.validar_aperturas(hojas)
    assert "invalidos" in str(exc.value)
    assert "periodicidad_ocurrencia" in str(exc.value)


@pytest.mark.unit
def test_tipos_indexacion_invalidos():
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["001"],
                "periodicidad_ocurrencia": ["Mensual"],
                "tipo_indexacion_severidad": ["Por fecha de atencion"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        aperturas.validar_aperturas(hojas)
    assert "invalidos" in str(exc.value)
    assert "tipo_indexacion_severidad" in str(exc.value)


@pytest.mark.unit
def test_medidas_indexacion_invalidas():
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["001"],
                "periodicidad_ocurrencia": ["Mensual"],
                "tipo_indexacion_severidad": ["Por fecha de ocurrencia"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        aperturas.validar_aperturas(hojas)
    assert "medida_indexacion_severidad" in str(exc.value)
    assert "no puede ser" in str(exc.value)


@pytest.mark.unit
def test_variables_apertura_sobrantes():
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001"],
                "codigo_op": ["01"],
                "codigo_ramo_op": ["001"],
                "periodicidad_ocurrencia": ["Mensual"],
                "tipo_indexacion_severidad": ["Ninguna"],
                "medida_indexacion_severidad": ["Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"], "apertura_1": ["A"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        aperturas.validar_aperturas(hojas)
    assert "Sobrantes:" in str(exc.value)
    assert "apertura_1" in str(exc.value)
    assert "Aperturas_Primas" in str(exc.value)


@pytest.mark.unit
def test_cruces_nulos_aperturas():
    hojas = {
        "Aperturas_Siniestros": pl.DataFrame(
            {
                "apertura_reservas": ["01_001_A", "01_002_A"],
                "codigo_op": ["01", "01"],
                "codigo_ramo_op": ["001", "002"],
                "apertura_1": ["A", "A"],
                "periodicidad_ocurrencia": ["Mensual", "Mensual"],
                "tipo_indexacion_severidad": ["Ninguna", "Ninguna"],
                "medida_indexacion_severidad": ["Ninguna", "Ninguna"],
            }
        ),
        "Aperturas_Primas": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
        "Aperturas_Expuestos": pl.DataFrame(
            {"codigo_op": ["01"], "codigo_ramo_op": ["001"]}
        ),
    }
    with pytest.raises(ValueError) as exc:
        aperturas.validar_aperturas(hojas)
    assert "no cruzan" in str(exc.value)
    assert "Aperturas_Primas" in str(exc.value)


@pytest.mark.unit
def test_archivo_correcto():
    hojas = pl.read_excel("data/segmentacion_demo.xlsx", sheet_id=0)
    aperturas.validar_aperturas(hojas)

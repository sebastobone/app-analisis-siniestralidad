import src.constantes as ct
import xlwings as xw
from src.models import Offset, RangeDimension


def obtener_rangos_parametros(
    hoja: xw.Sheet,
    dimensiones_triangulo: RangeDimension,
    tipo_indexacion: ct.TIPOS_INDEXACION,
) -> dict[str, xw.Range]:
    num_ocurrencias = dimensiones_triangulo.height
    num_alturas = dimensiones_triangulo.width

    rangos = obtener_rangos_parametros_comunes(hoja, num_ocurrencias, num_alturas)

    if hoja.name in ("Frecuencia", "Severidad", "Plata"):
        agregar_rangos_parametros_comunes_triangulos(
            hoja, num_ocurrencias, num_alturas, rangos
        )

        if hoja.name == "Severidad" and tipo_indexacion != "Ninguna":
            agregar_rangos_unidad_indexacion(hoja, num_ocurrencias, num_alturas, rangos)

    return rangos


def obtener_rangos_parametros_comunes(
    hoja: xw.Sheet, num_ocurrencias: int, num_alturas: int
) -> dict[str, xw.Range]:
    return {
        "EXCLUSIONES": obtener_rango(
            hoja,
            "Exclusiones",
            "F1:F10000",
            Offset(y=ct.HEADER_TRIANGULOS, x=ct.COL_OCURRS_PLANTILLAS + 1),
            RangeDimension(height=num_ocurrencias, width=num_alturas * 2),
        ),
        "VENTANAS": obtener_rango(
            hoja,
            "Periodo inicial",
            "B1:B10000",
            Offset(y=1, x=2),
            RangeDimension(height=16, width=3),
        ),
        "TIPO_FACTORES_SELECCIONADOS": obtener_rango(
            hoja,
            "Periodo inicial",
            "B1:B10000",
            Offset(y=0, x=ct.COL_OCURRS_PLANTILLAS),
            RangeDimension(height=1, width=2),
        ),
        "FACTORES_SELECCIONADOS": obtener_rango(
            hoja,
            "FACTORES SELECCIONADOS",
            "F1:F10000",
            Offset(y=0, x=ct.COL_OCURRS_PLANTILLAS + 1),
            RangeDimension(height=1, width=num_alturas * 2),
        ),
    }


def agregar_rangos_parametros_comunes_triangulos(
    hoja: xw.Sheet, num_ocurrencias: int, num_alturas: int, rangos: dict[str, xw.Range]
):
    rangos.update(
        {
            "MET_PAGO_INCURRIDO": obtener_rango(
                hoja,
                "metodologia",
                "A1:A10000",
                Offset(y=0, x=2),
                RangeDimension(height=1, width=2),
            ),
            "ULTIMATE": obtener_rango(
                hoja,
                "ultimate",
                "D1:D10000",
                Offset(y=1, x=4),
                RangeDimension(height=num_ocurrencias, width=1),
            ),
            "METODOLOGIA": obtener_rango(
                hoja,
                "metodologia",
                "E1:E10000",
                Offset(y=1, x=5),
                RangeDimension(height=num_ocurrencias, width=1),
            ),
            "BASE": obtener_rango(
                hoja,
                "Base",
                "F1:F10000",
                Offset(y=ct.HEADER_TRIANGULOS, x=ct.COL_OCURRS_PLANTILLAS + 1),
                RangeDimension(height=num_ocurrencias, width=num_alturas),
            ),
            "INDICADOR": obtener_rango(
                hoja,
                "indicador",
                "F1:F10000",
                Offset(y=1, x=6),
                RangeDimension(height=num_ocurrencias, width=1),
            ),
            "COMENTARIOS": obtener_rango(
                hoja,
                "comentarios",
                "G1:G10000",
                Offset(y=1, x=7),
                RangeDimension(height=num_ocurrencias, width=1),
            ),
        }
    )


def agregar_rangos_unidad_indexacion(
    hoja: xw.Sheet, num_ocurrencias: int, num_alturas: int, rangos: dict[str, xw.Range]
):
    rangos.update(
        {
            "UNIDAD_INDEXACION": obtener_rango(
                hoja,
                "Unidad Indexacion",
                "F1:F10000",
                Offset(y=ct.HEADER_TRIANGULOS, x=ct.COL_OCURRS_PLANTILLAS + 1),
                RangeDimension(height=num_ocurrencias, width=num_alturas),
            )
        }
    )


def obtener_rango(
    sheet: xw.Sheet,
    palabra_buscada: str,
    fila_o_columna: str,
    offset: Offset,
    dimension_rango: RangeDimension,
) -> xw.Range:
    return sheet.range(
        sheet.cells(
            obtener_indice_en_rango(palabra_buscada, sheet.range(fila_o_columna))
            + offset.y,
            offset.x,
        ),
        sheet.cells(
            obtener_indice_en_rango(palabra_buscada, sheet.range(fila_o_columna))
            + offset.y
            + dimension_rango.height
            - 1,
            offset.x + dimension_rango.width - 1,
        ),
    )


def obtener_indice_en_rango(palabra_buscada: str, rango: xw.Range) -> int:
    return rango.value.index(palabra_buscada) + 1

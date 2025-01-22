from typing import Literal

import xlwings as xw

import src.constantes as ct
from src.models import Offset, RangeDimension


def obtener_rangos_parametros(
    hoja: xw.Sheet,
    dimensiones_triangulo: RangeDimension,
    mes_del_periodo: int,
    metodo_indexacion: Literal[
        "Ninguna", "Por fecha de ocurrencia", "Por fecha de pago"
    ],
    metodo_ult_ocurrencia: Literal["% Siniestralidad", "Frecuencia y Severidad"],
) -> dict[str, xw.Range]:
    num_ocurrencias = dimensiones_triangulo.height
    num_alturas = dimensiones_triangulo.width

    rangos = obtener_rangos_parametros_comunes(
        hoja, num_ocurrencias, num_alturas, mes_del_periodo
    )

    if hoja.name in ("Plantilla_Frec", "Plantilla_Seve", "Plantilla_Plata"):
        agregar_rangos_parametros_comunes_triangulos(
            hoja, num_ocurrencias, num_alturas, rangos
        )

        if hoja.name == "Plantilla_Seve":
            agregar_rangos_parametros_plantilla_severidad(
                hoja, metodo_indexacion, num_ocurrencias, num_alturas, rangos
            )

    elif hoja.name == "Plantilla_Entremes":
        agregar_rangos_parametros_comunes_entremes(
            hoja, mes_del_periodo, num_ocurrencias, rangos
        )

        if metodo_ult_ocurrencia == "% Siniestralidad":
            agregar_rangos_parametros_entremes_pct_siniestralidad(
                hoja, mes_del_periodo, num_ocurrencias, rangos
            )

        if metodo_ult_ocurrencia == "Frecuencia y Severidad":
            agregar_rangos_parametros_entremes_frec_seve(
                hoja, mes_del_periodo, num_ocurrencias, rangos
            )

    return rangos


def obtener_rangos_parametros_comunes(
    hoja: xw.Sheet, num_ocurrencias: int, num_alturas: int, mes_del_periodo: int
):
    ini = ct.FILA_INI_PARAMS
    rango_ocurrencias_totales = RangeDimension(
        height=num_ocurrencias + mes_del_periodo - 1, width=1
    )
    return {
        "MET_PAGO_INCURRIDO": hoja.range((ini, 3), (ini, 4)),
        "EXCLUSIONES": obtener_rango(
            hoja,
            "Exclusiones",
            "F1:F1000",
            Offset(y=ct.HEADER_TRIANGULOS, x=ct.COL_OCURRS_PLANTILLAS + 1),
            RangeDimension(height=num_ocurrencias, width=num_alturas * 2),
        ),
        "VENTANAS": obtener_rango(
            hoja,
            "Periodo inicial",
            "B1:B1000",
            Offset(y=1, x=2),
            RangeDimension(height=16, width=3),
        ),
        "FACTORES_SELECCIONADOS": obtener_rango(
            hoja,
            "FACTORES SELECCIONADOS",
            "F1:F1000",
            Offset(y=0, x=ct.COL_OCURRS_PLANTILLAS + 1),
            RangeDimension(height=1, width=num_alturas),
        ),
        "ULTIMATE": obtener_rango(
            hoja,
            "Ultimate",
            "D1:D1000",
            Offset(y=1, x=4),
            rango_ocurrencias_totales,
        ),
        "METODOLOGIA": obtener_rango(
            hoja,
            "Metodologia",
            "E1:E1000",
            Offset(y=1, x=5),
            rango_ocurrencias_totales,
        ),
    }


def agregar_rangos_parametros_comunes_triangulos(
    hoja: xw.Sheet, num_ocurrencias: int, num_alturas: int, rangos: dict[str, xw.Range]
):
    rangos.update(
        {
            "BASE": obtener_rango(
                hoja,
                "Base",
                "F1:F1000",
                Offset(y=ct.HEADER_TRIANGULOS, x=ct.COL_OCURRS_PLANTILLAS + 1),
                RangeDimension(height=num_ocurrencias, width=num_alturas),
            ),
            "INDICADOR": obtener_rango(
                hoja,
                "Indicador",
                "F1:F1000",
                Offset(y=1, x=6),
                RangeDimension(height=num_ocurrencias, width=1),
            ),
            "COMENTARIOS": obtener_rango(
                hoja,
                "Comentarios",
                "G1:G1000",
                Offset(y=1, x=7),
                RangeDimension(height=num_ocurrencias, width=1),
            ),
        }
    )


def agregar_rangos_parametros_plantilla_severidad(
    hoja: xw.Sheet,
    metodo_indexacion: Literal[
        "Ninguna", "Por fecha de ocurrencia", "Por fecha de pago"
    ],
    num_ocurrencias: int,
    num_alturas: int,
    rangos: dict[str, xw.Range],
):
    ini = ct.FILA_INI_PARAMS
    rangos.update(
        {
            "TIPO_INDEXACION": hoja.range((ini + 1, 3), (ini + 1, 4)),
            "MEDIDA_INDEXACION": hoja.range((ini + 2, 3), (ini + 2, 4)),
        }
    )

    if metodo_indexacion in ("Por fecha de ocurrencia", "Por fecha de pago"):
        rangos.update(
            {
                "UNIDAD_INDEXACION": obtener_rango(
                    hoja,
                    "Unidad_Indexacion",
                    "F1:F1000",
                    Offset(y=ct.HEADER_TRIANGULOS, x=ct.COL_OCURRS_PLANTILLAS + 1),
                    RangeDimension(height=num_ocurrencias, width=num_alturas),
                )
            }
        )


def agregar_rangos_parametros_comunes_entremes(
    hoja: xw.Sheet,
    mes_del_periodo: int,
    num_ocurrencias: int,
    rangos: dict[str, xw.Range],
):
    ini = ct.FILA_INI_PARAMS
    rango_ocurrencias_triangulo = RangeDimension(height=num_ocurrencias - 1, width=1)
    rango_ocurrencias_anteriores = RangeDimension(
        height=num_ocurrencias + mes_del_periodo - 2, width=1
    )
    rangos.update(
        {
            "FREC_SEV_ULTIMA_OCURRENCIA": hoja.range((ini + 1, 3), (ini + 1, 4)),
            "VARIABLE_DESPEJADA": hoja.range((ini + 2, 3), (ini + 2, 4)),
            "COMENTARIOS": obtener_rango(
                hoja,
                "Comentarios",
                "J1:J1000",
                Offset(y=1, x=10),
                rango_ocurrencias_triangulo,
            ),
            "FACTOR_COMPLETITUD": obtener_rango(
                hoja,
                "Factor completitud",
                "T1:T1000",
                Offset(y=1, x=20),
                rango_ocurrencias_triangulo,
            ),
            "PCT_SUE_BF": obtener_rango(
                hoja,
                "% SUE BF",
                "AA1:AA1000",
                Offset(y=1, x=27),
                rango_ocurrencias_triangulo,
            ),
            "VELOCIDAD_BF": obtener_rango(
                hoja,
                "Velocidad BF",
                "AB1:AB1000",
                Offset(y=1, x=28),
                rango_ocurrencias_triangulo,
            ),
            "PCT_SUE_NUEVO": obtener_rango(
                hoja,
                "% SUE Nuevo",
                "AG1:AG1000",
                Offset(y=1, x=33),
                rango_ocurrencias_triangulo,
            ),
            "AJUSTE_PARCIAL": obtener_rango(
                hoja,
                "% Ajuste",
                "AK1:AK1000",
                Offset(y=1, x=37),
                rango_ocurrencias_anteriores,
            ),
            "COMENTARIOS_AJUSTE": obtener_rango(
                hoja,
                "Comentarios Aj.",
                "AN1:AN1000",
                Offset(y=1, x=40),
                rango_ocurrencias_anteriores,
            ),
        }
    )


def agregar_rangos_parametros_entremes_pct_siniestralidad(
    hoja: xw.Sheet,
    mes_del_periodo: int,
    num_ocurrencias: int,
    rangos: dict[str, xw.Range],
):
    rango_ocurrencias_entremes = RangeDimension(height=mes_del_periodo, width=1)
    rango_ocurrencias_totales = crear_rango_ocurrencias_totales(
        num_ocurrencias, mes_del_periodo
    )
    rangos.update(
        {
            "ULTIMATE_NUEVO_PERIODO": obtener_rango(
                hoja,
                "Ultimate",
                "D1:D1000",
                Offset(y=num_ocurrencias, x=4),
                rango_ocurrencias_entremes,
            ),
            "PCT_ULTIMATE_NUEVO_PERIODO": obtener_rango(
                hoja,
                "% SUE",
                "G1:G1000",
                Offset(y=num_ocurrencias, x=6),
                rango_ocurrencias_entremes,
            ),
            "ULTIMATE_FRECUENCIA_SEVERIDAD": obtener_rango(
                hoja,
                "Ultimate",
                "D1:D1000",
                Offset(
                    y=num_ocurrencias + mes_del_periodo + ct.SEP_TRIANGULOS + 1,
                    x=4,
                ),
                rango_ocurrencias_totales,
            ),
            "COMENTARIOS_FRECUENCIA_SEVERIDAD": obtener_rango(
                hoja,
                "Ultimate",
                "D1:D1000",
                Offset(
                    y=num_ocurrencias + mes_del_periodo + ct.SEP_TRIANGULOS + 1,
                    x=5,
                ),
                rango_ocurrencias_totales,
            ),
        }
    )


def agregar_rangos_parametros_entremes_frec_seve(
    hoja: xw.Sheet,
    mes_del_periodo: int,
    num_ocurrencias: int,
    rangos: dict[str, xw.Range],
):
    rango_ocurrencias_totales = crear_rango_ocurrencias_totales(
        num_ocurrencias, mes_del_periodo
    )
    rangos.update(
        {
            "ULTIMATE_FRECUENCIA": obtener_rango(
                hoja,
                "Ultimate",
                "D1:D1000",
                Offset(
                    y=num_ocurrencias + mes_del_periodo + ct.SEP_TRIANGULOS + 1,
                    x=4,
                ),
                rango_ocurrencias_totales,
            ),
            "COMENTARIOS_FRECUENCIA": obtener_rango(
                hoja,
                "Ultimate",
                "D1:D1000",
                Offset(
                    y=num_ocurrencias + mes_del_periodo + ct.SEP_TRIANGULOS + 1,
                    x=5,
                ),
                rango_ocurrencias_totales,
            ),
            "ULTIMATE_SEVERIDAD": obtener_rango(
                hoja,
                "Ultimate",
                "D1:D1000",
                Offset(
                    y=num_ocurrencias + mes_del_periodo + ct.SEP_TRIANGULOS + 1,
                    x=10,
                ),
                rango_ocurrencias_totales,
            ),
            "COMENTARIOS_SEVERIDAD": obtener_rango(
                hoja,
                "Ultimate",
                "D1:D1000",
                Offset(
                    y=num_ocurrencias + mes_del_periodo + ct.SEP_TRIANGULOS + 1,
                    x=11,
                ),
                rango_ocurrencias_totales,
            ),
        }
    )


def crear_rango_ocurrencias_totales(
    num_ocurrencias: int, mes_del_periodo: int
) -> RangeDimension:
    return RangeDimension(height=num_ocurrencias + mes_del_periodo - 1, width=1)


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

import polars as pl

from src import constantes as ct
from src import utils
from src.logger_config import logger
from src.models import Parametros
from src.validation import validaciones as valid


def _validar_medidas_indexacion(df: pl.DataFrame) -> None:
    medidas_indexacion = (
        df.select(["tipo_indexacion_severidad", "medida_indexacion_severidad"])
        .unique()
        .with_columns(
            pl.when(
                (pl.col("tipo_indexacion_severidad") != "Ninguna")
                & (pl.col("medida_indexacion_severidad") == "Ninguna")
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("es_invalida")
        )
    )
    if medidas_indexacion.filter(pl.col("es_invalida")).height > 0:
        raise ValueError(
            utils.limpiar_espacios_log(
                """
                La columna "medida_indexacion_severidad" en la hoja 
                "Aperturas_Siniestros" contiene valores invalidos. Si 
                "tipo_indexacion_severidad" es diferente de "Ninguna",
                entonces "medida_indexacion_severidad" no puede ser "Ninguna".
                """
            )
        )


def _validar_cruces_aperturas(
    nombre_hoja_referencia: str,
    nombre_hoja_propuesta: str,
    aperturas_referencia: pl.DataFrame,
    aperturas_propuestas: pl.DataFrame,
    nulos_derecha_admisibles: bool,
) -> None:
    cruce = aperturas_referencia.join(
        aperturas_propuestas,
        on=aperturas_propuestas.collect_schema().names(),
        how="full",
    )

    if nulos_derecha_admisibles:
        cruce = cruce.select(aperturas_referencia.collect_schema().names())

    valid.validar_no_nulos(
        cruce,
        """
        No tiene las mismas aperturas en las hojas {nombre_hoja_referencia}
        y {nombre_hoja_propuesta}. Revise los valores que no cruzan: {nulos}
        """,
        {
            "nombre_hoja_referencia": nombre_hoja_referencia,
            "nombre_hoja_propuesta": nombre_hoja_propuesta,
        },
    )


def validar_archivo_segmentacion(hojas: dict[str, pl.DataFrame]) -> None:
    valid.validar_subconjunto(
        ["Aperturas_Siniestros", "Aperturas_Primas", "Aperturas_Expuestos"],
        list(hojas.keys()),
        "Faltan las siguientes hojas en el archivo de segmentacion: {faltantes}",
        None,
        "error",
    )

    mensaje_columnas_faltantes = """
        Faltan las siguientes columnas en la hoja "{nombre_hoja}"
        del archivo de segmentacion: {faltantes}
    """

    mensaje_valores_no_permitidos = """
        La columna {columna} en la hoja {nombre_hoja} contiene valores invalidos.
        Se encontro {faltantes}, pero los valores permitidos son {permitidos}.
    """

    mensaje_aperturas_duplicadas = """
        La hoja {nombre_hoja} contiene aperturas duplicadas.
    """

    # Validar siniestros
    columnas_necesarias_siniestros = [
        "codigo_op",
        "codigo_ramo_op",
        "periodicidad_ocurrencia",
        "tipo_indexacion_severidad",
        "medida_indexacion_severidad",
    ]
    columnas_encontradas_siniestros = (
        hojas["Aperturas_Siniestros"].collect_schema().names()
    )

    valid.validar_subconjunto(
        columnas_necesarias_siniestros,
        columnas_encontradas_siniestros,
        mensaje_columnas_faltantes,
        {"nombre_hoja": "Aperturas_Siniestros"},
        "error",
    )
    valid.validar_unicidad(
        hojas["Aperturas_Siniestros"].drop(
            [
                "periodicidad_ocurrencia",
                "tipo_indexacion_severidad",
                "medida_indexacion_severidad",
            ]
        ),
        mensaje_aperturas_duplicadas,
        {"nombre_hoja": "Aperturas_Siniestros"},
        "error",
    )
    valid.validar_subconjunto(
        hojas["Aperturas_Siniestros"]
        .get_column("periodicidad_ocurrencia")
        .unique()
        .to_list(),
        list(ct.PERIODICIDADES.keys()),
        mensaje_valores_no_permitidos,
        {
            "columna": "periodicidad_ocurrencia",
            "nombre_hoja": "Aperturas_Siniestros",
            "permitidos": list(ct.PERIODICIDADES.keys()),
        },
        "error",
    )
    valid.validar_subconjunto(
        hojas["Aperturas_Siniestros"]
        .get_column("tipo_indexacion_severidad")
        .unique()
        .to_list(),
        ["Ninguna", "Por fecha de ocurrencia", "Por fecha de movimiento"],
        mensaje_valores_no_permitidos,
        {
            "columna": "tipo_indexacion_severidad",
            "nombre_hoja": "Aperturas_Siniestros",
            "permitidos": [
                "Ninguna",
                "Por fecha de ocurrencia",
                "Por fecha de movimiento",
            ],
        },
        "error",
    )
    _validar_medidas_indexacion(hojas["Aperturas_Siniestros"])

    mensaje_variables_sobrantes = """
        Los variables de apertura de la hoja "{nombre_hoja}"
        deben estar contenidas en las variables de apertura de la hoja
        "Aperturas_Siniestros". Sobrantes: {faltantes}
    """

    # Validar primas y expuestos
    aperturas_siniestros = hojas["Aperturas_Siniestros"].drop(
        [
            "periodicidad_ocurrencia",
            "tipo_indexacion_severidad",
            "medida_indexacion_severidad",
        ]
    )
    for nombre_hoja in ["Aperturas_Primas", "Aperturas_Expuestos"]:
        columnas_encontradas = hojas[nombre_hoja].collect_schema().names()

        valid.validar_subconjunto(
            ["codigo_op", "codigo_ramo_op"],
            columnas_encontradas,
            mensaje_columnas_faltantes,
            {"nombre_hoja": nombre_hoja},
            "error",
        )
        valid.validar_subconjunto(
            columnas_encontradas,
            columnas_encontradas_siniestros,
            mensaje_variables_sobrantes,
            {"nombre_hoja": nombre_hoja},
            "error",
        )
        valid.validar_unicidad(
            hojas[nombre_hoja],
            mensaje_aperturas_duplicadas,
            {"nombre_hoja": nombre_hoja},
            "error",
        )
        _validar_cruces_aperturas(
            "Aperturas_Siniestros",
            nombre_hoja,
            aperturas_siniestros,
            hojas[nombre_hoja],
            False,
        )

    logger.info(
        "Validacion de aperturas en el archivo de segmentacion completada exitosamente."
    )


def validar_aptitud_cuadre_contable(
    hojas: dict[str, pl.DataFrame], p: Parametros, cantidad: ct.CANTIDADES_CUADRE
) -> None:
    valid.validar_subconjunto(
        [f"Cuadre_{cantidad.capitalize()}", f"Meses_cuadre_{cantidad}"],
        list(hojas.keys()),
        """
        En el archivo segmentacion faltan las hojas {faltantes} para realizar el
        cuadre contable de {cantidad}. Agregue estas hojas y vuelva a cargar el
        archivo.
        """,
        {"cantidad": cantidad},
        "error",
    )

    # Validacion de aperturas donde se va a asignar el cuadre
    columnas_aperturas = utils.obtener_nombres_aperturas(p.negocio, cantidad)

    valid.validar_subconjunto(
        columnas_aperturas,
        hojas[f"Cuadre_{cantidad.capitalize()}"].collect_schema().names(),
        """
        La hoja {nombre_hoja} debe contener las columnas de aperturas de
        {cantidad}: {aperturas}. Faltan las siguientes: {faltantes}
        """,
        {
            "nombre_hoja": f"Cuadre_{cantidad.capitalize()}",
            "cantidad": cantidad,
            "aperturas": columnas_aperturas,
        },
        "error",
    )
    valid.validar_unicidad(
        hojas[f"Cuadre_{cantidad.capitalize()}"].select(columnas_aperturas),
        "La hoja {nombre_hoja} contiene aperturas duplicadas.",
        {"nombre_hoja": f"Cuadre_{cantidad.capitalize()}"},
        "error",
    )

    aperturas = utils.obtener_aperturas(p.negocio, cantidad)
    _validar_cruces_aperturas(
        f"Aperturas_{cantidad.capitalize()}",
        f"Cuadre_{cantidad.capitalize()}",
        aperturas,
        hojas[f"Cuadre_{cantidad.capitalize()}"],
        True,
    )

    # Validacion de periodos a cuadrar
    hoja_meses = hojas[f"Meses_cuadre_{cantidad}"]

    if cantidad == "siniestros":
        columnas_cantidades = ct.COLUMNAS_SINIESTROS_CUADRE
    else:
        columnas_cantidades = list(ct.Valores().model_dump()[cantidad].keys())

    valid.validar_subconjunto(
        ["fecha_registro"] + columnas_cantidades,
        hoja_meses.collect_schema().names(),
        """
        En la hoja {nombre_hoja} faltan las siguientes columnas: {faltantes}.
        Agreguelas y vuelva a cargar el archivo.
        """,
        {"nombre_hoja": f"Meses_cuadre_{cantidad}"},
        "error",
    )

    _validar_tipos_datos_cuadre(hoja_meses, cantidad, columnas_cantidades)

    valid.validar_unicidad(
        hoja_meses.select(["fecha_registro"]),
        """
        En la hoja {nombre_hoja} existen fechas duplicadas.
        Eliminelas y vuelva a cargar el archivo.
        """,
        {"nombre_hoja": f"Meses_cuadre_{cantidad}"},
        "error",
    )

    fechas_esperadas = pl.date_range(
        p.mes_inicio, p.mes_corte, interval="1mo", eager=True
    ).dt.month_start()

    fechas_encontradas = (
        hojas[f"Meses_cuadre_{cantidad}"].get_column("fecha_registro").cast(pl.Date)
    )

    valid.validar_subconjunto(
        fechas_esperadas.to_list(),
        fechas_encontradas.to_list(),
        """
        En la hoja {nombre_hoja} faltan las siguientes fechas: {faltantes}.
        Agreguelas y vuelva a cargar el archivo.
        """,
        {"nombre_hoja": f"Meses_cuadre_{cantidad}"},
        "error",
    )

    valor_maximo = hoja_meses.drop("fecha_registro").max().max_horizontal().item()
    if valor_maximo > 1:
        raise ValueError(
            f"""
            La hoja "Meses_cuadre_{cantidad}" contiene valores mayores a 1.
            Reviselas y vuelva a cargar el archivo.
            """
        )

    valor_minimo = hoja_meses.drop("fecha_registro").min().min_horizontal().item()
    if valor_minimo < 0:
        raise ValueError(
            f"""
            La hoja "Meses_cuadre_{cantidad}" contiene valores menores a 0.
            Reviselas y vuelva a cargar el archivo.
            """
        )

    logger.info(
        utils.limpiar_espacios_log(
            """
            Validacion de parametros para cuadre contable en el archivo
            de segmentacion completada exitosamente.
            """
        )
    )


def _validar_tipos_datos_cuadre(
    hoja_meses: pl.DataFrame,
    cantidad: ct.CANTIDADES_CUADRE,
    columnas_cantidades: list[str],
) -> None:
    if not hoja_meses.get_column("fecha_registro").dtype.is_temporal():
        raise ValueError(
            utils.limpiar_espacios_log(
                f"""
                La columna "fecha_registro" de la hoja "Meses_cuadre_{cantidad}"
                debe estar en formato de fecha.
                """
            )
        )
    for col in columnas_cantidades:
        if not hoja_meses.get_column(col).dtype.is_integer():
            raise ValueError(
                utils.limpiar_espacios_log(
                    f"""
                    Las columna "{col}" en la hoja "Meses_cuadre_{cantidad}"
                    deben tener valores enteros de 0 o 1.
                    """
                )
            )

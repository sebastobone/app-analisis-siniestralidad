import csv
import io

import polars as pl
import xlsxwriter
from fastapi import UploadFile

from src import constantes as ct
from src import utils
from src.logger_config import logger
from src.models import ArchivosInput
from src.validation import cantidades, segmentacion

ERROR_TIPOS_DATOS = """
    Ocurrio un error al transformar los tipos de datos del archivo
    {nombre_archivo} en la carga de {cantidad}:
"""


def procesar_archivos(
    archivos: ArchivosInput,
    negocio: str,
) -> None:
    if archivos.segmentacion:
        procesar_archivo_segmentacion(archivos.segmentacion, negocio)

    for cantidad, archivos_cantidad in {
        "siniestros": archivos.siniestros,
        "primas": archivos.primas,
        "expuestos": archivos.expuestos,
    }.items():
        if archivos_cantidad:
            validar_unicidad_nombres(archivos_cantidad, cantidad)
            for archivo in archivos_cantidad:
                procesar_archivo_cantidad(archivo, negocio, cantidad)


def procesar_archivo_segmentacion(
    archivo_segmentacion: UploadFile, negocio: str
) -> None:
    contenido = archivo_segmentacion.file.read()
    hojas = pl.read_excel(io.BytesIO(contenido), sheet_id=0)

    segmentacion.validar_archivo_segmentacion(hojas)

    with xlsxwriter.Workbook(f"data/segmentacion_{negocio}.xlsx") as writer:
        for hoja in hojas.keys():
            hojas[hoja].write_excel(writer, worksheet=hoja)

    logger.success("Archivo de segmentacion procesado y guardado exitosamente.")


def procesar_archivo_cantidad(
    archivo_cantidad: UploadFile, negocio: str, cantidad: ct.LISTA_CANTIDADES
) -> None:
    filename = str(archivo_cantidad.filename)
    extension = filename.split(".")[-1]

    df = leer_archivo_cantidad(
        filename, archivo_cantidad.file.read(), extension, cantidad
    )
    cantidades.validar_archivo(negocio, df, filename, cantidad)
    df.pipe(
        utils.asignar_tipos_columnas,
        cantidad,
        ERROR_TIPOS_DATOS,
        {"cantidad": cantidad, "nombre_archivo": filename},
    ).pipe(utils.agrupar_por_columnas_relevantes, negocio, cantidad).write_parquet(
        f"data/carga_manual/{cantidad}/{filename.replace(extension, 'parquet')}"
    )

    logger.success(f"Archivo {filename} procesado y guardado exitosamente.")


def leer_archivo_cantidad(
    filename: str, contenido: bytes, extension: str, cantidad: ct.LISTA_CANTIDADES
) -> pl.DataFrame:
    contenido_bytes = io.BytesIO(contenido)
    tipo_datos_descriptores = ct.Descriptores().model_dump()[cantidad]
    tipo_datos_valores = ct.Valores().model_dump()[cantidad]
    tipo_datos = {**tipo_datos_descriptores, **tipo_datos_valores}

    if extension in ["csv", "txt"]:
        separador = detectar_separador(contenido)
        try:
            df = pl.read_csv(
                contenido_bytes, separator=separador, schema_overrides=tipo_datos
            )
        except pl.exceptions.ComputeError as e:
            raise lanzar_error_tipo_datos(e, filename, cantidad) from e
    elif extension == "xlsx":
        validar_unicidad_excel(filename, contenido_bytes)
        try:
            df = pl.read_excel(contenido_bytes, schema_overrides=tipo_datos)
        except pl.exceptions.ComputeError as e:
            raise lanzar_error_tipo_datos(e, filename, cantidad) from e
    elif extension == "parquet":
        df = pl.read_parquet(contenido_bytes)

    return df


def lanzar_error_tipo_datos(
    e: Exception, filename: str, cantidad: ct.LISTA_CANTIDADES
) -> ValueError:
    raise ValueError(
        utils.limpiar_espacios_log(
            f"""
            Ocurrio un error al leer el archivo {filename} de la
            carga de {cantidad}: {str(e)}
            """
        )
    )


def validar_unicidad_nombres(
    archivos: list[UploadFile], cantidad: ct.LISTA_CANTIDADES
) -> None:
    nombres_archivos = []
    duplicados = []
    for archivo in archivos:
        filename = str(archivo.filename)
        extension = filename.split(".")[-1]
        nombre = filename.replace(f".{extension}", "")
        nombres_archivos.append(nombre)

        if nombres_archivos.count(nombre) > 1:
            duplicados.append(filename)

    if len(nombres_archivos) != len(set(nombres_archivos)):
        raise ValueError(
            utils.limpiar_espacios_log(
                f"""
                Los nombres de los archivos deben ser diferentes. En la carga
                de {cantidad} se encontraron nombres duplicados: {duplicados}
                """
            )
        )


def detectar_separador(contenido_archivo: bytes) -> str:
    texto = contenido_archivo.decode(errors="replace")
    muestra = texto[:2048]

    sniffer = csv.Sniffer()
    dialect = sniffer.sniff(muestra)
    return dialect.delimiter


def validar_unicidad_excel(filename: str, contenido_bytes: io.BytesIO):
    hojas = pl.read_excel(contenido_bytes, sheet_id=0)
    numero_hojas = len(hojas.keys())
    if numero_hojas > 1:
        raise ValueError(f"El archivo {filename} tiene mas de una hoja.")


def eliminar_archivos() -> None:
    utils.vaciar_directorio("data/carga_manual/siniestros")
    utils.vaciar_directorio("data/carga_manual/primas")
    utils.vaciar_directorio("data/carga_manual/expuestos")

    logger.info("Archivos de siniestros, primas, y expuestos cargados eliminados.")

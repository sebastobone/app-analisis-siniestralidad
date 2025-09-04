import csv
import io
import zipfile
from datetime import date

import polars as pl
import xlsxwriter
from fastapi import UploadFile

from src import constantes as ct
from src import utils
from src.informacion.mocks import generar_mock
from src.logger_config import logger
from src.models import ArchivosCantidades, Parametros
from src.validation import cantidades, segmentacion


def procesar_archivos_cantidades(archivos: ArchivosCantidades, p: Parametros) -> None:
    for cantidad, archivos_cantidad in {
        "siniestros": archivos.siniestros,
        "primas": archivos.primas,
        "expuestos": archivos.expuestos,
    }.items():
        if archivos_cantidad:
            validar_unicidad_nombres(archivos_cantidad, cantidad)
            for archivo in archivos_cantidad:
                procesar_archivo_cantidad(archivo, p, cantidad)


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
    archivo_cantidad: UploadFile, p: Parametros, cantidad: ct.CANTIDADES
) -> None:
    filename = str(archivo_cantidad.filename)
    extension = filename.split(".")[-1]

    df = leer_archivo_cantidad(
        filename, archivo_cantidad.file.read(), extension, cantidad
    )
    cantidades.validar_archivo(p.negocio, df, filename, cantidad)
    df = cantidades.organizar_archivo(
        df, p.negocio, (p.mes_inicio, p.mes_corte), cantidad, filename
    )

    print(df)

    df.write_parquet(
        f"data/carga_manual/{cantidad}/{filename.replace(extension, 'parquet')}"
    )

    logger.success(f"Archivo {filename} procesado y guardado exitosamente.")


def leer_archivo_cantidad(
    filename: str, contenido: bytes, extension: str, cantidad: ct.CANTIDADES
) -> pl.DataFrame:
    contenido_bytes = io.BytesIO(contenido)
    tipo_datos_descriptores = ct.DESCRIPTORES[cantidad]
    tipo_datos_valores = ct.VALORES[cantidad]
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
    e: Exception, filename: str, cantidad: ct.CANTIDADES
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
    archivos: list[UploadFile], cantidad: ct.CANTIDADES
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


def descargar_ejemplos_cantidades() -> io.BytesIO:
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for cantidad in ct.LISTA_CANTIDADES:
            df = generar_mock(
                (date(2020, 1, 1), date(2025, 12, 31)), cantidad, num_rows=1000
            )

            if cantidad == "siniestros":
                df = df.drop("apertura_reservas")

            excel_buffer = crear_excel({cantidad: df})
            zip_file.writestr(f"{cantidad}.xlsx", excel_buffer.getvalue())

    zip_buffer.seek(0)

    return zip_buffer


def crear_excel(hojas: dict[str, pl.DataFrame]) -> io.BytesIO:
    excel_buffer = io.BytesIO()

    with xlsxwriter.Workbook(excel_buffer) as writer:
        for hoja in list(hojas.keys()):
            hojas[hoja].write_excel(writer, worksheet=hoja)

    excel_buffer.seek(0)
    return excel_buffer


def descargar_ejemplo_segmentacion() -> io.BytesIO:
    with open("data/segmentacion_demo.xlsx", "rb") as file:
        buffer = io.BytesIO(file.read())
    buffer.seek(0)
    return buffer

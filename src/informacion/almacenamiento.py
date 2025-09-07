from pathlib import Path

import polars as pl
from sqlmodel import col, select

from src import constantes as ct
from src.dependencias import SessionDep
from src.logger_config import logger
from src.models import MetadataCantidades


def guardar_archivo(
    df: pl.DataFrame, session: SessionDep, metadata: MetadataCantidades
) -> None:
    df.write_parquet(metadata.ruta)
    metadata.numero_filas = df.height
    guardar_metadata_archivo(session, metadata)

    # En csv para poder visualizarlo facil, en caso de ser necesario
    ruta_csv = Path(metadata.ruta).with_suffix(".csv")
    df.write_csv(ruta_csv, separator="\t")

    logger.info(f"{metadata.numero_filas} registros almacenados en {metadata.ruta}.")


def guardar_metadata_archivo(session: SessionDep, metadata: MetadataCantidades) -> None:
    eliminar_metadata_archivo(session, metadata.ruta)
    session.add(metadata)
    session.commit()
    session.refresh(metadata)


def eliminar_metadata_archivo(session: SessionDep, ruta: str) -> None:
    try:
        existing_data = obtener_metadata_archivo(session, ruta)
        if existing_data:
            session.delete(existing_data)
            session.commit()
    except IndexError:
        pass


def obtener_metadata_archivo(session: SessionDep, ruta: str) -> MetadataCantidades:
    return session.exec(
        select(MetadataCantidades).where(MetadataCantidades.ruta == ruta)
    ).all()[0]


def obtener_cantidatos_controles(
    session: SessionDep, cantidad: ct.CANTIDADES
) -> list[dict[str, str]]:
    query = select(
        MetadataCantidades.ruta,
        MetadataCantidades.nombre_original,
        MetadataCantidades.origen,
    ).where(
        MetadataCantidades.cantidad == cantidad,
        col(MetadataCantidades.origen).in_(["extraccion", "carga_manual"]),
    )
    resultados = session.exec(query).all()
    return [
        {"ruta": ruta, "nombre": nombre, "origen": origen}
        for ruta, nombre, origen in resultados
    ]

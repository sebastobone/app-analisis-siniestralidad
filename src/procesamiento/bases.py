import polars as pl
from sqlmodel import col, select

from src import constantes as ct
from src import utils
from src.dependencias import SessionDep
from src.informacion import almacenamiento as alm
from src.logger_config import logger
from src.models import MetadataCantidades, Parametros
from src.procesamiento import base_primas_expuestos as bpdn
from src.procesamiento import base_siniestros as bsin
from src.validation import cantidades


def generar_bases_plantilla(p: Parametros, session: SessionDep) -> None:
    insumo_siniestros = consolidar_archivos_cantidades(p, "siniestros", session)
    insumo_primas = consolidar_archivos_cantidades(p, "primas", session)
    insumo_expuestos = consolidar_archivos_cantidades(p, "expuestos", session)

    base_triangulos, base_ult_ocurr = bsin.generar_bases_siniestros(
        insumo_siniestros, p.tipo_analisis, p.mes_inicio, p.mes_corte
    )

    base_triangulos.write_parquet("data/processed/base_triangulos.parquet")
    base_ult_ocurr.write_parquet("data/processed/base_ultima_ocurrencia.parquet")

    bpdn.generar_base_primas_expuestos(
        insumo_primas, "primas", p.negocio
    ).write_parquet("data/processed/primas.parquet")

    bpdn.generar_base_primas_expuestos(
        insumo_expuestos, "expuestos", p.negocio
    ).write_parquet("data/processed/expuestos.parquet")


def consolidar_archivos_cantidades(
    p: Parametros, cantidad: ct.CANTIDADES, session: SessionDep
) -> pl.LazyFrame:
    query_candidatos = select(MetadataCantidades.ruta).where(
        MetadataCantidades.cantidad == cantidad,
        col(MetadataCantidades.origen).in_(["extraccion", "carga_manual", "demo"]),
    )
    candidatos_iniciales = set(session.exec(query_candidatos).all())

    if not candidatos_iniciales:
        raise FileNotFoundError(
            f"No se encontraron archivos de {cantidad} almacenados."
        )

    query_archivo_post_cuadre = select(
        MetadataCantidades.ruta, MetadataCantidades.rutas_padres
    ).where(
        MetadataCantidades.cantidad == cantidad,
        MetadataCantidades.origen == "post_cuadre_contable",
    )
    archivo_post_cuadre = session.exec(query_archivo_post_cuadre).one_or_none()

    if archivo_post_cuadre:
        rutas_padres = set(archivo_post_cuadre[1])
        candidatos_finales = candidatos_iniciales - rutas_padres
        candidatos_finales.add(archivo_post_cuadre[0])
    else:
        candidatos_finales = candidatos_iniciales

    query_datos_archivos = select(
        MetadataCantidades.origen, MetadataCantidades.nombre_original
    ).where(col(MetadataCantidades.ruta).in_(candidatos_finales))

    datos_archivos = session.exec(query_datos_archivos).all()

    descripcion_archivos = ", ".join(
        f"{origen} - {nombre}" for origen, nombre in datos_archivos
    )

    logger.info(
        utils.limpiar_espacios_log(
            f"""
            Las bases para la plantilla se construiran a partir de los siguientes
            {len(candidatos_finales)} archivos de {cantidad}: {descripcion_archivos}.
            """
        )
    )

    dfs = [pl.read_parquet(ruta) for ruta in sorted(candidatos_iniciales)]

    df_consolidado = pl.DataFrame(pl.concat(dfs)).pipe(
        cantidades.organizar_archivo,
        p.negocio,
        (p.mes_inicio, p.mes_corte),
        cantidad,
        cantidad,
    )

    alm.guardar_archivo(
        df_consolidado,
        session,
        MetadataCantidades(
            ruta=f"data/consolidado/{cantidad}.parquet",
            nombre_original=f"{cantidad}.parquet",
            origen="consolidado",
            cantidad=cantidad,
            rutas_padres=list(candidatos_finales),
        ),
    )

    return df_consolidado.lazy()

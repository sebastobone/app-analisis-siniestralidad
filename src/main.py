import polars as pl

from src import utils
from src.controles_informacion import generacion as ctrl
from src.controles_informacion.evidencias import generar_evidencias_parametros
from src.extraccion.tera_connect import correr_query
from src.logger_config import logger
from src.models import Parametros
from src.procesamiento import base_primas_expuestos as bpdn
from src.procesamiento import base_siniestros as bsin
from src.procesamiento.autonomia import adds


async def correr_query_siniestros(p: Parametros) -> None:
    if p.negocio == "autonomia":
        await adds.sap_sinis_ced(p.mes_corte)

    if p.negocio == "demo":
        utils.generar_mock_siniestros(
            (utils.yyyymm_to_date(p.mes_inicio), utils.yyyymm_to_date(p.mes_corte))
        ).write_parquet("data/raw/siniestros.parquet")
        logger.info("Datos ficticios de siniestros generados.")
    else:
        await correr_query(
            f"data/queries/{p.negocio}/siniestros.sql",
            "data/raw/siniestros",
            "parquet",
            p,
        )


async def correr_query_primas(p: Parametros) -> None:
    if p.negocio == "autonomia":
        await adds.sap_primas_ced(p.mes_corte)

    if p.negocio == "demo":
        utils.generar_mock_primas(
            (utils.yyyymm_to_date(p.mes_inicio), utils.yyyymm_to_date(p.mes_corte))
        ).write_parquet("data/raw/primas.parquet")
        logger.info("Datos ficticios de primas generados.")
    else:
        await correr_query(
            f"data/queries/{p.negocio}/primas.sql", "data/raw/primas", "parquet", p
        )


async def correr_query_expuestos(p: Parametros) -> None:
    if p.negocio == "demo":
        utils.generar_mock_expuestos(
            (utils.yyyymm_to_date(p.mes_inicio), utils.yyyymm_to_date(p.mes_corte))
        ).write_parquet("data/raw/expuestos.parquet")
        logger.info("Datos ficticios de expuestos generados.")
    else:
        await correr_query(
            f"data/queries/{p.negocio}/expuestos.sql",
            "data/raw/expuestos",
            "parquet",
            p,
        )


async def generar_controles(p: Parametros) -> None:
    await ctrl.restablecer_salidas_queries("data/raw")

    await ctrl.verificar_existencia_afos(p.negocio)

    await ctrl.generar_controles("siniestros", p)
    await ctrl.generar_controles("primas", p)
    await ctrl.generar_controles("expuestos", p)

    await generar_evidencias_parametros(p.negocio, p.mes_corte)


def generar_bases_plantilla(p: Parametros) -> None:
    base_triangulos, base_ult_ocurr = bsin.generar_bases_siniestros(
        pl.scan_parquet("data/raw/siniestros.parquet"),
        p.tipo_analisis,
        utils.yyyymm_to_date(p.mes_inicio),
        utils.yyyymm_to_date(p.mes_corte),
    )

    base_triangulos.write_parquet("data/processed/base_triangulos.parquet")
    base_ult_ocurr.write_parquet("data/processed/base_ultima_ocurrencia.parquet")

    bpdn.generar_base_primas_expuestos(
        pl.scan_parquet("data/raw/primas.parquet"), "primas", p.negocio
    ).write_parquet("data/processed/primas.parquet")

    bpdn.generar_base_primas_expuestos(
        pl.scan_parquet("data/raw/expuestos.parquet"), "expuestos", p.negocio
    ).write_parquet("data/processed/expuestos.parquet")

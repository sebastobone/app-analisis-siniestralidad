import polars as pl

from src import utils
from src.controles_informacion import generacion as ctrl
from src.controles_informacion.evidencias import generar_evidencias_parametros
from src.extraccion.tera_connect import correr_query
from src.models import Parametros
from src.procesamiento import base_primas_expuestos as bpdn
from src.procesamiento import base_siniestros as bsin
from src.procesamiento.autonomia import adds, siniestros_gen


async def correr_query_siniestros(p: Parametros) -> None:
    if p.negocio == "autonomia":
        await correr_query(
            "data/queries/catalogos/planes.sql",
            "data/catalogos/planes",
            "parquet",
            p,
        )
        await correr_query(
            "data/queries/catalogos/sucursales.sql",
            "data/catalogos/sucursales",
            "parquet",
            p,
        )
        await correr_query(
            "data/queries/autonomia/siniestros_cedidos.sql",
            "data/raw/siniestros_cedidos",
            "parquet",
            p,
        )
        await correr_query(
            "data/queries/autonomia/siniestros_brutos.sql",
            "data/raw/siniestros_brutos",
            "parquet",
            p,
        )
        await siniestros_gen.main(p.mes_inicio, p.mes_corte, p.aproximar_reaseguro)
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
    await correr_query(
        f"data/queries/{p.negocio}/primas.sql", "data/raw/primas", "parquet", p
    )


async def correr_query_expuestos(p: Parametros) -> None:
    await correr_query(
        f"data/queries/{p.negocio}/expuestos.sql",
        "data/raw/expuestos",
        "parquet",
        p,
    )


async def generar_controles(p: Parametros) -> None:
    await ctrl.generar_controles("siniestros", p)
    await ctrl.generar_controles("primas", p)
    await ctrl.generar_controles("expuestos", p)

    await generar_evidencias_parametros(p.negocio, p.mes_corte)


def generar_bases_plantilla(p: Parametros) -> None:
    _, _, _ = bsin.generar_bases_siniestros(
        pl.scan_parquet("data/raw/siniestros.parquet"),
        p.tipo_analisis,
        utils.yyyymm_to_date(p.mes_inicio),
        utils.yyyymm_to_date(p.mes_corte),
    )

    bpdn.generar_base_primas_expuestos(
        pl.scan_parquet("data/raw/primas.parquet"), "primas", p.negocio
    ).write_parquet("data/processed/primas.parquet")

    bpdn.generar_base_primas_expuestos(
        pl.scan_parquet("data/raw/expuestos.parquet"), "expuestos", p.negocio
    ).write_parquet("data/processed/expuestos.parquet")

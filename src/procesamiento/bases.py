import polars as pl

from src.models import Parametros
from src.procesamiento import base_primas_expuestos as bpdn
from src.procesamiento import base_siniestros as bsin


def generar_bases_plantilla(p: Parametros) -> None:
    base_triangulos, base_ult_ocurr = bsin.generar_bases_siniestros(
        pl.scan_parquet("data/raw/siniestros.parquet"),
        p.tipo_analisis,
        p.mes_inicio,
        p.mes_corte,
    )

    base_triangulos.write_parquet("data/processed/base_triangulos.parquet")
    base_ult_ocurr.write_parquet("data/processed/base_ultima_ocurrencia.parquet")

    bpdn.generar_base_primas_expuestos(
        pl.scan_parquet("data/raw/primas.parquet"), "primas", p.negocio
    ).write_parquet("data/processed/primas.parquet")

    bpdn.generar_base_primas_expuestos(
        pl.scan_parquet("data/raw/expuestos.parquet"), "expuestos", p.negocio
    ).write_parquet("data/processed/expuestos.parquet")

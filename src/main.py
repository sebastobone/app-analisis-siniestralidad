from src.procesamiento.autonomia import adds
from src.procesamiento.autonomia import siniestros_gen
from src.extraccion.tera_connect import correr_query
from src.controles_informacion import controles_informacion as ctrl
from src.procesamiento import base_siniestros as bsin
from src.procesamiento import base_primas_expuestos as bpdn
import polars as pl


def correr_query_siniestros(
    negocio: str,
    mes_inicio: int,
    mes_corte: int,
    tipo_analisis: str,
    aproximar_reaseguro: bool,
) -> None:
    if negocio == "autonomia":
        correr_query(
            "data/queries/catalogos/planes.sql",
            "data/catalogos/planes",
            "parquet",
            negocio,
            mes_inicio,
            mes_corte,
            aproximar_reaseguro,
        )
        correr_query(
            "data/queries/catalogos/sucursales.sql",
            "data/catalogos/sucursales",
            "parquet",
            negocio,
            mes_inicio,
            mes_corte,
            aproximar_reaseguro,
        )
        adds.sap_sinis_ced(mes_corte)
        correr_query(
            "data/queries/autonomia/siniestros_cedidos.sql",
            "data/raw/siniestros_cedidos",
            "parquet",
            negocio,
            mes_inicio,
            mes_corte,
            aproximar_reaseguro,
        )
        correr_query(
            "data/queries/autonomia/siniestros_brutos.sql",
            "data/raw/siniestros_brutos",
            "parquet",
            negocio,
            mes_inicio,
            mes_corte,
            aproximar_reaseguro,
        )
        siniestros_gen.main(mes_inicio, mes_corte, aproximar_reaseguro)
    else:
        correr_query(
            f"data/queries/{negocio}/siniestros.sql",
            "data/raw/siniestros",
            "parquet",
            negocio,
            mes_inicio,
            mes_corte,
            aproximar_reaseguro,
        )

    bsin.aperturas(negocio)
    bsin.bases_siniestros(tipo_analisis, mes_inicio, mes_corte)


def correr_query_primas(
    negocio: str,
    mes_inicio: int,
    mes_corte: int,
    aproximar_reaseguro: bool,
) -> None:
    if negocio == "autonomia":
        adds.sap_primas_ced(mes_corte)
    correr_query(
        f"data/queries/{negocio}/primas.sql",
        "data/raw/primas",
        "parquet",
        negocio,
        mes_inicio,
        mes_corte,
        aproximar_reaseguro,
    )
    bpdn.bases_primas_expuestos(
        pl.scan_parquet("data/raw/primas.parquet"), "primas", negocio
    ).write_parquet("data/processed/primas.parquet")


def correr_query_expuestos(
    negocio: str,
    mes_inicio: int,
    mes_corte: int,
) -> None:
    correr_query(
        f"data/queries/{negocio}/expuestos.sql",
        "data/raw/expuestos",
        "parquet",
        negocio,
        mes_inicio,
        mes_corte,
    )
    bpdn.bases_primas_expuestos(
        pl.scan_parquet("data/raw/expuestos.parquet"), "expuestos", negocio
    ).write_parquet("data/processed/expuestos.parquet")


def generar_controles(
    negocio: str,
    mes_corte: int,
    cuadre_contable_sinis: bool,
    add_fraude_soat: bool,
    cuadre_contable_primas: bool,
) -> None:
    ctrl.set_permissions("data/controles_informacion", "write")

    ctrl.generar_controles(
        "siniestros",
        negocio,
        mes_corte,
        cuadre_contable_sinis=cuadre_contable_sinis,
        add_fraude_soat=add_fraude_soat,
    )
    ctrl.generar_controles(
        "primas", negocio, mes_corte, cuadre_contable_primas=cuadre_contable_primas
    )
    ctrl.generar_controles("expuestos", negocio, mes_corte)

    ctrl.evidencias_parametros(negocio, mes_corte)
    ctrl.set_permissions("data/controles_informacion", "read")

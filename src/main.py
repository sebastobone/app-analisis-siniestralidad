from src.procesamiento.autonomia import adds
from src.procesamiento.autonomia import siniestros_gen
from src.extraccion.tera_connect import read_query
from src.controles_informacion import controles_informacion as ctrl
from src.procesamiento import base_siniestros as bsin
from src.procesamiento import base_primas_expuestos as bpdn
import xlwings as xw


def correr_query_siniestros(
    negocio: str,
    mes_inicio: int,
    mes_corte: int,
    tipo_analisis: str,
    aproximar_reaseguro: bool,
) -> None:
    if negocio == "autonomia":
        read_query(
            "data/queries/catalogos/planes.sql",
            "data/catalogos/planes",
            "parquet",
            negocio,
            mes_inicio,
            mes_corte,
            aproximar_reaseguro,
        )
        read_query(
            "data/queries/catalogos/sucursales.sql",
            "data/catalogos/sucursales",
            "parquet",
            negocio,
            mes_inicio,
            mes_corte,
            aproximar_reaseguro,
        )
        adds.sap_sinis_ced(mes_corte)
        read_query(
            "data/queries/autonomia/siniestros_cedidos.sql",
            "data/raw/siniestros_cedidos",
            "parquet",
            negocio,
            mes_inicio,
            mes_corte,
            aproximar_reaseguro,
        )
        read_query(
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
        read_query(
            f"data/queries/{negocio}/siniestros.sql",
            "data/raw/siniestros",
            "parquet",
            negocio,
            mes_inicio,
            mes_corte,
            aproximar_reaseguro,
        )

    bsin.aperturas()
    bsin.bases_siniestros(tipo_analisis, mes_inicio, mes_corte)


def correr_query_primas(
    negocio: str,
    mes_inicio: int,
    mes_corte: int,
    tipo_analisis: str,
    aproximar_reaseguro: bool,
) -> None:
    if negocio == "autonomia":
        adds.sap_primas_ced(mes_corte)
    read_query(
        f"data/queries/{negocio}/primas.sql",
        "data/raw/primas",
        "parquet",
        negocio,
        mes_inicio,
        mes_corte,
        aproximar_reaseguro,
    )
    bpdn.bases_primas_expuestos("primas")


def correr_query_expuestos(
    negocio: str,
    mes_inicio: int,
    mes_corte: int,
    tipo_analisis: str,
    aproximar_reaseguro: bool,
) -> None:
    read_query(
        f"data/queries/{negocio}/expuestos.sql",
        "data/raw/expuestos",
        "parquet",
        negocio,
        mes_inicio,
        mes_corte,
        aproximar_reaseguro,
    )
    bpdn.bases_primas_expuestos("expuestos")


def generar_controles(negocio: str, mes_corte: int) -> None:
    ctrl.set_permissions("data/controles_informacion", "write")

    ctrl.generar_controles("siniestros", negocio, mes_corte)
    ctrl.generar_controles("primas", negocio, mes_corte)
    ctrl.generar_controles("expuestos", negocio, mes_corte)

    ctrl.evidencias_parametros()
    ctrl.set_permissions("data/controles_informacion", "read")


def abrir_plantilla(plantilla_path: str) -> xw.Book:
    xw.Book(plantilla_path).set_mock_caller()
    wb = xw.Book.caller()

    wb.macro("eliminar_modulos")
    wb.macro("crear_modulos")

    return wb

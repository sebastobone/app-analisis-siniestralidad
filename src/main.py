from procesamiento.autonomia import adds
from procesamiento.autonomia import siniestros_gen
from extraccion.tera_connect import read_query
from controles_informacion import controles_informacion as ctrl
from procesamiento import base_siniestros as bsin
from procesamiento import base_primas_expuestos as bpdn
import plantilla


def correr_query_siniestros(negocio: str) -> None:
    if negocio == "autonomia":
        read_query(
            "data/queries/catalogos/planes.sql", "data/catalogos/planes", "parquet"
        )
        read_query(
            "data/queries/catalogos/sucursales.sql",
            "data/catalogos/sucursales",
            "parquet",
        )
        adds.sap_sinis_ced()
        read_query(
            "data/queries/autonomia/siniestros_cedidos.sql",
            "data/raw/siniestros_cedidos",
            "parquet",
        )
        read_query(
            "data/queries/autonomia/siniestros_brutos.sql",
            "data/raw/siniestros_brutos",
            "parquet",
        )
        siniestros_gen.main()
    else:
        read_query(f"data/queries/{negocio}/siniestros.sql")

    bsin.aperturas()
    bsin.bases_siniestros()


def correr_query_primas(negocio: str) -> None:
    if negocio == "autonomia":
        adds.sap_primas_ced()
    read_query(f"data/queries/{negocio}/primas.sql", "data/raw/primas", "parquet")
    bpdn.bases_primas_expuestos("primas")


def correr_query_expuestos(negocio: str) -> None:
    read_query(f"data/queries/{negocio}/expuestos.sql", "data/raw/expuestos", "parquet")
    bpdn.bases_primas_expuestos("expuestos")


def generar_controles() -> None:
    ctrl.set_permissions("data/controles_informacion", "write")

    ctrl.generar_controles("siniestros")
    ctrl.generar_controles("primas")
    ctrl.generar_controles("expuestos")

    ctrl.evidencias_parametros()
    ctrl.set_permissions("data/controles_informacion", "read")


def abrir_plantilla(path: str) -> None:
    return plantilla.abrir_plantilla(path)


def preparar_plantilla(wb) -> None:
    return plantilla.preparar_plantilla(wb)

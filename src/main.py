from src.procesamiento.autonomia import adds
from src.procesamiento.autonomia import siniestros_gen
from src.extraccion.tera_connect import read_query
from src.controles_informacion import controles_informacion as ctrl
from src.procesamiento import base_siniestros as bsin
from src.procesamiento import base_primas_expuestos as bpdn
from src import plantilla
import xlwings as xw


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


def abrir_plantilla(plantilla_path: str) -> xw.Book:
    xw.Book(plantilla_path).set_mock_caller()
    wb = xw.Book.caller()

    wb.macro("eliminar_modulos")
    wb.macro("crear_modulos")

    return wb


def modos_plantilla(wb: xw.Book, modo: str, plant: str | None = None) -> None:
    if modo == "preparar":
        plantilla.preparar_plantilla(wb)
    elif modo == "generar":
        plantilla.generar_plantilla(
            wb,
            str(plant),
            wb.sheets["Aperturas"]["A2"].value,
            wb.sheets["Atributos"]["A2"].value,
        )
    elif modo in ("guardar", "traer"):
        plantilla.guardar_traer_fn(
            wb,
            modo,
            str(plant),
            wb.sheets["Aperturas"]["A2"].value,
            wb.sheets["Atributos"]["A2"].value,
        )
    elif modo == "almacenar":
        plantilla.almacenar_analisis(wb)
    elif modo == "guardar_todo":
        plantilla.traer_guardar_todo(wb, str(plant))
    elif modo == "traer_guardar_todo":
        plantilla.traer_guardar_todo(wb, str(plant), traer=True)

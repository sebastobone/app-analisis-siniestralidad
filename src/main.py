from extraction.tera_connect import read_query
from controles_informacion import controles_informacion as ctrl
from processing import data_processing as pr
import plantilla

read_query("siniestros")
read_query("primas")
read_query("expuestos")

# Descomentar la siguiente fila si se necesita volver a correr la info, permite sobrescribir los archivos de controles
ctrl.set_permissions("data/controles_informacion", "write")

ctrl.generar_controles("siniestros")
ctrl.generar_controles("primas")
ctrl.generar_controles("expuestos")

ctrl.evidencias_parametros()
ctrl.set_permissions("data/controles_informacion", "read")

pr.aperturas()
pr.bases_siniestros()
pr.bases_primas_expuestos("expuestos", ["expuestos", "vigentes"])
pr.bases_primas_expuestos(
    "primas",
    [
        "prima_bruta",
        "prima_bruta_devengada",
        "prima_retenida",
        "prima_retenida_devengada",
    ],
)

plantilla.modos("prep")

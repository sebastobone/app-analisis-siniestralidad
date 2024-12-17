from procesamiento.autonomia import adds
import procesamiento as pr
from procesamiento.autonomia import siniestros_gen
import constantes as ct
from extraccion.tera_connect import read_query
from controles_informacion import controles_informacion as ctrl
from procesamiento import base_siniestros as bsin
from procesamiento import base_primas_expuestos as bpdn
# import plantilla

# read_query("data/queries/catalogos/planes.sql")
# read_query("data/queries/catalogos/sucursales.sql")


# if ct.NEGOCIO == "autonomia":
    # adds.main()
    # read_query("data/queries/autonomia/siniestros_cedidos.sql")
    # read_query("data/queries/autonomia/siniestros_brutos.sql")
    # siniestros_gen.main()
    # read_query("data/queries/autonomia/primas.sql")
    # read_query("data/queries/autonomia/expuestos.sql")

# Descomentar la siguiente fila si se necesita volver a correr la info, permite sobrescribir los archivos de controles
ctrl.set_permissions("data/controles_informacion", "write")

ctrl.generar_controles("siniestros")
# ctrl.generar_controles("primas")
# ctrl.generar_controles("expuestos")

# ctrl.evidencias_parametros()
# ctrl.set_permissions("data/controles_informacion", "read")

# bsin.aperturas()
# bsin.bases_siniestros()
# bpdn.bases_primas_expuestos("expuestos", ["expuestos", "vigentes"])
# bpdn.bases_primas_expuestos(
#     "primas",
#     [
#         "prima_bruta",
#         "prima_bruta_devengada",
#         "prima_retenida",
#         "prima_retenida_devengada",
#     ],
# )

# # plantilla.modos("prep")

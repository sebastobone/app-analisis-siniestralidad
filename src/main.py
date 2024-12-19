from procesamiento.autonomia import adds
from procesamiento.autonomia import siniestros_gen
from constantes import NEGOCIO
from extraccion.tera_connect import read_query
from controles_informacion import controles_informacion as ctrl
from procesamiento import base_siniestros as bsin
from procesamiento import base_primas_expuestos as bpdn

read_query("data/queries/catalogos/planes.sql", "data/catalogos/planes", "parquet")
read_query(
    "data/queries/catalogos/sucursales.sql", "data/catalogos/sucursales", "parquet"
)

if NEGOCIO == "autonomia":
    adds.main()
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
    read_query("data/queries/autonomia/primas.sql", "data/raw/primas", "parquet")
    read_query("data/queries/autonomia/expuestos.sql", "data/raw/expuestos", "parquet")

print("Generando controles de informacion...")

# Descomentar la siguiente fila si se necesita volver a correr la info, permite sobrescribir los archivos de controles
ctrl.set_permissions("data/controles_informacion", "write")

ctrl.generar_controles("siniestros")
ctrl.generar_controles("primas")
ctrl.generar_controles("expuestos")

ctrl.evidencias_parametros()
ctrl.set_permissions("data/controles_informacion", "read")

print("Creando inputs para la plantilla...")

bsin.aperturas()
bsin.bases_siniestros()
bpdn.bases_primas_expuestos("expuestos", ["expuestos", "vigentes"])
bpdn.bases_primas_expuestos(
    "primas",
    [
        "prima_bruta",
        "prima_bruta_devengada",
        "prima_retenida",
        "prima_retenida_devengada",
    ],
)

print("Extraccion y procesamiento de la informacion terminado exitosamente.")

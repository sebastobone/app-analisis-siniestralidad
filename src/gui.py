import tkinter as tk
from tkinter import filedialog, messagebox
from procesamiento.autonomia import adds
import procesamiento as pr
from procesamiento.autonomia import siniestros_gen
import constantes as ct
from extraccion.tera_connect import read_query
from controles_informacion import controles_informacion as ctrl
from procesamiento import base_siniestros as bsin
from procesamiento import base_primas_expuestos as bpdn
import plantilla

# read_query("data/queries/catalogos/planes.sql", "data/catalogos/planes", "parquet")
# read_query(
#     "data/queries/catalogos/sucursales.sql", "data/catalogos/sucursales", "parquet"
# )


# if ct.NEGOCIO == "autonomia":
# adds.main()
# read_query(
#     "data/queries/autonomia/siniestros_cedidos.sql",
#     "data/raw/siniestros_cedidos",
#     "parquet",
# )
# read_query(
#     "data/queries/autonomia/siniestros_brutos.sql",
#     "data/raw/siniestros_brutos",
#     "parquet",
# )
# siniestros_gen.main()
# read_query("data/queries/autonomia/primas.sql")
# read_query("data/queries/autonomia/expuestos.sql")

# Descomentar la siguiente fila si se necesita volver a correr la info, permite sobrescribir los archivos de controles
# ctrl.set_permissions("data/controles_informacion", "write")

# ctrl.generar_controles("siniestros")
# ctrl.generar_controles("primas")
# ctrl.generar_controles("expuestos")

# ctrl.evidencias_parametros()
# ctrl.set_permissions("data/controles_informacion", "read")

def gen_bases():
    wtf = filedialog.askopenfilename(title="Select data file")
    try:
        wtf()
    except Exception as e:
        messagebox.showerror("Error", str(e))
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


WB = plantilla.abrir_plantilla(filedialog.askopenfilename())



def preparar_plantilla_cmd():
    plantilla.preparar_plantilla(WB)


def modos_plantilla_cmd():
    modo_sel = modo_var.get()
    plantilla_sel = plant_var.get()
    plantilla.modos(modo_sel, plantilla_sel)


def almacenar_plantilla_cmd():
    plantilla.preparar_plantilla(WB)


root = tk.Tk()
root.title("Plantilla de seguimiento")
root.geometry("400x300")

boton_preparar = tk.Button(root, text="Preparar plantilla", command=preparar_plantilla_cmd)
boton_preparar.pack(pady=20)

modo_var = tk.StringVar(value="gen")
modo_label = tk.Label(root, text="Seleccione accion:")
modo_label.pack()
modo_menu = tk.OptionMenu(root, modo_var, "gen", "guardar", "traer")
modo_menu.pack()

plant_var = tk.StringVar(value="plata")
plant_label = tk.Label(root, text="Seleccione plantilla:")
plant_label.pack()
plant_menu = tk.OptionMenu(root, plant_var, "plata", "frec", "seve", "entremes")
plant_menu.pack()

boton_ejecutar = tk.Button(root, text="Ejecutar accion", command=modos_plantilla_cmd)
boton_ejecutar.pack(pady=20)

root.mainloop()





# # plantilla.modos("prep")

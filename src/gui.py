import tkinter as tk
from tkinter import filedialog, messagebox
import constantes as ct
import plantilla


WB = plantilla.abrir_plantilla(filedialog.askopenfilename())


def preparar_plantilla_cmd():
    try:
        plantilla.preparar_plantilla(WB)
    except Exception as e:
        messagebox.showerror("Error", str(e))


def modos_plantilla_cmd():
    modo_sel = modo_var.get()
    plantilla_sel = plant_var.get()
    plantilla.modos(WB, modo_sel, plantilla_sel)


def almacenar_analisis_cmd():
    plantilla.almacenar_analisis(WB)


root = tk.Tk()
root.title("Plantilla de seguimiento")
root.geometry("400x300")

boton_preparar = tk.Button(
    root, text="Preparar plantilla", command=preparar_plantilla_cmd
)
boton_preparar.pack(pady=20)

modo_var = tk.StringVar(value="generar")
modo_label = tk.Label(root, text="Seleccione accion:")
modo_label.pack()
modo_menu = tk.OptionMenu(root, modo_var, "generar", "guardar", "traer")
modo_menu.pack()

if ct.TIPO_ANALISIS == "Entremes":
    main_plant = "entremes"
    otras_plant = []
elif ct.TIPO_ANALISIS == "Triangulos":
    main_plant = "plata"
    otras_plant = ["frec", "seve"]

plant_var = tk.StringVar(value=main_plant)
plant_label = tk.Label(root, text="Seleccione plantilla:")
plant_label.pack()
plant_menu = tk.OptionMenu(root, plant_var, main_plant, *otras_plant)
plant_menu.pack()

boton_ejecutar = tk.Button(root, text="Ejecutar accion", command=modos_plantilla_cmd)
boton_ejecutar.pack(pady=20)

boton_almacenar = tk.Button(
    root, text="Almacenar analisis", command=almacenar_analisis_cmd
)
boton_almacenar.pack(pady=20)

root.mainloop()


# # plantilla.modos("prep")

from flask import Flask, request, render_template, redirect, url_for
import main

app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

negocio = None
message = None
wb = None
wb_path = None


@app.route("/", methods=["GET", "POST"])
def index():
    global negocio, message, wb_path
    if request.method == "POST":
        negocio = request.form.get("dropdown_negocio")
        wb_path = request.form.get("wb_path")
    return render_template("index.html", negocio=negocio, message=message)


@app.route("/boton_correr_query_siniestros", methods=["POST"])
def boton_correr_siniestros():
    global message
    message = "Query de siniestros ejecutado exitosamente."
    main.correr_query_siniestros(negocio)
    return redirect(url_for("index"))


@app.route("/boton_correr_query_primas", methods=["POST"])
def boton_correr_query_primas():
    global message
    message = "Query de primas ejecutado exitosamente."
    main.correr_query_primas(negocio)
    return redirect(url_for("index"))


@app.route("/boton_correr_query_expuestos", methods=["POST"])
def boton_correr_query_expuestos():
    global message
    message = "Query de expuestos ejecutado exitosamente."
    main.correr_query_primas(negocio)
    return redirect(url_for("index"))


@app.route("/boton_generar_controles", methods=["POST"])
def boton_generar_controles():
    main.generar_controles()
    return redirect(url_for("index"))


@app.route("/boton_abrir_plantilla", methods=["POST"])
def boton_abrir_plantilla():
    global wb
    wb = main.abrir_plantilla(wb_path)
    return redirect(url_for("index"))


@app.route("/boton_preparar_plantilla", methods=["POST"])
def boton_preparar_plantilla():
    main.modos_plantilla(wb, "preparar")
    return redirect(url_for("index"))


@app.route("/boton_generar_plantilla_frecuencia", methods=["POST"])
def boton_generar_plantilla_frecuencia():
    main.modos_plantilla(wb, "generar", "frec")
    return redirect(url_for("index"))


@app.route("/boton_guardar_plantilla_frecuencia", methods=["POST"])
def boton_guardar_plantilla_frecuencia():
    main.modos_plantilla(wb, "guardar", "frec")
    return redirect(url_for("index"))


@app.route("/boton_traer_plantilla_frecuencia", methods=["POST"])
def boton_traer_plantilla_frecuencia():
    main.modos_plantilla(wb, "traer", "frec")
    return redirect(url_for("index"))


@app.route("/boton_generar_plantilla_severidad", methods=["POST"])
def boton_generar_plantilla_severidad():
    main.modos_plantilla(wb, "generar", "seve")
    return redirect(url_for("index"))


@app.route("/boton_guardar_plantilla_severidad", methods=["POST"])
def boton_guardar_plantilla_severidad():
    main.modos_plantilla(wb, "guardar", "seve")
    return redirect(url_for("index"))


@app.route("/boton_traer_plantilla_severidad", methods=["POST"])
def boton_traer_plantilla_severidad():
    main.modos_plantilla(wb, "traer", "seve")
    return redirect(url_for("index"))


@app.route("/boton_generar_plantilla_plata", methods=["POST"])
def boton_generar_plantilla_plata():
    main.modos_plantilla(wb, "generar", "plata")
    return redirect(url_for("index"))


@app.route("/boton_guardar_plantilla_plata", methods=["POST"])
def boton_guardar_plantilla_plata():
    main.modos_plantilla(wb, "guardar", "plata")
    return redirect(url_for("index"))


@app.route("/boton_traer_plantilla_plata", methods=["POST"])
def boton_traer_plantilla_plata():
    main.modos_plantilla(wb, "traer", "plata")
    return redirect(url_for("index"))


@app.route("/boton_almacenar_analisis", methods=["POST"])
def boton_almacenar_analisis():
    main.modos_plantilla(wb, "almacenar")
    return redirect(url_for("index"))



if __name__ == "__main__":
    app.run(debug=True)

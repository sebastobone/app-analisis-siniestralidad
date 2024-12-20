from flask import Flask, request, render_template, redirect, url_for
import main

app = Flask(__name__)

negocio = None
message = None


@app.route("/", methods=["GET", "POST"])
def index():
    global negocio
    if request.method == "POST":
        negocio = request.form.get("dropdown_negocio")
        print(negocio)
    return render_template("index.html", selected=negocio)


@app.route("/boton_correr_query_siniestros", methods=["POST"])
def boton_correr_siniestros():
    global message
    main.correr_query_siniestros(negocio)
    message = "Query de siniestros ejecutado exitosamente."
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(debug=True)

from flask import Flask, request, jsonify
import plantilla

app = Flask(__name__)

@app.route("/correr_macro", methods=["POST"])
def correr_macro():

    data = request.json
    modo = data.get("modo", "DEFAULT_MODE")

    try:
        plantilla(modo)
        return jsonify({"status": "success", "message": f"{modo} ejecutado."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})
    
if __name__ == "__main__":
    app.run(port=5000)
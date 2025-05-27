import { showToast } from "./toast.js";

document
  .getElementById("prepararPlantilla")
  .addEventListener("click", async function (event) {
    event.preventDefault();

    showToast("Ejecutando...");

    const response = await fetch(`http://127.0.0.1:8000/preparar-plantilla`, {
      method: "POST",
    });

    const data = await response.json();

    if (data.status == "multiples_resultados_anteriores") {
      document.getElementById("popup-overlay").style.display = "flex";
      document
        .getElementById("analisisAnteriorTriangulos")
        .addEventListener("click", () => {
          seleccionarTipoAnalisisAnterior("triangulos");
        });
      document
        .getElementById("analisisAnteriorEntremes")
        .addEventListener("click", () => {
          seleccionarTipoAnalisisAnterior("entremes");
        });
    } else {
      showToast(
        response.ok ? "Ejecucion exitosa" : "Error en la ejecucion",
        response.ok ? "success" : "error",
      );
    }
  });

async function seleccionarTipoAnalisisAnterior(tipo) {
  document.getElementById("popup-overlay").style.display = "none";
  showToast("Ejecutando...");

  const response = await fetch(
    `http://127.0.0.1:8000/preparar-plantilla?tipo_analisis_anterior=${tipo}`,
    {
      method: "POST",
    },
  );
  showToast(
    response.ok ? "Ejecucion exitosa" : "Error en la ejecucion",
    response.ok ? "success" : "error",
  );
}

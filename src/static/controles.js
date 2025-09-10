import { showToast } from "./toast.js";
import { handleRequest } from "./apiUtils.js";

function obtenerArchivosSeleccionados(containerId) {
  return Array.from(
    document.querySelectorAll(`#${containerId} input[type=checkbox]:checked`),
  ).map((checkbox) => checkbox.value);
}

document
  .getElementById("generarControles")
  .addEventListener("click", async (event) => {
    event.preventDefault();
    showToast("Generando controles...");

    const cuadrar_siniestros =
      document.getElementById("cuadrarSiniestros").checked;
    const cuadrar_primas = document.getElementById("cuadrarPrimas").checked;

    const archivos_siniestros = obtenerArchivosSeleccionados(
      "candidatos_siniestros",
    );
    const archivos_primas = obtenerArchivosSeleccionados("candidatos_primas");
    const archivos_expuestos = obtenerArchivosSeleccionados(
      "candidatos_expuestos",
    );

    const controles = {
      cuadrar_siniestros,
      cuadrar_primas,
      archivos_siniestros,
      archivos_primas,
      archivos_expuestos,
    };

    const formData = new FormData();
    formData.append("controles", JSON.stringify(controles));

    const afoGenerales = document.getElementById("afoGenerales");
    const afoVida = document.getElementById("afoVida");

    if (afoGenerales && afoGenerales.files.length > 0) {
      formData.append("generales", afoGenerales.files[0]);
    }
    if (afoVida && afoVida.files.length > 0) {
      formData.append("vida", afoVida.files[0]);
    }

    await handleRequest(
      () =>
        fetch("http://127.0.0.1:8000/generar-controles", {
          method: "POST",
          body: formData,
        }),
      "Error al generar controles",
    );
  });

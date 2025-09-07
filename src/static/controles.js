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

    const siniestros = obtenerArchivosSeleccionados("candidatos_siniestros");
    const primas = obtenerArchivosSeleccionados("candidatos_primas");
    const expuestos = obtenerArchivosSeleccionados("candidatos_expuestos");

    await handleRequest(
      () =>
        fetch("http://127.0.0.1:8000/generar-controles", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ siniestros, primas, expuestos }),
        }),
      "Error al generar controles",
    );
  });

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

    await handleRequest(
      () =>
        fetch("http://127.0.0.1:8000/generar-controles", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            cuadrar_siniestros,
            cuadrar_primas,
            archivos_siniestros,
            archivos_primas,
            archivos_expuestos,
          }),
        }),
      "Error al generar controles",
    );
  });

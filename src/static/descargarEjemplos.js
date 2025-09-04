import { showToast } from "./toast.js";

/**
 * @param {string} botonId
 * @param {string} url
 * @param {string} nombreDescarga
 * @param {string} mensajeInicio
 * @param {string} mensajeExito
 * @param {string} mensajeError
 */
function descargarEjemplos(
  botonId,
  url,
  nombreDescarga,
  mensajeInicio,
  mensajeExito,
  mensajeError,
) {
  const button = document.getElementById(botonId);

  button.addEventListener("click", async () => {
    try {
      showToast(mensajeInicio);

      const response = await fetch(url, { method: "GET" });
      if (!response.ok) throw new Error(errorMessage);

      const blob = await response.blob();
      const downloadUrl = URL.createObjectURL(blob);

      const a = document.createElement("a");
      a.href = downloadUrl;
      a.download = nombreDescarga;
      a.click();
      a.remove();

      URL.revokeObjectURL(downloadUrl);
      showToast(mensajeExito, "success");
    } catch (error) {
      showToast(mensajeError, "error");
      console.error(error);
    }
  });
}

descargarEjemplos(
  "descargarEjemploSegmentacion",
  "http://127.0.0.1:8000/descargar-ejemplo-segmentacion",
  "segmentacion.xlsx",
  "Descargando ejemplo de segmentación...",
  "Ejemplo descargado correctamente",
  "Error al descargar el ejemplo",
);

descargarEjemplos(
  "descargarEjemplosCantidades",
  "http://127.0.0.1:8000/descargar-ejemplos-cantidades",
  "ejemplos.zip",
  "Descargando ejemplos...",
  "Ejemplos descargados correctamente",
  "Error al descargar los ejemplos",
);

import { showToast } from "./toast.js";

document
  .getElementById("descargarEjemplos")
  .addEventListener("click", async () => {
    try {
      showToast("Descargando ejemplos...");

      const response = await fetch("http://127.0.0.1:8000/descargar-ejemplos", {
        method: "GET",
      });

      if (!response.ok) throw new Error("Error al descargar los ejemplos");

      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "ejemplos.zip";
      a.click();

      a.remove();
      URL.revokeObjectURL(url);
      showToast("Ejemplos descargados correctamente", "success");
    } catch (error) {
      showToast(error.message, "error");
    }
  });

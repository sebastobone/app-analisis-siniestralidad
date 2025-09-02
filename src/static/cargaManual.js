import { showToast } from "./toast.js";

document.querySelectorAll(".cargaManual").forEach((button) => {
  button.addEventListener("click", async function (event) {
    event.preventDefault();

    try {
      showToast("Cargando archivos...");

      const formData = new FormData();
      var segmentacion = document.getElementById("archivoSegmentacion").files;
      var siniestros = document.getElementById("archivosSiniestros").files;
      var primas = document.getElementById("archivosSiniestros").files;
      var expuestos = document.getElementById("archivosSiniestros").files;

      if (segmentacion.length > 0) {
        formData.append("segmentacion", segmentacion[0]);
      }

      if (siniestros.length > 0) {
        for (let i = 0; i < siniestros.length; i++) {
          formData.append("siniestros", siniestros[i]);
        }
      }

      if (primas.length > 0) {
        for (let i = 0; i < primas.length; i++) {
          formData.append("primas", primas[i]);
        }
      }

      if (expuestos.length > 0) {
        for (let i = 0; i < expuestos.length; i++) {
          formData.append("expuestos", expuestos[i]);
        }
      }

      const endpoint = button.getAttribute("endpoint");
      const response = await fetch(`http://127.0.0.1:8000/${endpoint}`, {
        method: "POST",
        body: formData,
      });

      if (!response.ok) throw new Error("Error al cargar los archivos");

      const data = await response.json();
      showToast(data.message, "success");
    } catch (error) {
      showToast(error.message, "error");
      throw error;
    }
  });
});

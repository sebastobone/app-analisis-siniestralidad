import { showToast } from "./toast.js";
import { actualizarCheckboxesCandidatos, fetchSimple } from "./apiUtils.js";

document
  .getElementById("cargarArchivos")
  .addEventListener("click", async function (event) {
    event.preventDefault();

    try {
      showToast("Cargando archivos...");

      const formData = new FormData();
      var siniestros = document.getElementById("archivosSiniestros").files;
      var primas = document.getElementById("archivosPrimas").files;
      var expuestos = document.getElementById("archivosExpuestos").files;

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

      const response = await fetch(`http://127.0.0.1:8000/cargar-archivos`, {
        method: "POST",
        body: formData,
      });

      if (!response.ok) throw new Error("Error al cargar los archivos");

      const data = await response.json();
      showToast(data.message, "success");

      actualizarCheckboxesCandidatos(data.candidatos_siniestros, "siniestros");
      actualizarCheckboxesCandidatos(data.candidatos_primas, "primas");
    } catch (error) {
      showToast(error.message, "error");
      throw error;
    }
  });

document
  .getElementById("eliminarArchivosCargados")
  .addEventListener("click", async (event) => {
    event.preventDefault();
    showToast("Eliminando archivos...");
    const data = await fetchSimple(
      "eliminar-archivos-cargados",
      "Error al eliminar los archivos",
    );
    actualizarCheckboxesCandidatos(data.candidatos_siniestros, "siniestros");
    actualizarCheckboxesCandidatos(data.candidatos_primas, "primas");
  });

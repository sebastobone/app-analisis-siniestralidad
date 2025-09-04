import { showToast } from "./toast.js";

document
  .getElementById("traerParametros")
  .addEventListener("click", async function (event) {
    event.preventDefault();

    try {
      const response = await fetch("http://127.0.0.1:8000/traer-parametros");
      const parametros = await response.json();

      document.getElementById("negocio").value = parametros.negocio;
      document.getElementById("mes_inicio").value = parametros.mes_inicio;
      document.getElementById("mes_corte").value = parametros.mes_corte;
      document.getElementById("tipo_analisis").value = parametros.tipo_analisis;
      document.getElementById("nombre_plantilla").value =
        parametros.nombre_plantilla;

      if (!response.ok) throw new Error("Error al traer los parametros");
      showToast("Parametros traidos correctamente", "success");
    } catch (error) {
      showToast(error.message, "error");
      throw error;
    }
  });

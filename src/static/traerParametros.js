import { showToast } from "./toast.js";

document
  .getElementById("traerParametros")
  .addEventListener("click", async function (event) {
    event.preventDefault();

    try {
      const response = await fetch("http://127.0.0.1:8000/traer-parametros");
      const parametros = await response.json();

      document.getElementById("negocio").value = parametros.negocio;
      document.getElementById("mesInicio").value = parametros.mes_inicio;
      document.getElementById("mesCorte").value = parametros.mes_corte;
      document.getElementById("tipoAnalisis").value = parametros.tipo_analisis;
      document.getElementById("aproximarReaseguro").value =
        parametros.aproximar_reaseguro;
      document.getElementById("cuadreContableSinis").value =
        parametros.cuadre_contable_sinis;
      document.getElementById("addFraudeSoat").value =
        parametros.add_fraude_soat;
      document.getElementById("cuadreContablePrimas").value =
        parametros.cuadre_contable_primas;
      document.getElementById("nombrePlantilla").value =
        parametros.nombre_plantilla;

      if (!response.ok) throw new Error("Error al traer los parametros");
      showToast("Parametros traidos correctamente", "success");
    } catch (error) {
      showToast(error.message, "error");
    }
  });

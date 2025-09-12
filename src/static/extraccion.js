import { bindApiButton } from "./modosPlantilla.js";
import {
  actualizarCheckboxesCandidatos,
  handleRequest,
  appendFileIfExists,
} from "./apiUtils.js";
import { showToast } from "./toast.js";

bindApiButton(".extraccion", async (button) => {
  showToast("Corriendo query...");
  const endpoint = button.getAttribute("endpoint");

  const user = document.getElementById("user").value;
  const password = document.getElementById("password").value;

  const credenciales = {
    user,
    password,
  };

  const formData = new FormData();
  formData.append("credenciales", JSON.stringify(credenciales));

  appendFileIfExists(formData, "querySiniestros", "siniestros");
  appendFileIfExists(formData, "queryPrimas", "primas");
  appendFileIfExists(formData, "queryExpuestos", "expuestos");

  const data = await handleRequest(
    () =>
      fetch(`http://127.0.0.1:8000/${endpoint}`, {
        method: "POST",
        body: formData,
      }),
    "Error al correr el query",
  );

  const cantidad = button.getAttribute("cantidad");
  actualizarCheckboxesCandidatos(data.candidatos, cantidad);
});

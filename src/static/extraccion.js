import { bindApiButton } from "./modosPlantilla.js";
import { actualizarCheckboxesCandidatos, fetchForm } from "./apiUtils.js";
import { showToast } from "./toast.js";

bindApiButton(".extraccion", async (button) => {
  showToast("Corriendo query...");
  const endpoint = button.getAttribute("endpoint");
  const cantidad = button.getAttribute("cantidad");
  const data = await fetchForm(
    "credencialesTeradata",
    endpoint,
    "Error al correr el query",
  );
  actualizarCheckboxesCandidatos(data.candidatos, cantidad);
});

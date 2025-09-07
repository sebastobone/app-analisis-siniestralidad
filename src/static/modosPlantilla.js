import { showToast } from "./toast.js";
import { fetchForm, fetchSimple } from "./apiUtils.js";

export function bindApiButton(selector, handler) {
  document.querySelectorAll(selector).forEach((button) => {
    button.addEventListener("click", async (event) => {
      event.preventDefault();
      await handler(button);
    });
  });
}

bindApiButton(".modo", async (button) => {
  showToast("Ejecutando...");
  const endpoint = button.getAttribute("endpoint");
  await fetchForm("modosPlantilla", endpoint, "Error en la ejecucion");
});

bindApiButton(".apiButton", async (button) => {
  showToast("Ejecutando...");
  const endpoint = button.getAttribute("endpoint");
  await fetchSimple(endpoint, "Ocurrio un error");
});

document
  .getElementById("prepararPlantilla")
  .addEventListener("click", async (event) => {
    event.preventDefault();
    showToast("Preparando plantilla...");
    await fetchForm(
      "referenciasEntremesForm",
      "preparar-plantilla",
      "Error al preparar la plantilla",
    );
  });

document
  .getElementById("traerEntremes")
  .addEventListener("click", async (event) => {
    event.preventDefault();

    showToast("Preparando plantilla...");
    await fetchForm(
      "referenciasEntremesForm",
      "preparar-plantilla",
      "Error al preparar la plantilla",
    );

    showToast("Trayendo formulas de la hoja Entremes...");
    await fetchSimple(
      "traer-entremes",
      "Error al traer formulas de la hoja Entremes",
    );
  });

document
  .getElementById("traerGuardarTodo")
  .addEventListener("click", async (event) => {
    event.preventDefault();

    showToast("Preparando plantilla...");
    await fetchForm(
      "referenciasEntremesForm",
      "preparar-plantilla",
      "Error al preparar la plantilla",
    );

    showToast("Trayendo y guardando todo...");
    await fetchForm(
      "modosPlantilla",
      "traer-guardar-todo",
      "Error en la ejecucion",
    );
  });

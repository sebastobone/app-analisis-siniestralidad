import { showToast } from "./toast.js";

async function enviarParametros(formData) {
  try {
    const response = await fetch("http://127.0.0.1:8000/ingresar-parametros", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: formData.toString(),
    });

    if (!response.ok) throw new Error("Error al ingresar los parametros");

    showToast("Parametros ingresados correctamente", "success");
    return true;
  } catch (error) {
    showToast(error.message, "error");
    return false;
  }
}

async function generarDropdownPlantillas(tipoAnalisis) {
  try {
    let listaPlantillas;

    if (tipoAnalisis === "entremes") {
      listaPlantillas = ["completar_diagonal"];
    } else {
      listaPlantillas = ["frecuencia", "severidad", "plata"];
    }

    const dropdownPlantillas = document.getElementById("plantilla");
    dropdownPlantillas.innerHTML = "";

    listaPlantillas.forEach((plantilla) => {
      let option = document.createElement("option");
      option.value = plantilla;
      option.text = plantilla.charAt(0).toUpperCase() + plantilla.slice(1);

      if (plantilla === "plata") {
        option.selected = true;
      }

      dropdownPlantillas.appendChild(option);
    });

    showToast("Lista desplegable de plantillas actualizada", "success");
  } catch (error) {
    showToast(error.message, "error");
  }
}

document
  .getElementById("ingresarParametros")
  .addEventListener("submit", async function (event) {
    event.preventDefault();

    const tipoAnalisis = document.getElementById("tipoAnalisis").value;

    const formData = new URLSearchParams({
      negocio: document.getElementById("negocio").value,
      mes_inicio: document.getElementById("mesInicio").value,
      mes_corte: document.getElementById("mesCorte").value,
      tipo_analisis: tipoAnalisis,
      aproximar_reaseguro: document.getElementById("aproximarReaseguro").value,
      cuadre_contable_sinis: document.getElementById("cuadreContableSinis")
        .value,
      add_fraude_soat: document.getElementById("addFraudeSoat").value,
      cuadre_contable_primas: document.getElementById("cuadreContablePrimas")
        .value,
      nombre_plantilla: document.getElementById("nombrePlantilla").value,
    });

    const success = await enviarParametros(formData);
    if (success) await generarDropdownPlantillas(tipoAnalisis);
  });

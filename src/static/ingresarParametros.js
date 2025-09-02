import { showToast } from "./toast.js";
import { generarDropdownPlantillas } from "./dropdowns/generarPlantillas.js";

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
  } catch (error) {
    showToast(error.message, "error");
    throw error;
  }
}

async function mostrarFuncionalidades(tipoAnalisis, negocio) {
  if (tipoAnalisis === "entremes") {
    await generarReferenciasEntremes();
    document.getElementById("referenciasEntremes").style.display = "block";
    document.getElementById("botonesEntremes").style.display = "block";
    document.getElementById("botonesGraficaFactores").style.display = "none";
  } else {
    document.getElementById("referenciasEntremes").style.display = "none";
    document.getElementById("botonesEntremes").style.display = "none";
    document.getElementById("botonesGraficaFactores").style.display = "block";
  }

  if (negocio === "demo") {
    await fetch("http://127.0.0.1:8000/generar-mocks", {
      method: "POST",
    });
    document.getElementById("extraccion").style.display = "none";
    document.getElementById("cargaManual").style.display = "none";
    document.getElementById("controles").style.display = "none";
  } else {
    document.getElementById("extraccion").style.display = "block";
    document.getElementById("cargaManual").style.display = "block";
    document.getElementById("controles").style.display = "block";
  }

  document.getElementById("seccionPlantilla").style.display = "block";
  document.getElementById("resultados").style.display = "block";
}

async function generarReferenciasEntremes() {
  try {
    const response = await fetch(
      "http://127.0.0.1:8000/obtener-analisis-anteriores",
    );

    if (!response.ok) throw new Error("Error al obtener resultados anteriores");
    showToast("Resultados anteriores obtenidos exitosamente", "success");

    const data = await response.json();

    const dropdownReferenciaActuarial = document.getElementById(
      "referenciaActuarial",
    );
    dropdownReferenciaActuarial.innerHTML = "";
    const dropdownReferenciaContable =
      document.getElementById("referenciaContable");
    dropdownReferenciaContable.innerHTML = "";

    data.analisis_anteriores.forEach((analisis) => {
      let option = document.createElement("option");
      option.value = analisis;
      option.text = analisis.charAt(0).toUpperCase() + analisis.slice(1);
      dropdownReferenciaActuarial.appendChild(option);
    });

    data.analisis_anteriores.forEach((analisis) => {
      let option = document.createElement("option");
      option.value = analisis;
      option.text = analisis.charAt(0).toUpperCase() + analisis.slice(1);
      dropdownReferenciaContable.appendChild(option);
    });
  } catch (error) {
    showToast(error.message, "error");
    throw error;
  }
}

document
  .getElementById("guardarParametros")
  .addEventListener("click", async function (event) {
    event.preventDefault();

    const negocio = document.getElementById("negocio").value;
    const tipoAnalisis = document.getElementById("tipoAnalisis").value;

    const formData = new URLSearchParams({
      negocio: document.getElementById("negocio").value,
      mes_inicio: document.getElementById("mesInicio").value,
      mes_corte: document.getElementById("mesCorte").value,
      tipo_analisis: tipoAnalisis,
      nombre_plantilla: document.getElementById("nombrePlantilla").value,
    });

    await enviarParametros(formData);
    await generarDropdownPlantillas(tipoAnalisis);
    await mostrarFuncionalidades(tipoAnalisis, negocio);
  });

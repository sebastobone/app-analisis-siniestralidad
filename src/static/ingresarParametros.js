import { showToast } from "./toast.js";
import { generarDropdownPlantillas } from "./dropdowns/generarPlantillas.js";
import { generarDropdownAperturas } from "./dropdowns/generarAperturas.js";

async function enviarParametros(queryParams, formData) {
  const response = await fetch(
    "http://127.0.0.1:8000/ingresar-parametros?" + queryParams,
    {
      method: "POST",
      body: formData,
    },
  );
  if (!response.ok) throw new Error("Error al ingresar los parametros");
  showToast("Parametros ingresados correctamente", "success");
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
}

document
  .getElementById("guardarParametros")
  .addEventListener("click", async function (event) {
    event.preventDefault();

    try {
      var negocio = document.getElementById("negocio").value;
      var tipoAnalisis = document.getElementById("tipo_analisis").value;

      var parametrosForm = document.getElementById("parametrosForm");
      var queryParams = new URLSearchParams(
        new FormData(parametrosForm),
      ).toString();

      var formData = new FormData();
      var archivoSegmentacion = document.getElementById(
        "archivoSegmentacion",
      ).files;
      if (archivoSegmentacion.length > 0) {
        formData.append("archivo_segmentacion", archivoSegmentacion[0]);
      }

      await enviarParametros(queryParams, formData);
      await generarDropdownAperturas(tipoAnalisis);
      await generarDropdownPlantillas(tipoAnalisis);
      await mostrarFuncionalidades(tipoAnalisis, negocio);
    } catch (error) {
      showToast(error.message, "error");
      throw error;
    }
  });

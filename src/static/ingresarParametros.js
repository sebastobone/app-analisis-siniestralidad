import { showToast } from "./toast.js";
import { generarDropdownPlantillas } from "./dropdowns/generarPlantillas.js";
import { generarDropdownAperturas } from "./dropdowns/generarAperturas.js";
import { generarReferenciasEntremes } from "./dropdowns/generarReferenciasEntremes.js";
import { actualizarCheckboxesCandidatos } from "./apiUtils.js";

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
  const data = await response.json();
  return data;
}

async function mostrarFuncionalidades(tipoAnalisis, negocio) {
  if (tipoAnalisis === "entremes") {
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

function crearCargaAfos(afos) {
  const container = document.getElementById("cargarAfos");

  container.innerHTML = "";

  afos.forEach((afo) => {
    const divAfo = document.createElement("div");
    divAfo.classList.add("input-group");

    const label = document.createElement("label");
    label.classList.add("custom-file-upload");
    const text = document.createTextNode(`Cargar AFO de ${afo}:`);
    label.appendChild(text);

    divAfo.appendChild(label);

    const fileInput = document.createElement("input");
    fileInput.type = "file";
    fileInput.id = `afo${afo}`;
    fileInput.name = `afo${afo}`;
    fileInput.accept = ".xlsx";

    divAfo.appendChild(fileInput);

    container.appendChild(divAfo);
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

      const data = await enviarParametros(queryParams, formData);
      await generarDropdownAperturas(data.aperturas);
      await generarDropdownPlantillas(tipoAnalisis);

      crearCargaAfos(data.afos);

      actualizarCheckboxesCandidatos(data.candidatos_siniestros, "siniestros");
      actualizarCheckboxesCandidatos(data.candidatos_primas, "primas");

      if (tipoAnalisis === "entremes") {
        await generarReferenciasEntremes(data.tipos_analisis_mes_anterior);
      }

      await mostrarFuncionalidades(tipoAnalisis, negocio);
    } catch (error) {
      showToast(error.message, "error");
      throw error;
    }
  });

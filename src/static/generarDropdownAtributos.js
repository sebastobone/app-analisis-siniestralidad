document.addEventListener("DOMContentLoaded", function () {
  const dropdownAtributos = document.getElementById("atributo");
  const dropdownPlantillas = document.getElementById("plantilla");

  const opciones = {
    frecuencia: ["bruto"],
    severidad: ["bruto", "retenido"],
    plata: ["bruto", "retenido"],
    completar_diagonal: ["bruto", "retenido"],
  };

  dropdownPlantillas.addEventListener("change", function () {
    const plantillaSeleccionada = dropdownPlantillas.value;

    dropdownAtributos.innerHTML = "";

    opciones[plantillaSeleccionada].forEach((atributo) => {
      const option = document.createElement("option");
      option.value = atributo;
      option.textContent = atributo.charAt(0).toUpperCase() + atributo.slice(1);
      dropdownAtributos.appendChild(option);
    });
  });
});

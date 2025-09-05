export async function generarDropdownPlantillas(tipoAnalisis) {
  try {
    const opciones = {
      entremes: ["completar_diagonal", "frecuencia"],
      triangulos: ["frecuencia", "severidad", "plata"],
    };

    const dropdownPlantillas = document.getElementById("plantilla");
    dropdownPlantillas.innerHTML = "";

    opciones[tipoAnalisis].forEach((plantilla) => {
      let option = document.createElement("option");
      option.value = plantilla;
      option.text = plantilla.charAt(0).toUpperCase() + plantilla.slice(1);

      if (plantilla === "plata") {
        option.selected = true;
      }

      dropdownPlantillas.appendChild(option);
    });
  } catch (error) {
    showToast(error.message, "error");
    throw error;
  }
}

export async function generarDropdownPlantillas(tipoAnalisis) {
  try {
    let listaPlantillas;

    if (tipoAnalisis === "entremes") {
      listaPlantillas = ["completar_diagonal", "frecuencia"];
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
  } catch (error) {
    showToast(error.message, "error");
    throw error;
  }
}

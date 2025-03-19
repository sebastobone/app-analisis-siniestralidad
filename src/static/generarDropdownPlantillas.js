document
  .getElementById("ingresarParametros")
  .addEventListener("submit", async function () {
    try {
      const response = await fetch(
        "http://127.0.0.1:8000/generar-dropdown-plantillas",
      );
      const data = await response.json();
      const dropdownPlantillas = document.getElementById("plantilla");

      dropdownPlantillas.innerHTML = "";

      data.plantillas.forEach((plantilla) => {
        let option = document.createElement("option");
        option.value = plantilla;
        option.text = plantilla.charAt(0).toUpperCase() + plantilla.slice(1);
        dropdownPlantillas.appendChild(option);
      });
    } catch (error) {
      console.error("Error al obtener las plantillas:", error);
    }
  });

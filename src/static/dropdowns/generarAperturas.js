import { showToast } from "../toast.js";

document
  .getElementById("generarAperturas")
  .addEventListener("click", async function () {
    try {
      const response = await fetch("http://127.0.0.1:8000/generar-aperturas");

      if (!response.ok) {
        throw new Error("Error al obtener las aperturas");
      }

      const data = await response.json();
      const dropdownAperturas = document.getElementById("apertura");

      dropdownAperturas.innerHTML = "";

      data.aperturas.forEach((apertura) => {
        let option = document.createElement("option");
        option.value = apertura;
        option.text = apertura;
        dropdownAperturas.appendChild(option);
      });

      showToast("Aperturas generadas exitosamente", "success");
    } catch (error) {
      showToast(error.message, "error");
      throw error;
    }
  });

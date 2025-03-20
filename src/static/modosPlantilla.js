import { showToast } from "./toast.js";

document
  .getElementById("modosPlantilla")
  .addEventListener("submit", async function (event) {
    event.preventDefault();

    showToast("Ejecutando...");

    const formData = new URLSearchParams({
      apertura: document.getElementById("apertura").value,
      atributo: document.getElementById("atributo").value,
      plantilla: document.getElementById("plantilla").value,
      modo: document.getElementById("modo").value,
    });

    const response = await fetch("http://127.0.0.1:8000/modos-plantilla", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: formData.toString(),
    });

    if (response.ok) {
      showToast("Ejecucion exitosa", "success");
    } else {
      showToast("Error en la ejecucion", "error");
    }
  });

import { showToast } from "./toast.js";

document.querySelectorAll(".modo").forEach((button) => {
  button.addEventListener("click", async function (event) {
    event.preventDefault();

    showToast("Ejecutando...");

    const formData = new URLSearchParams({
      apertura: document.getElementById("apertura").value,
      atributo: document.getElementById("atributo").value,
      plantilla: document.getElementById("plantilla").value,
    });

    const endpoint = button.getAttribute("endpoint");
    const response = await fetch(`http://127.0.0.1:8000/${endpoint}`, {
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
});

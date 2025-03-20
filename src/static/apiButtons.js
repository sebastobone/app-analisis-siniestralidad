import { showToast } from "./toast.js";

document.querySelectorAll(".apiButton").forEach((button) => {
  button.addEventListener("click", async function (event) {
    event.preventDefault();

    showToast("Ejecutando...");

    const endpoint = button.getAttribute("endpoint");
    const response = await fetch(`http://127.0.0.1:8000/${endpoint}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
    });

    if (response.ok) {
      showToast("Ejecucion exitosa", "success");
    } else {
      showToast("Error en la ejecucion", "error");
    }
  });
});

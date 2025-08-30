import { showToast } from "./toast.js";

document.querySelectorAll(".extraccion").forEach((button) => {
  button.addEventListener("click", async function (event) {
    event.preventDefault();

    showToast("Ejecutando...");

    const formData = new URLSearchParams({
      user: document.getElementById("user").value,
      password: document.getElementById("password").value,
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

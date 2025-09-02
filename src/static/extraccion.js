import { showToast } from "./toast.js";

document.querySelectorAll(".extraccion").forEach((button) => {
  button.addEventListener("click", async function (event) {
    event.preventDefault();

    try {
      showToast("Corriendo query...");

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

      if (!response.ok) throw new Error("Error al correr el query");
      const data = await response.json();
      showToast(data.message, "success");
    } catch (error) {
      showToast(error.message, "error");
    }
  });
});

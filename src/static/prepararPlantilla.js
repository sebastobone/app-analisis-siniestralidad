import { showToast } from "./toast.js";

document
  .getElementById("prepararPlantilla")
  .addEventListener("click", async function (event) {
    event.preventDefault();

    try {
      showToast("Preparando plantilla...");

      const formData = new URLSearchParams({
        referencia_actuarial: document.getElementById("referenciaActuarial")
          .value,
        referencia_contable:
          document.getElementById("referenciaContable").value,
      });

      const response = await fetch(`http://127.0.0.1:8000/preparar-plantilla`, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: formData.toString(),
      });

      if (!response.ok) throw new Error("Error al preparar la plantilla");

      const data = await response.json();
      showToast(data.message, "success");
    } catch (error) {
      showToast(error.message, "error");
      throw error;
    }
  });

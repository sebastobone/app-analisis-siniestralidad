import { showToast } from "./toast.js";

document
  .getElementById("prepararPlantilla")
  .addEventListener("click", async function (event) {
    event.preventDefault();

    showToast("Ejecutando...");

    const formData = new URLSearchParams({
      referencia_actuarial: document.getElementById("referenciaActuarial")
        .value,
      referencia_contable: document.getElementById("referenciaContable").value,
    });

    const response = await fetch(`http://127.0.0.1:8000/preparar-plantilla`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: formData.toString(),
    });

    showToast(
      response.ok ? "Ejecucion exitosa" : "Error en la ejecucion",
      response.ok ? "success" : "error",
    );
  });

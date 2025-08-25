import { showToast } from "./toast.js";

document
  .getElementById("traerEntremes")
  .addEventListener("click", async function (event) {
    event.preventDefault();

    showToast("Preparando plantilla...");

    const formDataPreparar = new URLSearchParams({
      referencia_actuarial: document.getElementById("referenciaActuarial")
        .value,
      referencia_contable: document.getElementById("referenciaContable").value,
    });

    const responsePreparar = await fetch(
      `http://127.0.0.1:8000/preparar-plantilla`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: formDataPreparar.toString(),
      },
    );

    showToast(
      responsePreparar.ok
        ? "Plantilla preparada"
        : "Error al preparar plantilla",
      responsePreparar.ok ? "success" : "error",
    );

    showToast("Ejecutando...");

    const response = await fetch(`http://127.0.0.1:8000/traer-entremes`, {
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

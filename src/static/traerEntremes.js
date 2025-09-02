import { showToast } from "./toast.js";

document
  .getElementById("traerEntremes")
  .addEventListener("click", async function (event) {
    event.preventDefault();

    try {
      showToast("Preparando plantilla...");

      const formDataPreparar = new URLSearchParams({
        referencia_actuarial: document.getElementById("referenciaActuarial")
          .value,
        referencia_contable:
          document.getElementById("referenciaContable").value,
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

      if (!responsePreparar.ok) throw new Error("Error al preparar plantilla");

      showToast("Trayendo formulas de la hoja Entremes...");

      const response = await fetch(`http://127.0.0.1:8000/traer-entremes`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
      });

      if (!response.ok)
        throw new Error("Error al traer formulas de la hoja Entremes");

      const data = await response.json();
      showToast(data.message, "success");
    } catch (error) {
      showToast(error.message, "error");
      throw error;
    }
  });

import { showToast } from "./toast.js";

document
  .getElementById("traerGuardarTodo")
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

      showToast("Plantilla preparada exitosamente", "success");

      showToast("Trayendo y guardando todo...");

      const formData = new URLSearchParams({
        apertura: document.getElementById("apertura").value,
        atributo: document.getElementById("atributo").value,
        plantilla: document.getElementById("plantilla").value,
      });

      const response = await fetch(`http://127.0.0.1:8000/traer-guardar-todo`, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: formData.toString(),
      });

      if (!response.ok) throw new Error("Error al traer y guardar todo");
      const data = await response.json();
      showToast(data.message, "success");
    } catch (error) {
      showToast(error.message, "error");
    }
  });

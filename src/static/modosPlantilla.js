document
  .getElementById("modosPlantilla")
  .addEventListener("submit", async function (event) {
    event.preventDefault();

    const formData = new URLSearchParams({
      apertura: document.getElementById("apertura").value,
      atributo: document.getElementById("atributo").value,
      metodologia: document.getElementById("metodologia").value,
      plantilla: document.getElementById("plantilla").value,
      modo: document.getElementById("modo").value,
    });

    await fetch("http://127.0.0.1:8000/modos-plantilla", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: formData.toString(),
    });
  });

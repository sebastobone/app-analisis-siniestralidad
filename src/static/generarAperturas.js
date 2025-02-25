document
  .getElementById("generarAperturas")
  .addEventListener("click", async function () {
    try {
      const response = await fetch("http://127.0.0.1:8000/generar-aperturas");
      const data = await response.json();
      const dropdownAperturas = document.getElementById("apertura");
      const tableBody = document.querySelector("#tablaPeriodicidades tbody");
      const periodicidades = ["Trimestral", "Mensual", "Semestral", "Anual"];

      dropdownAperturas.innerHTML = "";

      data.aperturas.forEach((apertura) => {
        let option = document.createElement("option");
        option.value = apertura;
        option.text = apertura;
        dropdownAperturas.appendChild(option);

        let row = tableBody.insertRow();
        row.insertCell(0).textContent = apertura;
        let selectCell = row.insertCell(1);
        let select = document.createElement("select");

        for (let periodicidad of periodicidades) {
          let opt = document.createElement("option");
          opt.value = periodicidad;
          opt.textContent = periodicidad;
          select.appendChild(opt);
        }

        selectCell.appendChild(select);
      });
    } catch (error) {
      console.error("Error al obtener las aperturas:", error);
    }
  });

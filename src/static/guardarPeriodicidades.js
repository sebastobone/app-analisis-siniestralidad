const numRows = 5;
const tableBody = document.getElementById("tablaPeriodicidades");
const periodicidades = document.getElementById("periodicidades");

const response = await fetch("http://127.0.0.1:8000/generar-aperturas");
const data = await response.json();

data.aperturas.forEach((apertura) => {
  let row = tableBody.insertRow();
  row.insertCell(0).textContent = apertura;
  let selectCell = row.insertCell(1);
  let select = document.createElement("select");

  for (let option of periodicidades.options) {
    let opt = document.createElement("option");
    opt.value = option.value;
    opt.textContent = option.textContent;
    select.appendChild(opt);
  }

  selectCell.appendChild(select);
});

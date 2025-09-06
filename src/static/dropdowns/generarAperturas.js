export async function generarDropdownAperturas(aperturas) {
  const dropdownAperturas = document.getElementById("apertura");

  dropdownAperturas.innerHTML = "";

  aperturas.forEach((apertura) => {
    let option = document.createElement("option");
    option.value = apertura;
    option.text = apertura;
    dropdownAperturas.appendChild(option);
  });
}

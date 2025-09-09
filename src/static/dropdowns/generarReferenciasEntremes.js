export async function generarReferenciasEntremes(tipos_analisis_mes_anterior) {
  const dropdownReferenciaActuarial = document.getElementById(
    "referenciaActuarial",
  );
  dropdownReferenciaActuarial.innerHTML = "";
  const dropdownReferenciaContable =
    document.getElementById("referenciaContable");
  dropdownReferenciaContable.innerHTML = "";

  tipos_analisis_mes_anterior.forEach((tipo_analisis) => {
    let option = document.createElement("option");
    option.value = tipo_analisis;
    option.text =
      tipo_analisis.charAt(0).toUpperCase() + tipo_analisis.slice(1);
    dropdownReferenciaActuarial.appendChild(option);
  });

  tipos_analisis_mes_anterior.forEach((tipo_analisis) => {
    let option = document.createElement("option");
    option.value = tipo_analisis;
    option.text =
      tipo_analisis.charAt(0).toUpperCase() + tipo_analisis.slice(1);
    dropdownReferenciaContable.appendChild(option);
  });
}

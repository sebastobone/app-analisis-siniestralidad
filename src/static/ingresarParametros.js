document
  .getElementById("ingresarParametros")
  .addEventListener("submit", async function (event) {
    event.preventDefault();

    const formData = new URLSearchParams({
      negocio: document.getElementById("negocio").value,
      mes_inicio: document.getElementById("mesInicio").value,
      mes_corte: document.getElementById("mesCorte").value,
      tipo_analisis: document.getElementById("tipoAnalisis").value,
      aproximar_reaseguro: document.getElementById("aproximarReaseguro").value,
      cuadre_contable_sinis: document.getElementById("cuadreContableSinis")
        .value,
      add_fraude_soat: document.getElementById("addFraudeSoat").value,
      cuadre_contable_primas: document.getElementById("cuadreContablePrimas")
        .value,
      nombre_plantilla: document.getElementById("nombrePlantilla").value,
    });

    await fetch("http://127.0.0.1:8000/ingresar-parametros", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: formData.toString(),
    });
  });

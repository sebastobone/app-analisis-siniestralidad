document
  .getElementById("ingresarParametros")
  .addEventListener("submit", async function (event) {
    event.preventDefault();

    const formData = new URLSearchParams({
      negocio: document.getElementById("negocio").value,
      mes_inicio: document.getElementById("mes_inicio").value,
      mes_corte: document.getElementById("mes_corte").value,
      tipo_analisis: document.getElementById("tipo_analisis").value,
      aproximar_reaseguro: document.getElementById("aproximar_reaseguro").value,
      cuadre_contable_sinis: document.getElementById("cuadre_contable_sinis")
        .value,
      add_fraude_soat: document.getElementById("add_fraude_soat").value,
      cuadre_contable_primas: document.getElementById("cuadre_contable_primas")
        .value,
      nombre_plantilla: document.getElementById("nombre_plantilla").value,
    });

    await fetch("http://127.0.0.1:8000/ingresar-parametros", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: formData.toString(),
    });
  });

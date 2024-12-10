import polars as pl
import metodos_controles as ctrl

# Descomentar la siguiente fila si se necesita volver a correr la info, permite sobrescribir los archivos de controles
ctrl.set_permissions("data/controles_informacion", "write")


def generar_controles(file: str) -> None:
    if file == "siniestros":
        qtys = ["pago_bruto", "aviso_bruto", "pago_retenido", "aviso_retenido"]
        group_cols = ["apertura_reservas", "mes_ocurr", "mes_mov"]

    elif file == "primas":
        qtys = [
            "prima_bruta",
            "prima_retenida",
            "prima_bruta_devengada",
            "prima_retenida_devengada",
        ]
        group_cols = ["apertura_reservas", "mes_mov"]

    elif file == "expuestos":
        qtys = ["expuestos"]
        group_cols = ["apertura_reservas", "mes_mov"]

    df = pl.scan_csv(
        f"data/raw/{file}.csv",
        separator="\t",
        try_parse_dates=True,
        schema_overrides={"codigo_ramo_op": pl.String, "codigo_op": pl.String},
    )

    valid_pre_cuadre = ctrl.controles_informacion(
        df,
        file,
        group_cols,
        qtys,
        estado_cuadre="pre_cuadre_contable",
    )

    if file in ("siniestros", "primas"):
        df.collect().write_csv(f"data/raw/{file}_pre_cuadre.csv", separator="\t")
        df_cuadrado = ctrl.cuadre_contable(df, file, valid_pre_cuadre)
        _ = ctrl.controles_informacion(
            df_cuadrado,
            file,
            group_cols,
            qtys,
            estado_cuadre="post_cuadre_contable",
        )


generar_controles("siniestros")
generar_controles("primas")
generar_controles("expuestos")

ctrl.evidencias_parametros()
ctrl.set_permissions("data/controles_informacion", "read")

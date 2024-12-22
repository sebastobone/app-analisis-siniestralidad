import xlwings as xw
from src.metodos_plantilla import base_plantillas
from src.metodos_plantilla import tablas_resumen
from src.metodos_plantilla import guardar_traer
import polars as pl
import src.constantes as ct
import time


def check_plantilla(plantilla: str | None) -> None:
    if plantilla is None:
        raise Exception("Especifique una plantilla.")
    elif plantilla not in ("frec", "seve", "plata", "entremes"):
        raise Exception(
            """Plantilla no encontrada. Opciones disponibles: 
            frec, seve, plata, entremes"""
        )


def modos(wb: xw.Book, modo: str, plantilla: str | None = None) -> None:
    if modo == "preparar":
        preparar_plantilla(wb)
    elif modo == "generar":
        check_plantilla(plantilla)
        generar_plantilla(
            wb,
            plantilla,
            wb.sheets["Aperturas"]["A2"].value,
            wb.sheets["Atributos"]["A2"].value,
        )
    elif modo in ("guardar", "traer"):
        check_plantilla(plantilla)
        guardar_traer_fn(
            wb,
            modo,
            plantilla,
            wb.sheets["Aperturas"]["A2"].value,
            wb.sheets["Atributos"]["A2"].value,
        )
    elif modo == "almacenar":
        almacenar_analisis(wb)
    elif modo == "guardar_todo":
        check_plantilla(plantilla)
        traer_guardar_todo(wb, plantilla)
    elif modo == "traer_guardar_todo":
        check_plantilla(plantilla)
        traer_guardar_todo(wb, plantilla, traer=True)
    else:
        raise Exception(
            """
            Modo no encontrado. Opciones disponibles: 
            preparar, generar, guardar, traer, almacenar
            """
        )


def preparar_plantilla(wb: xw.Book) -> None:
    mes_corte = int(ct.END_DATE.strftime("%Y%m"))

    wb.sheets["Modo"]["A4"].value = "Mes corte"
    wb.sheets["Modo"]["B4"].value = mes_corte
    wb.sheets["Modo"]["A5"].value = "Mes anterior"
    wb.sheets["Modo"]["B5"].value = (
        mes_corte - 1 if mes_corte % 100 != 1 else ((mes_corte // 100) - 1) * 100 + 12
    )

    if ct.TIPO_ANALISIS == "Triangulos":
        wb.sheets["Plantilla_Entremes"].visible = False
        wb.sheets["Plantilla_Frec"].visible = True
        wb.sheets["Plantilla_Seve"].visible = True
        wb.sheets["Plantilla_Plata"].visible = True
    elif ct.TIPO_ANALISIS == "Entremes":
        wb.sheets["Plantilla_Entremes"].visible = True
        wb.sheets["Plantilla_Frec"].visible = False
        wb.sheets["Plantilla_Seve"].visible = False
        wb.sheets["Plantilla_Plata"].visible = False

    for sheet in ["Aux_Totales", "Aux_Expuestos", "Aux_Primas", "Atipicos"]:
        wb.sheets[sheet].clear_contents()

    s = time.time()

    periodicidades = (
        wb.sheets["Aperturas"].tables["periodicidades"].data_body_range.value
    )

    diagonales, expuestos, primas, atipicos = tablas_resumen.tablas_resumen(
        ct.path_plantilla(wb), periodicidades, ct.TIPO_ANALISIS
    )

    wb.sheets["Aux_Totales"]["A1"].options(index=False).value = diagonales.to_pandas()

    wb.macro("formulas_aux_totales")(diagonales.shape[0])

    wb.sheets["Aux_Expuestos"]["A1"].options(index=False).value = expuestos.to_pandas()
    wb.sheets["Aux_Primas"]["A1"].options(index=False).value = primas.to_pandas()
    wb.sheets["Atipicos"]["A1"].options(index=False).value = atipicos.to_pandas()

    wb.sheets["Modo"]["A2"].value = time.time() - s


def generar_plantilla(
    wb: xw.Book, plantilla: str, apertura: str, atributo: str
) -> None:
    s = time.time()

    plantilla = f"Plantilla_{plantilla.capitalize()}"

    atributo = atributo if plantilla != "Plantilla_Frec" else "bruto"
    cantidades = (
        ["pago", "incurrido"]
        if plantilla != "Plantilla_Frec"
        else ["conteo_pago", "conteo_incurrido"]
    )

    periodicidades = (
        wb.sheets["Aperturas"].tables["periodicidades"].data_body_range.value
    )

    df = base_plantillas.base_plantillas(
        ct.path_plantilla(wb), apertura, atributo, periodicidades, cantidades
    )

    NUM_OCURRENCIAS = df.shape[0]
    NUM_ALTURAS = df.shape[1] // len(cantidades)
    MES_DEL_PERIODO = ct.mes_del_periodo(ct.END_DATE, NUM_OCURRENCIAS, NUM_ALTURAS)

    wb.sheets["Modo"]["B6"].value = MES_DEL_PERIODO

    wb.macro("limpiar_plantilla")(plantilla)

    wb.sheets[plantilla]["F7"].value = df

    wb.macro(f"generar_{plantilla}")(
        NUM_OCURRENCIAS,
        NUM_ALTURAS,
        ct.HEADER_TRIANGULOS,
        ct.SEP_TRIANGULOS,
        ct.FILA_INI_PLANTILLAS,
        ct.COL_OCURRS_PLANTILLAS,
        atributo,
        MES_DEL_PERIODO,
    )

    wb.sheets["Modo"]["A2"].value = time.time() - s


def guardar_traer_fn(
    wb: xw.Book, modo: str, plantilla: str, apertura: str, atributo: str
) -> None:
    s = time.time()

    plantilla = f"Plantilla_{plantilla.capitalize()}"
    atributo = atributo if plantilla != "Plantilla_Frec" else "bruto"

    NUM_OCURRENCIAS = ct.num_ocurrencias(wb.sheets[plantilla])
    NUM_ALTURAS = ct.num_alturas(wb.sheets[plantilla])

    if plantilla == "Plantilla_Entremes":
        MES_DEL_PERIODO = ct.mes_del_periodo(ct.END_DATE, NUM_OCURRENCIAS, NUM_ALTURAS)
    else:
        MES_DEL_PERIODO = 1

    if modo == "guardar":
        guardar_traer.guardar(
            wb.sheets[plantilla],
            apertura,
            atributo,
            NUM_OCURRENCIAS,
            NUM_ALTURAS,
            MES_DEL_PERIODO,
        )

        cols_ultimate = {
            "Plantilla_Frec": "frec_ultimate",
            "Plantilla_Seve": f"seve_ultimate_{atributo}",
            "Plantilla_Plata": f"plata_ultimate_{atributo}",
            "Plantilla_Entremes": f"plata_ultimate_{atributo}",
        }

        wb.macro("guardar_ultimate")(
            plantilla,
            NUM_OCURRENCIAS,
            MES_DEL_PERIODO,
            cols_ultimate[plantilla],
            atributo,
        )

        wb.sheets["Modo"]["A2"].value = time.time() - s

    elif modo == "traer":
        guardar_traer.traer(
            wb.sheets[plantilla],
            apertura,
            atributo,
            NUM_OCURRENCIAS,
            NUM_ALTURAS,
            MES_DEL_PERIODO,
        )

        wb.sheets["Modo"]["A2"].value = time.time() - s


def almacenar_analisis(wb: xw.Book) -> None:
    s = time.time()

    df_tips = ct.sheet_to_dataframe(wb, "Aux_Totales").with_columns(atipico=0)
    df_atips = ct.sheet_to_dataframe(wb, "Atipicos").with_columns(atipico=1)
    df_new = pl.concat([df_tips, df_atips]).with_columns(MES_CORTE=ct.MES_CORTE)

    wb_res = xw.Book(f"{ct.path_plantilla(wb)}/resultados.xlsx")
    df_hist = ct.sheet_to_dataframe(wb_res, "BD", df_new.collect_schema())

    # Quitar versiones anteriores del analisis del mes actual para la apertura-reserva
    info_eliminar = df_new.select(["apertura_reservas", "ct.MES_CORTE"]).with_columns(
        eliminar=1
    )

    df_hist = (
        df_hist.join(
            info_eliminar, on=["apertura_reservas", "ct.MES_CORTE"], how="left"
        )
        .filter(pl.col("eliminar").is_null())
        .drop("eliminar")
    )

    df = pl.concat([df_hist, df_new])

    wb_res.sheets["BD"].clear_contents()
    # Por si falla, que no borre los nombres de las columnas
    wb_res.sheets["BD"]["A1"].value = df.collect_schema().names()
    wb_res.sheets["BD"]["A1"].options(index=False).value = df.collect().to_pandas()

    wb.sheets["Modo"]["A2"].value = time.time() - s


def traer_guardar_todo(wb: xw.Book, plantilla: str, traer: bool = False):
    s = time.time()

    aperturas = pl.read_csv("data/processed/aperturas.csv", separator="\t").get_column(
        "apertura_reservas"
    )
    atributos = ["bruto", "retenido"] if plantilla != "Plantilla_Frec" else ["bruto"]

    for apertura in aperturas:
        for atributo in atributos:
            generar_plantilla(wb, plantilla, apertura, atributo)
            if traer:
                guardar_traer_fn(wb, "traer", plantilla, apertura, atributo)
            guardar_traer_fn(wb, "guardar", plantilla, apertura, atributo)

    wb.sheets["Modo"]["A2"].value = time.time() - s


def abrir_plantilla(plantilla_path: str) -> xw.Book:
    xw.Book(plantilla_path).set_mock_caller()
    wb = xw.Book.caller()

    wb.macro("eliminar_modulos")
    wb.macro("crear_modulos")

    return wb

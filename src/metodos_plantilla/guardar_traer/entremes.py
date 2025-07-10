import polars as pl
import xlwings as xw
from src import utils


def obtener_rango_formulado_entremes(wb: xw.Book) -> xw.Range:
    tabla_entremes = utils.sheet_to_dataframe(wb, "Entremes")

    columna_inicial = (
        tabla_entremes.collect_schema()
        .names()
        .index("porcentaje_desarrollo_pago_bruto")
    )
    columna_final = tabla_entremes.shape[1]
    numero_filas = tabla_entremes.shape[0]

    hoja: xw.Sheet = wb.sheets["Entremes"]
    return hoja.range(
        hoja.cells(1, columna_inicial + 1), hoja.cells(numero_filas + 1, columna_final)
    )


def guardar_entremes(wb: xw.Book) -> None:
    rango_formulado_entremes = obtener_rango_formulado_entremes(wb)
    pl.DataFrame(rango_formulado_entremes.formula).transpose().write_parquet(
        f"data/db/{wb.name}_Entremes.parquet"
    )


def traer_entremes(wb: xw.Book) -> None:
    rango_formulado_entremes = obtener_rango_formulado_entremes(wb)
    try:
        rango_formulado_entremes.formula = pl.read_parquet(
            f"data/db/{wb.name}_Entremes.parquet"
        ).rows()
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            utils.limpiar_espacios_log(
                """
                No se encontraron formulas para el entremes.
                Para traer un analisis, primero tiene que haberse guardado.
                """
            )
        ) from exc

import xlwings as xw

from src import constantes as ct
from src import utils
from src.models import ModosPlantilla


def ajustar_grafica_factores(wb: xw.Book, modos: ModosPlantilla):
    hoja_plantilla = modos.plantilla.capitalize()

    dimensiones_triangulo = utils.obtener_dimensiones_triangulo(
        wb.sheets[hoja_plantilla]
    )

    wb.macro("AjustarGraficaFactores")(
        hoja_plantilla, dimensiones_triangulo.height, ct.SEP_TRIANGULOS
    )

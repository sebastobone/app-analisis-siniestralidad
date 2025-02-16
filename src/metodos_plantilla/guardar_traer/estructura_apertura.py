import xlwings as xw
from src import utils
from src.models import EstructuraApertura, RangeDimension


def obtener_estructura_apertura(
    wb: xw.Book, plantilla_name: str, mes_corte: int
) -> EstructuraApertura:
    apertura = str(wb.sheets[plantilla_name]["C2"].value)
    atributo = str(wb.sheets[plantilla_name]["C3"].value).lower()

    num_ocurrencias = utils.num_ocurrencias(wb.sheets[plantilla_name])
    num_alturas = utils.num_alturas(wb.sheets[plantilla_name])

    if plantilla_name == "Plantilla_Entremes":
        mes_del_periodo = utils.mes_del_periodo(
            utils.yyyymm_to_date(mes_corte), num_ocurrencias, num_alturas
        )
    else:
        mes_del_periodo = 1

    dimensiones_triangulo = RangeDimension(height=num_ocurrencias, width=num_alturas)

    return EstructuraApertura(
        apertura=apertura,
        atributo=atributo,
        dimensiones_triangulo=dimensiones_triangulo,
        mes_del_periodo=mes_del_periodo,
    )

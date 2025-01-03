from src.extraccion.tera_connect import correr_query
from src.models import Parametros

correr_query(
    "data/optimizaciones/siniestros_autonomia_opt.sql",
    "data/optimizaciones/siniestros_autonomia_opt",
    "txt",
    Parametros(
        negocio="autonomia",
        mes_inicio=201401,
        mes_corte=202410,
        aproximar_reaseguro=False,
        cuadre_contable_sinis=False,
        add_fraude_soat=False,
        cuadre_contable_primas=False,
        nombre_plantilla="plantilla_test_autonomia",
    ),
)

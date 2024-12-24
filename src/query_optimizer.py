from src.extraccion.tera_connect import correr_query

correr_query(
    "data/optimizaciones/siniestros_autonomia_opt.sql",
    "data/optimizaciones/siniestros_autonomia_opt",
    "txt",
    "autonomia",
    201401,
    202410,
    0,
)

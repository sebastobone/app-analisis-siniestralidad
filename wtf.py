import xlwings as xw
from src import plantilla as plant

wb = xw.Book("tests/mock_plantilla.xlsm")

plant.guardar_traer_fn(wb, "traer", "frec", "01_001_A_D", "bruto", 202312)

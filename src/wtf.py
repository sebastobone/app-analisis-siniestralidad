import xlwings as xw

wb = xw.Book("src/plantilla.xlsm")

wb.macro("gonorrea")

print(wb.macro("gonorrea")())
print(wb.macro("mysum")(1, 2))

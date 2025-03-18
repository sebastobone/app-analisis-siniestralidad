# Consolidación de resultados

El botón "Actualizar y abrir archivo de resultados" concatena todos los archivos presentes en la carpeta `output/resultados` y los pega en la hoja "Resultados" del archivo `output/resultados.xlsx`. Si este archivo no está creado, lo crea automáticamente. Si está creado, solamente se sobrescribe la hoja "Resultados", el resto de hojas (en caso de existir) no se modificarán.

Si en la carpeta `output/resultados` hay dos archivos que tienen resultados de la misma apertura, en el archivo `output/resultados.xlsx` solamente quedará la estimación más reciente de esta apertura, la otra será ignorada.

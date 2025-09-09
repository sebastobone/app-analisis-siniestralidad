# Ingreso de parámetros

- **Negocio**: Permite identificar cuál archivo de segmentación y cuáles queries utilizar.
- **Mes de la primera ocurrencia**: Determina el mes que corresponde a la primera fila de los triángulos. Los meses anteriores se agruparán en esta primera fila, de modo que los movimientos de estos meses también quedan recogidos aquí, facilitando la comparación contra SAP.
- **Mes de corte**: Mes de corte de la información.
- **Tipo de análisis**: Puede ser triángulos o entremés.
- **Nombre de la plantilla**: Nombre con el que se guardará la plantilla del análisis. Tenga en cuenta que dentro de un mismo proyecto puede usar varias plantillas, de modo que este parámetro debe llevar el nombre de la que quiere crear (o abrir, si ya está creada) para hacer la estimación. Todas las plantillas quedan guardadas en la carpeta :material-folder: `plantillas`.

Una vez ingrese todos los parámetros, presione el botón **"Guardar parametros"**. Al presionar este botón, se genera la lista desplegable de plantillas en la sección **"Plantilla"** y la sección de **"Referencias entremés"**, en caso de que el tipo de análisis seleccionado sea entremés. Los parámetros ingresados serán almacenados en el archivo :material-file: `data/database.db`.

Si desea recuperar parámetros guardados para no tener que volverlos a ingresar uno por uno, presione el botón **"Traer parametros"**, y luego presione el botón **"Guardar parametros"**.

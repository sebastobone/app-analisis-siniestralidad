# Ingreso de parámetros

- **Negocio**: Permite identificar cuál archivo de segmentación y cuáles queries utilizar.
- **Mes de la primera ocurrencia**: Determina el mes que corresponde a la primera fila de los triángulos. Los meses anteriores se agruparán en esta primera fila, de modo que los movimientos de estos meses también quedan recogidos aquí, facilitando la comparación contra SAP.
- **Mes de corte**: Mes de corte de la información.
- **Tipo de análisis**: Puede ser triángulos o entremés.
- **Aproximar reaseguro**: Solamente aplica para el negocio de **autonomía**, el cual tiene una estrategia de aproximación de reaseguro dada la indisponibilidad de esta información al momento del cierre. El resto de negocios deben dejar este parámetro en "No".
- **Cuadrar siniestros contra SAP**: Define si se reasignarán las diferencias entre SAP y Teradata para los siniestros, de modo que las cifras de pago bruto, pago retenido, aviso bruto y aviso retenido sean exactamente las contables. La reasignación se hará en función de las aperturas definidas en el archivo `data/segmentacion.xlsx`, así que se generará un error si no hizo previamente esta definición.
- **Añadir fraude a siniestros**: Solamente aplica para el negocio de **SOAT**, el resto de negocios deben dejar este parámetro en "No".
- **Cuadrar primas contra SAP**: Mismo concepto que el cuadre de siniestros, pero en este caso, para las primas brutas y retenidas, tanto emitidas como devengadas. Requiere también configuración en `data/segmentacion.xlsx`.
- **Nombre de la plantilla**: Nombre con el que se guardará la plantilla del análisis. Tenga en cuenta que dentro de un mismo proyecto puede usar varias plantillas, de modo que este parámetro debe llevar el nombre de la que quiere crear (o abrir, si ya está creada) para hacer la estimación. Todas las plantillas quedan guardadas en la carpeta `plantillas`.

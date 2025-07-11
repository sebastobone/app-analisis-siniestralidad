# Extracción de información

Al pulsar los botones de ejecución de consultas, la sección “Estado” mostrará en tiempo real el progreso de la operación. Los resultados se guardan en la carpeta :material-folder: `data/raw` en dos formatos:

- `.parquet`: usado internamente por la aplicación.
- `.csv`: para que el usuario pueda explorar los datos con herramientas externas si lo requiere.

Para cada conjunto de datos (por ejemplo, siniestros), se crean dos archivos:

- :material-file: `siniestros.parquet` (o :material-file: `siniestros.csv`): versión sujeta a modificaciones durante el proceso de cuadre contable.
- :material-file: `siniestros_teradata.parquet` (o :material-file: `siniestros_teradata.csv`): copia inalterada de los datos crudos extraídos de Teradata, que sirve como referencia original.

Este mismo esquema de nombres y formatos se aplica a todas las demás cantidades que procese la aplicación.

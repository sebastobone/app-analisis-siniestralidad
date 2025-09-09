# Extracción de información

Las consultas que se ejecutan están almacenadas en la carpeta :material-folder: `data/queries/{negocio}`.

## Verificación de tablas de segmentación

Si las consultas utilizan **tablas de segmentación**, estas serán leídas desde el archivo :material-file: `data/segmentacion_{negocio}.xlsx`. El sistema realiza automáticamente las siguientes validaciones:

- Que las tablas estén correctamente nombradas, según el estándar descrito en [esta documentación](../config/aperturas.md).
- Que haya el número adecuado de tablas requeridas por la consulta.
- Que cada tabla tenga el número correcto de columnas.
- Que las columnas cumplan con los tipos de datos definidos en la consulta.
- Que no existan registros duplicados (en caso de haberlos, se eliminarán y se mostrará una alerta).
- Que no haya valores nulos en las columnas.

Si alguna validación falla, el sistema mostrará un mensaje de error. Deberá corregir la tabla correspondiente y volver a ejecutar la consulta.

## Verificaciones sobre consultas

Las consultas deben utilizar los valores definidos en los parámetros **"Mes de la primera ocurrencia"** y **"Mes de corte"** para parametrizar los filtros de fechas.

Al ejecutar una consulta, el estado del proceso se mostrará en tiempo real en la sección **“Estado”**. Una vez finalizada la ejecución, el sistema valida lo siguiente:

- Que el resultado contenga las [columnas mínimas requeridas](../config/queries.md), según el tipo de consulta.
- Que las columnas de fechas estén en formato correcto.
- Que el rango de fechas obtenido coincida con los valores definidos en los parámetros mencionados. Si esta validación falla, deberá revisar la parametrización de fechas dentro de la consulta.
- Que las columnas de apertura no contengan valores vacíos ni valores -1.
- **Solo para consultas de siniestros**: Que todas las aperturas generadas estén presentes en el archivo :material-file: `data/segmentacion_{negocio}.xlsx`. Si existen aperturas en ese archivo que no aparecen en el resultado de la consulta, se mostrará una alerta (no se generará un error).

En caso de que alguna de estas verificaciones falle, el sistema lo notificará y no continuará con el procesamiento hasta que se corrija el error y se vuelva a ejecutar la consulta.

**Nota:** el sistema elimina automáticamente cualquier columna que no corresponda a:

- Una **columna mínima requerida**, o
- Una **columna de apertura** definida en el archivo :material-file: `data/segmentacion_{negocio}.xlsx`.

## Almacenamiento de consultas

Los resultados se guardan en la carpeta :material-folder: `data/raw` en dos formatos:

- `.parquet`: usado internamente por la aplicación.
- `.csv`: para que el usuario pueda explorar los datos con herramientas externas si lo requiere.

Para cada conjunto de datos (por ejemplo, siniestros), se crean dos archivos:

- :material-file: `siniestros.parquet` (o :material-file: `siniestros.csv`): versión sujeta a modificaciones durante el proceso de cuadre contable.
- :material-file: `siniestros_teradata.parquet` (o :material-file: `siniestros_teradata.csv`): copia inalterada de los datos crudos extraídos de Teradata, que sirve como referencia original.

Este mismo esquema de nombres y formatos se aplica a primas y expuestos.

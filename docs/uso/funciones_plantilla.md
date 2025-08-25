# Funciones de la plantilla

## Abrir

La función **"Abrir"** permite acceder a una plantilla con el nombre especificado:

- Si la plantilla **no existe**, el sistema la creará automáticamente.
- Si la plantilla **ya existe**, se abrirá tal como está, **sin eliminar su contenido actual**.

A diferencia de la función **"Preparar"**, esta opción **no borra ni reinicia** la plantilla.

## Preparar

Esta función tiene dos objetivos principales: preparar los datos para el procesamiento y generar la estructura de la plantilla.

### Procesamiento

Lee los archivos ubicados en la carpeta :material-folder: `data/raw` y los transforma al formato requerido por la plantilla. Los resultados se guardan en la carpeta :material-folder: `data/processed`.

#### Siniestros

- **Triángulos**: se generan bases de triángulos con ocurrencias en cuatro periodicidades (mensual, trimestral, semestral y anual).
- **Para entremés**: se generan bases de triángulos con ocurrencias en tres periodicidades (trimestral, semestral, anual) y desarrollo mensual, junto con una base mensual para el periodo en curso.

#### Primas y expuestos

Se crea una única base con las cuatro periodicidades posibles a partir de los datos mensuales. Para las periodicidades mayores a mensual, los datos se agregan así:

- **Primas:** se agregan por suma.
- **Expuestos:** se agregan por promedio.

### Plantilla

Esta parte de la función:

- Crea la plantilla con el nombre especificado si no existe; si ya existe, la abre y **elimina el contenido de las hojas Resumen, Atípicos, y Entremés**.
- Genera las hojas **Resumen**, **Atípicos** y **Entremés** a partir de los archivos :material-file: `data/processed/base_triangulos.parquet` y :material-file: `data/processed/base_ultima_ocurrencia.parquet`. Estos insumos se filtran según las periodicidades definidas en la hoja **"Apertura_Siniestros"** del archivo :material-file: `data/segmentacion_{negocio}.xlsx`.
- Genera la hoja **Histórico** a partir de los archivos de la carpeta :material-folder: `output/resultados`.
- Oculta y elimina el contenido las hojas no relevantes según el tipo de análisis. Por ejemplo, en un análisis de triángulos se ocultan y se elimina el contenido las hojas **Entremés** y **Completar_diagonal**.

**Advertencia:** si ejecuta esta función sobre una plantilla ya existente, se borrarán los resultados de la hoja **Resumen**, y tendrá que ejecutar la función **"Traer y guardar"** o remitirse al versionamiento de OneDrive para recuperar los resultados. Si solo desea abrir una plantilla existente sin modificar su contenido, utilice la función **"Abrir"**.

## Funciones por apertura

Estas funciones aplican exclusivamente a las hojas de análisis por apertura (Frecuencia, Severidad, Plata y Completar_diagonal).

### Generar

Crea toda la estructura necesaria para el análisis de triángulos correspondiente a la apertura y atributo seleccionados.

### Guardar

Almacena todos los parámetros definidos en la plantilla para la apertura y el atributo seleccionados. Esto incluye:

- Triángulo de exclusiones.
- Ventanas de factores de desarrollo.
- Tipo de factor seleccionado (promedio, mediana, promedio ponderado, etc.).
- Vector de factores seleccionados.

Además, en los casos de Frecuencia, Severidad y Plata, también se guardan:

- Tipo de metodología (pago o incurrido).
- Vector de ultimate (valores y fórmulas).
- Metodología por ocurrencia (Chain-Ladder, Bornhuetter-Ferguson o Indicador).
- Triángulo de factores de desarrollo (por si se decide modificar los factores de una o varias ocurrencias).
- Vector de indicador (relevante solo para metodologías Bornhuetter-Ferguson e Indicador).
- Columna de comentarios.

Estos datos se almacenan en formato `.parquet` en la carpeta :material-folder: `data/db`.

### Traer

Carga en la plantilla los parámetros almacenados previamente para la apertura y el atributo seleccionados. Es el inverso de la función **"Guardar"**.

### Guardar fórmulas entremés

Guarda todas las fórmulas de la hoja **Entremés** comenzando desde la columna **porcentaje_desarrollo_pago_bruto**. Los datos se almacenan en formato `.parquet` dentro de la carpeta :material-folder: `data/db`.

### Traer fórmulas entremés

Ejecuta **"Preparar"** para actualizar los datos de las hojas **Resumen**, **Atípicos**, y **Entremés**. Luego, se pegan las fórmulas previamente guardadas para la hoja **Entremés**, comenzando desde la columna **porcentaje_desarrollo_pago_bruto**.

### Guardar todo

Ejecuta la función **"Guardar"** para todas las aperturas en la plantilla y atributo seleccionados.

### Traer y guardar todo

Ejecuta **"Preparar"** para actualizar los datos de las hojas **Resumen**, **Atípicos**, y **Entremés**. Luego, se ejecutan las funciones **"Traer"** y **"Guardar"** para todas las aperturas en la plantilla y atributo seleccionados. Esta funcionalidad es útil cuando se ha modificado la información real, pero se desea conservar el mismo criterio actuarial y actualizar las estimaciones rápidamente.

## Almacenar análisis

Concatena la información contenida en las hojas **Resumen** y **Atípicos**, y guarda el resultado en la carpeta :material-folder: `output/resultados`. El archivo generado representa la estimación final de siniestralidad última desarrollada en la plantilla.

## Ajustar límites para gráfica de factores

Cuando se cambian los parámetros de periodos y altura para la gráfica de factores, los ejes pueden quedar desajustados respecto a los nuevos datos. Esta función permite ajustarlos.

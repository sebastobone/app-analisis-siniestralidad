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

- Crea la plantilla con el nombre especificado si no existe; si ya existe, **la abre y elimina todo su contenido**.
- Genera las hojas **Resumen** y **Atípicos** a partir de los archivos :material-file: `data/processed/base_triangulos.parquet` y :material-file: `data/processed/base_ultima_ocurrencia.parquet`, filtrando según las periodicidades definidas en la hoja **"Apertura_Siniestros"** del archivo :material-folder: `data/segmentacion_{negocio}.xlsx`.
- Genera la hoja **Histórico** a partir de los archivos de la carpeta :material-folder: `output/resultados`.
- Oculta las hojas no relevantes según el tipo de análisis. Por ejemplo, en un análisis de triángulos se ocultan las hojas **Entremés** y **Completar_diagonal**.

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

### Guardar todo

Ejecuta la función **"Guardar"** para todas las aperturas y atributos de la plantilla.

### Traer y guardar todo

Ejecuta **"Traer"** seguido de **"Guardar"** para todas las aperturas y atributos. Es útil cuando ha cambiado la información real pero se desea conservar el mismo criterio actuarial para actualizar las estimaciones rápidamente.

## Almacenar análisis

Concatena la información contenida en las hojas **Resumen** y **Atípicos**, y guarda el resultado en la carpeta :material-folder: `output/resultados`. El archivo generado representa la estimación final de siniestralidad última desarrollada en la plantilla.

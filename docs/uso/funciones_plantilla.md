# Funciones de la plantilla

## Abrir

Esta función crea una nueva plantilla con el nombre especificado, si aún no está creada. De lo contrario, abre la que está actualmente creada. A diferencia de la función "Preparar plantilla", esta función no elimina los contenidos actuales de la plantilla.

## Preparar

Esta función cumple las siguientes funciones:

- Crea una nueva plantilla con el nombre especificado, si aún no está creada. De lo contrario, abre la que está actualmente creada y elimina todos los contenidos.
- Genera la hoja de Resumen con las periodicidades especificadas en la hoja "Apertura_Siniestros" del archivo `data/segmentacion.xlsx`.
- Genera la hoja de Atípicos.
- Genera la hoja de Histórico a partir de los archivos almacenados en la carpeta `output/resultados`.
- Esconde las hojas no relevantes y muestra las relevantes. Por ejemplo, en un análisis de triángulos, esconde las hojas de Entremés y Completar_diagonal.

¡Cuidado! Si utiliza esta función sobre una plantilla ya creada, sobrescribirá sus resultados (los tendría que recuperar desde el historial de versiones de OneDrive). Si simplemente desea abrir una plantilla que ya había preparado, utilice la función "Abrir plantilla".

## Funciones por apertura

Las siguientes funciones aplican únicamente para las hojas donde se desarrollan análisis de triángulos por apertura, es decir:

- Frecuencia
- Severidad
- Plata
- Completar_diagonal

### Generar

Esta función genera toda la estructura necesaria para el análisis de triángulos en la plantilla seleccionada, para la apertura y el atributo seleccionados.

### Guardar

Esta función almacena todos los parámetros ingresados en la plantilla seleccionada, para la apertura y el atributo seleccionados. Estos parámetros son:

- Triángulos de exclusiones.
- Ventanas de factores de desarrollo.
- Tipo de factores seleccionados (promedio/mediana/promedio ponderado ventana/etc).
- Vector de factores seleccionados.

Adicionalmente, en el caso de las plantillas de triángulos (Frecuencia, Severidad, y Plata) se guardan también las siguientes cantidades:

- Metodología (pago o incurrido).
- Vector de ultimate (como fórmulas y valores).
- Metodología por ocurrencia (chain-ladder, bornhuetter-ferguson, indicador).
- Triángulo de factores de desarrollo completo (por si se decide modificar los factores de una o varias ocurrencias).
- Vector de indicador (sólo es relevante para metodologías de bornhuetter-ferguson e indicador).
- Columna de comentarios.

Estas cantidades se almacenan en la carpeta `data/db` como una serie de archivos `.parquet`.

### Traer

Esta función recupera los parámetros almacenados para la apertura y el atributo seleccionados en la plantilla seleccionada, leyendo los datos de la carpeta `data/db`. Se puede entender como el inverso del modo "Guardar".

### Guardar todo

Esta función itera sobre todas las aperturas y atributos ejecutando el modo "Guardar".

### Traer y guardar todo

Esta función itera sobre todas las aperturas y atributos ejecutando el modo "Traer" y, posteriormente, el modo "Guardar". Es útil para situaciones donde se da un cambio en la información pero no en el criterio actuarial seleccionado, y se requiere actualizar rápidamente las cifras siguiendo el mismo criterio.

## Almacenar análisis

Esta función concatena la tabla de la hoja Resumen con la tabla de la hoja Atípicos y almacena el resultado en la carpeta `output/resultados`. Este archivo contiene la estimación de siniestralidad última desarrollada en la plantilla correspondiente.

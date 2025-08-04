# Descripción de la plantilla

## Hojas comunes

- **Resumen**: Contiene el total de las cifras de siniestros, primas, y expuestos por apertura y ocurrencia. Además, es el lugar donde se almacenan los resultados de siniestralidad última, frecuencia última, y severidad última; así como los comentarios, metodologías, y criterios asociados a las estimaciones. Contiene únicamente información de siniestros típicos.

- **Atípicos**: Es el homólogo a la hoja resumen, pero contiene únicamente información de siniestros atípicos.

- **Histórico**: Tiene la misma estructura que la hoja resumen, con la diferencia de que contiene toda la información de cierres anteriores (leída desde la carpeta `output/resultados`). Adicionalmente, presenta también una columna que indica el mes del cierre correspondiente (*mes_corte*) y el tipo de análisis (si fue triángulos o entremés). Contiene información de siniestros típicos y atípicos.

## Hojas para análisis de triángulos

- **Indexaciones**: Esta hoja se utiliza para definir vectores de indexación para la severidad, en caso de que se vaya a utilizar las metodologías de indexación por fecha de ocurrencia o por fecha pago. De lo contrario, se puede dejar vacía.

- **Frecuencia**: Permite hacer análisis de triángulos de frecuencia, sea por pago o por incurrido.

- **Severidad**: Permite hacer análisis de triángulos de severidad, sea por pago o por incurrido, y opcionalmente por una metodología de indexación por fecha de ocurrencia o fecha de pago.

- **Plata**: Permite hacer análisis de triángulos de plata, sea por pago o por incurrido.

## Hojas para análisis de entremés

- **Entremés**: Tiene la misma estructura de la hoja Resumen, pero cuenta con columnas adicionales para estimar las diferentes metodologías del entremés.

- **Completar diagonal**: Permite hacer análisis de triángulos para estimar la completitud de la diagonal y actualizar la siniestralidad última en función de cómo se viene desarrollando el entremés.

# Particularides en el proceso de SOAT

## Extracción de información

### Expuestos desde VSOAT_POLIZA

En la compañía, se cuenta con varios atributos en Teradata que permiten identificar y relacionar los contratos de seguros con sus características acordadas. Estos atributos se almacenan en diferentes tablas o vistas.

En el caso específico del SOAT, anteriormente, la exposición se calculaba utilizando los datos de la tabla V_POLIZA_CERTIFICADO_ETL, que contiene información sobre los certificados. Se ha identificado que la tabla VSOAT_POLIZA incluye pólizas cuyos certificados no están en la tabla V_POLIZA_CERTIFICADO_ETL. Por consiguiente, calcular la exposición exclusivamente desde esta última tabla podría resultar en una subestimación de esta.

Dado que la tabla VSOAT_POLIZA también proporciona las fechas necesarias para estimar la exposición de manera más precisa, se ha decidido adoptar VSOAT_POLIZA como la fuente de datos principal para los cálculos de exposición de SOAT, garantizando así la integridad y consistencia de los datos utilizados.

### Prima calculada manual

La prima devengada manual sin descuento o prima calculada manual es un concepto introducido para mantener la consistencia en los cálculos actuariales basados en la siniestralidad, sin verse afectados por los cambios en las primas de los códigos con tarifas diferenciales establecidos por el DECRETO 2497 DE 2022.

Para su cálculo, se parte de la prima emitida, asumiendo que todas las pólizas se expiden el día 16 de cada mes. En consecuencia, se devenga 1/24 de la prima tanto el mes de expedición como un año después, y 1/12 de la prima en los 11 meses intermedios. En Teradata, se crean tablas auxiliares para realizar este cálculo. Una tabla se combina con una versión auxiliar de sí misma, aplicando la lógica previamente descrita. A los meses fuera de este rango de fechas se les asigna un valor de 0.

Hasta este punto, se obtiene la prima devengada. Por su parte, para eliminar el efecto del descuento, se introduce un factor que reconoce la tarifa de los riesgos en ausencia de cambios, el cual varía anualmente y difiere entre vehículos con tarifa diferencial y el resto de los vehículos.

### Umbral aceptado diferencias históricas

En definición.

## Cuadre contable

El ajuste se realiza en la apertura de “MOTOS – DIGITAL” debido a que es la apertura de mayor participación en la siniestralidad del ramo. Anualmente, el equipo de Gestión Técnica validará la apertura de mayor participación y realizará los ajustes pertinentes para que la diferencia sea asignada correctamente.

En primas no se hace cuadre sobre la prima devengada, pues como se explicó anteriormente, esta se calcula manual desde Teradata, y por lo tanto no debería coincidir exactamente con la prima devengada de SAP.

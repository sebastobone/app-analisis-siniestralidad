# Analizar triángulos

Existen dos caminos para estimar por triángulos:

1. **Frecuencia y severidad**: Calcula la siniestralidad última como el producto de las dos cantidades.
2. **Plata**: Calcula la siniestralidad última directamente.

El proceso se mostrará con **Plata**, pero es análogo para frecuencia y severidad. Recuerde: la estimación se realiza **apertura por apertura**.

## Generar la hoja de análisis

![Preparación triángulos.](../assets/frontend/plantilla.png)

1. Seleccione en las listas desplegables la **apertura** y el **atributo**.
2. Seleccione la plantilla **"Plata"**.
3. Presione el botón **Generar**.

!!! tip
    Si le sale un error de OneDrive/SharePoint al presionar **Generar**, consulte la solución en la [guía de problemas frecuentes](../faq.md#2-error-de-onedrivesharepoint).

## Estructura de la hoja de análisis

### Triángulos acumulados

![Triángulos acumulados.](../assets/plantilla/triangulos.png)

Muestran los pagos (azul) e incurridos (amarillo).

- Cada fila corresponde a una ocurrencia (periodo de ocurrencia del siniestro).
- Cada columna corresponde a una altura (tiempo transcurrido desde la ocurrencia hasta el pago o movimiento).

### Ratios

![Ratios.](../assets/plantilla/ratios.png)

Aquí se calculan los **factores de desarrollo**, los cuales representan la tasa de evolución de la cifra de pago o incurrido entre una altura y la siguiente.

#### Gráfica de factores

![Gráfica factores.](../assets/plantilla/grafica_factores.png)

Permite visualizar el comportamiento y la tendencia de los factores de desarrollo en una altura específica.

- La **línea azul** muestra los factores de desarrollo observados para la altura seleccionada en cada periodo de ocurrencia.
- Las **líneas rojas** indican los percentiles 20 y 80.
- Las zonas sombreadas entre estas líneas representan rangos de percentiles: 20–40, 40–60 y 60–80.
- La **línea verde** muestra el factor actualmente seleccionado.

Esta gráfica es dinámica: se actualiza automáticamente al modificar cualquiera de los siguientes parámetros:

- Metodología (pago o incurrido)
- Número de periodos a visualizar
- Altura

!!! tip
    Si la gráfica se descuadra al cambiar parámetros, utilice el botón **Ajustar límite gráfica factores**.

### Exclusiones

![Exclusiones.](../assets/plantilla/exclusiones.png)

En algunos casos, pueden presentarse factores atípicos debido a condiciones de negocio o fenómenos específicos que alteran su comportamiento respecto al resto de la altura.

- Marque con **0** los factores que desea excluir.
- Los factores a incluir se dejan en **1**.

### Estadísticos

![Estadísticos.](../assets/plantilla/estadisticos.png)

La estrategia clave en el método Chain-Ladder consiste en seleccionar de forma adecuada los factores de desarrollo que se utilizarán  para proyectar los periodos faltantes del triángulo. Para cada altura del triángulo debe definirse un factor de desarrollo específico.

- A la izquierda se definen las **ventanas temporales** asociadas a cada estadístico, las cuales pueden ser modificadas por el usuario.
- El estadístico seleccionado se resalta en **azul claro**, puede modificarse desde la celda ubicada en la primera fila de esta sección.
- Si necesita cambiar manualmente la lógica de una altura específica, edite la fórmula en la fila **“factor seleccionado”**, que es la que se utiliza en el cálculo final del triángulo proyectado.

### Triángulo base

![Triángulo base.](../assets/plantilla/base.png)

Permite modificar manualmente los factores de desarrollo celda por celda.

!!! tip
    Esto es útil en casos donde, por ejemplo, se quiere seleccionar factores diferentes para una ocurrencia particular.

### Triángulos desarrollados

![Triángulos completos.](../assets/plantilla/evolucion.png)

- **"Evolucion Chain-Ladder"**: Es el triángulo evolucionado con los factores seleccionados.
- **"Evolucion"**: Es el triángulo evolucionado con los factores seleccionados, pero reescala las ocurrencias donde se seleccionó una metodología diferente a Chain-Ladder. Este triángulo representa el resultado final del análisis.

### Tabla resumen

![Tabla resumen.](../assets/plantilla/tabla_resumen.png)

Esta tabla contiene el resultado final del análisis.

- Las celdas con **fondo gris** son **paramétricas**, lo que significa que pueden ser modificadas libremente por el usuario.
- Por defecto, las estimaciones se basan en el **pago**. Para usar el **incurrido**, escriba "incurrido" en la celda superior izquierda.

#### Metodologías disponibles

|Metodología                    |¿Cómo activarla?               |¿En qué consiste?                                         |
|-------------------------------|-------------------------------|----------------------------------------------------------|
|**Chain-Ladder** (por defecto) |Escriba "chain-ladder"         |Estima con factores de desarrollo                         |
|**Indicador**                  |Escriba "indicador"            |Usa un valor esperado o un % sobre la prima devengada     |
|**Bornhuetter-Ferguson**       |Escriba "bornhuetter-ferguson" |Pondera Chain-Ladder e Indicador según el % de desarrollo |

La tabla incluye también columnas de apoyo (**"indicador chain-ladder"**, **"expuestos"**, etc.) y un espacio de **comentarios** para que el usuario registre los criterios, decisiones y justificaciones aplicadas en la estimación de cada ocurrencia.

!!! tip
    Procure no dejar la columna de comentarios vacía. Será útil para usted y para los auditores.

#### Gráfica de ultimate

![Gráfica de ultimate](../assets/plantilla/grafica_ultimate.png)

Muestra una comparación de pago, incurrido, y ultimate para cada periodo de ocurrencia.

## Pasos finales

1. Presione **Guardar**. La siguiente información será almacenada en la carpeta :material-folder: `data/db`:

    - Triángulo de exclusiones.
    - Ventanas de tiempo para estadísticos.
    - Vector de factores seleccionados.
    - Triángulo base.
    - Metodología (pago o incurrido).
    - Ultimate por ocurrencia.
    - Metodología por ocurrencia.
    - Indicador por ocurrencia.
    - Comentarios.

2. Para analizar una nueva apertura:

    - **Desde cero**: seleccione la apertura en la lista desplegable y presione **Generar**. Esto cargará la configuración por defecto.
    - **Manteniendo los parámetros y criterios actuales**: seleccione la apertura en la lista desplegable y presione **Actualizar**. Esto conserva la configuración vigente como punto de partida.

Si ha finalizado todos los análisis y validaciones y desea consolidar la información final, presione el botón **Almacenar análisis**.

## Modificar un análisis ya guardado

1. Seleccione la apertura y el atributo correspondientes desde las listas desplegables.
2. Presione **Traer** para cargar los datos almacenados.
3. Haga las modificaciones necesarias.
4. Presione **Guardar** para actualizar los resultados almacenados.

## Análisis adicionales

Si desea realizar cálculos adicionales:

1. Copie la información desde la hoja **Resumen** o desde el archivo :material-file: `output/resultados.xlsx` a un nuevo archivo independiente.
2. Guarde el nuevo archivo en la carpeta :material-folder: `plantillas`.

!!! info
    Este archivo no será afectado por los procesos de la aplicación, por lo que puede modificarlo libremente sin riesgo de sobrescritura.

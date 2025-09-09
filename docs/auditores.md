<!--markdownlint-disable MD007-->

# Guía para auditores

## Insumos de negocio

Para su revisión, el negocio le entregará:

- La carpeta donde desarrolló el análisis. La estructura de esta carpeta se describe en la [guía de estructura](https://sebastobone.github.io/app-analisis-siniestralidad/estructura).
- Los [parámetros utilizados](https://sebastobone.github.io/app-analisis-siniestralidad/uso/parametros).
- En caso de análisis de triángulos, el tipo de estimación realizada (**Frecuencia y Severidad** o **Plata**).

## Ejecutar la app

Para ejecutar la app, siga los siguientes pasos:

1. Asegúrese de tener instalado en su equipo [uv](https://docs.astral.sh/uv/getting-started/installation/) y Microsoft Excel.
2. En el explorador de archivos, haga clic derecho sobre la carpeta del análisis y seleccione **“Abrir en terminal”**.
3. Ejecute los siguientes comandos:

    ```sh
    uv sync
    uv run run.py
    ```

4. Abra su navegador e ingrese al [localhost](http://127.0.0.1:8000).

## Ingresar parámetros de negocio

En el navegador verá la siguiente interfaz:

![Ingreso de parámetros](uso/assets/frontend/parametros_auditoria.png)

1. Ingrese los parámetros comunicados por el negocio.
2. Presione **"Guardar parámetros"**.

## Abrir el libro de trabajo

1. En la interfaz web, diríjase a la sección **“Plantilla”**.

    ![Preparación triángulos.](uso/assets/triangulos/preparacion_triangulos.png)

2. Presione **"Generar aperturas"**.
3. Presione **"Abrir plantilla"**.

A partir de aquí, podrá acceder a los análisis realizados (triángulos o entremés).

## Análisis de triángulos

El negocio puede haber aplicado una de estas dos metodologías:

- **Frecuencia y Severidad**: la siniestralidad última se obtiene como el producto de ambas.
- **Plata**: la siniestralidad última se calcula directamente.

El procedimiento es análogo en ambos casos. Si el negocio utilizó **Frecuencia y Severidad**, las hojas de análisis relevantes serán **"Frecuencia"** y **"Severidad"**, y si utilizó **Plata**, será **"Plata"**.

!!! tip
    Para entender la estructura de las hojas de análisis, consulte la [guía de uso de triángulos](https://sebastobone.github.io/app-analisis-siniestralidad/uso/triangulos).

### Revisión de apertura

En la interfaz web:

1. Seleccione la **apertura** y el **atributo** que desea revisar.
2. Seleccione la **plantilla** que corresponda según la metodología que aplicó el negocio.
3. Presione **“Traer”**.

Los criterios de estimación del negocio están en:

#### Factores excluidos

![Exclusiones](uso/assets/triangulos/exclusiones.png)

#### Estadísticos de factores

- Ventanas de tiempo para estadísticos
- Vector de factores seleccionados

![Estadísticos](uso/assets/triangulos/estadisticos.png)

#### Triángulo base

![Triángulo base](uso/assets/triangulos/triangulo_base.png)

#### Tabla resumen

- Metodología de pago o incurrido
- Ultimate por ocurrencia
- Metodología por ocurrencia
- Indicador por ocurrencia
- Comentarios por ocurrencia

![Triángulo base](uso/assets/triangulos/tabla_resumen.png)

En la hoja **“Resumen”** encontrará los resultados consolidados de siniestralidad para todas las aperturas.

!!! note "Nota"
    Para cambiar de apertura, repita el proceso: seleccione en los menús desplegables y presione **"Traer"**.

## Análisis de entremés

Este análisis está en las hojas **"Entremés"** y **"Completar_diagonal"**.
La estructura de ambas hojas se detalla en la [guía de uso de entremés](https://sebastobone.github.io/app-analisis-siniestralidad/uso/entremes).

En la hoja **"Entremés"**, las columnas con los criterios definidos por el negocio están sombreadas en **gris**.

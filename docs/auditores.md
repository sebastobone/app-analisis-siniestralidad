<!--markdownlint-disable MD007-->

# Guía para auditores

## Insumos de negocio

Para su revisión, el negocio le entregará:

- La carpeta donde desarrolló el análisis. La estructura de esta carpeta se describe en la [guía de estructura](estructura.md).
- Los [parámetros utilizados](uso/parametros.md).
- En caso de análisis de triángulos, el tipo de estimación realizada (**Frecuencia y Severidad** o **Plata**).

## Ejecutar la app

Para ejecutar la app, siga los siguientes pasos:

1. Asegúrese de tener instalado en su equipo [uv](https://docs.astral.sh/uv/getting-started/installation/) y Microsoft Excel.
2. En el explorador de archivos, haga clic derecho sobre la carpeta del análisis y seleccione **“Abrir en terminal”**.

    ![Abrir terminal](assets/terminal.png)

3. Ejecute los siguientes comandos en la terminal:

    ```sh
    uv sync
    uv run run.py
    ```

    Una vez terminen de ejecutar, la terminal se verá así:

    ![Terminal](assets/terminal_run.png)

4. Abra su navegador e ingrese a la URL indicada en la terminal (_INFO: Uvicorn running on..._). Debería ver una página como esta:

    ![Interfaz web](uso/assets/frontend/interfaz.png)

    !!! info
        Esta URL es propia de su computador, por lo tanto, sólo usted puede acceder.

## Ingresar parámetros de negocio

En la página verá la siguiente sección:

![Ingreso de parámetros](uso/assets/frontend/parametros_auditoria.png)

1. Ingrese los parámetros comunicados por el negocio.
2. Presione **"Guardar parámetros"**.

## Abrir el libro de trabajo

1. En la interfaz web, diríjase a la sección **“Plantilla”**.

    ![Preparación triángulos.](uso/assets/triangulos/preparacion_triangulos.png)

2. Presione **"Generar aperturas"**.
3. Presione **"Abrir plantilla"**.

Esto abrirá una copia de la plantilla de Excel donde el negocio realizó sus estimaciones. A partir de aquí, podrá acceder a los criterios de estimación empleados.

## Análisis de triángulos

El negocio puede haber aplicado una de estas dos metodologías:

- **Frecuencia y Severidad**: la siniestralidad última se obtiene como el producto de ambas.
- **Plata**: la siniestralidad última se calcula directamente.

El procedimiento de revisión es el mismo en ambos casos:

- Si se utilizó **Frecuencia y Severidad**, las hojas relevantes serán **"Frecuencia"** y **"Severidad"**.
- Si se utilizó **Plata**, la hoja relevante será **"Plata"**.

!!! tip
    Para entender la estructura de las hojas de análisis, consulte la [guía de uso de triángulos](uso/triangulos.md).

### Revisión de apertura

En la interfaz web:

1. Seleccione la **apertura** y el **atributo** que desea revisar.
2. Seleccione la **plantilla** que corresponda a la metodología aplicada.
3. Presione **“Traer”**.

#### Ejemplo

El negocio **Movilidad** comunicó que utilizó la metodología de **Plata**. Como auditor, iniciaré la revisión en la apertura **01_040_MOTOS RESTO_MOTOS RESTO** para el atributo **bruto**. Debo seleccionar lo siguiente y luego presionar **"Traer"**:

![Ejemplo auditor](assets/ejemplo_auditor.png)

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
    Para cambiar de apertura, repita el proceso: seleccione la nueva apertura en los menús desplegables y presione **"Traer"**.

## Análisis de entremés

El análisis de entremés está en la hoja **"Entremés"**. Las columnas con los criterios definidos por el negocio están sombreadas en **gris**.

Si el negocio utilizó la metodología **"Completar diagonal"** en alguna apertura, podrá revisar los parámetros y criterios correspondientes en la hoja **"Completar_diagonal"**. Para ello:

1. Seleccione en la interfaz web la **apertura** y el **atributo** correspondientes.
2. Presione el botón **"Traer"**.

!!! tip
    Para entender las estructuras de las hojas **"Entremés"** y **"Completar_diagonal"**, consulte la [guía de uso de entremés](uso/entremes.md).

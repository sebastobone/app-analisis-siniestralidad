<!--markdownlint-disable MD007-->

# Validaciones de información

Cada vez que genera información de **siniestros, primas o expuestos** (ya sea por extracción desde Teradata o por carga manual), el sistema aplica automáticamente un conjunto de validaciones.

!!! warning "Errores en validación"
    Si algún archivo falla, el sistema mostrará un **mensaje de error**. Deberá corregir la consulta o el archivo y volver a intentarlo.

## Lista de validaciones

El sistema revisa:

1. **Columnas mínimas requeridas**

    - Deben estar todas presentes.
    - La lista se encuentra en la [guía de construcción de consultas](../../config/queries.md).

2. **Aperturas consistentes**

    - El archivo a cargar sólo puede incluir las aperturas definidas en el [archivo de segmentación](../../config/segmentacion.md).
    - Si tiene alguna apertura extra que no está en ese archivo → **error**.
    - No es necesario que el archivo a cargar contenga todas las aperturas definidas.

3. **Valores nulos**

    - No se permiten columnas con valores vacíos.

4. **Tipos de datos coherentes**

    - **Fechas:** deben estar en formato fecha.
    - **Cantidades:** deben estar en número:
        - Flotante → pagos, primas, expuestos.
        - Entero → conteos.

5. **Fechas fuera de rango**

    - Si hay registros **anteriores al Mes de primera ocurrencia** (definido en [parámetros](../parametros.md)), se agrupan en ese primer mes.
    - Si hay registros **posteriores al Mes de corte**, se eliminan.

## Almacenamiento final

Después de pasar las validaciones, cada archivo se guarda en la [carpeta correspondiente](../../estructura.md) en dos formatos:

- `.parquet`: usado internamente por la aplicación.
- `.csv`: disponible para explorar los datos con herramientas externas.

!!! note "Nota"
    Al guardar los archivos, el sistema elimina automáticamente cualquier columna que **no sea**:

    - Una **columna mínima requerida**.
    - Una **columna de apertura** definida en el [archivo de segmentación](../../config/segmentacion.md).

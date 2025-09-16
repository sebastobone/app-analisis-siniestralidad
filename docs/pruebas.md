<!--markdownlint-disable MD007-->
<!--markdownlint-disable MD046-->

# Pruebas de funcionamiento

La aplicación cuenta con un conjunto de **pruebas automatizadas** ubicadas en la carpeta :material-folder: `tests`.

Estas pruebas forman parte del proceso de aseguramiento de calidad y se ejecutan automáticamente antes de publicar cada nueva versión.

Su objetivo es **garantizar la estabilidad, exactitud y confiabilidad** de la aplicación, evitando que los cambios introduzcan errores en los procesos críticos que dependen de ella.

## Alcance de las pruebas

### Pruebas generales

A partir de un **negocio ficticio** con datos simulados, se valida que:

- Las **alertas y errores de extracción de información** se generen de manera correcta y consistente.
- Las **alertas y errores de carga de información** se generen de manera correcta y consistente.
- Las **validaciones del archivo de segmentación** funcionen según lo esperado.
- Las cifras **reales y estimadas** se mantengan consistentes entre todos los archivos y rutas relevantes:
    - :material-folder: `data/raw`
    - :material-folder: `data/carga_manual`
    - :material-folder: `data/pre_cuadre_contable`
    - :material-folder: `data/post_cuadre_contable`
    - :material-folder: `data/consolidado`
    - :material-folder: `data/processed`
    - :material-folder: `plantillas`
    - :material-folder: `output/resultados`
    - :material-file: `output/resultados.xlsx`
    - :material-file: `output/informe_ar.xlsx`
- Todas las funciones de la plantilla ejecuten exactamente los comportamientos descritos en la [documentación](uso/preparacion_plantilla.md#funciones-de-la-plantilla).

### Pruebas por negocio

Además, se simula un **flujo completo de estimación** para cada negocio, validando que:

- El **archivo de segmentación** esté estructurado de forma correcta.
- Las **consultas SQL** estén bien parametrizadas y devuelvan los datos esperados.
- El proceso de **cuadre contable** se ejecute correctamente y respete las reglas definidas en la segmentación.
- Se generen los **controles de información** y las **evidencias de extracción** necesarias para auditoría.

## Ejecutar las pruebas

### Instalar librerías de desarrollo

Antes de ejecutar las pruebas, asegúrese de instalar las dependencias necesarias. En una terminal, dentro de la carpeta del proyecto, ejecute:

```sh
uv sync --all-groups
```

### Configurar credenciales de Teradata

Para ejecutar pruebas que requieren conexión a **Teradata**, es necesario configurar un archivo privado de credenciales. Cree el archivo :material-file: `.env.private` en la raíz del proyecto con los siguientes comandos:

=== "Windows"

    ```powershell
    Set-Content -Path ".\.env.private" -Value 'TERADATA_USER="___"' -NoNewLine
    Add-Content -Path ".\.env.private" -Value "`nTERADATA_PASSWORD=""___"""
    ```

=== "MacOS"

    ```sh
    echo 'TERADATA_USER="___"' > .env.private
    echo 'TERADATA_PASSWORD="___"' >> .env.private
    ```

El archivo :material-file: `.env.private` quedará guardado en la carpeta principal del proyecto. Ingrese sus credenciales con un editor de texto (por ejemplo, Bloc de notas).

### Correr todas las pruebas

Una vez configurado el entorno, ejecute todas las pruebas con:

```sh
uv run pytest
```

### Correr pruebas sin conexión a Teradata

Si desea ejecutar únicamente las pruebas que **no requieren conexión a base de datos** (útiles en entornos sin acceso a Teradata), use:

```sh
uv run pytest -m "not teradata"
```

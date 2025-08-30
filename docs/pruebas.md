<!--markdownlint-disable MD007-->
<!--markdownlint-disable MD046-->

# Pruebas de funcionamiento

La aplicación cuenta con pruebas automatizadas ubicadas en la carpeta :material-folder: `tests`. Estas pruebas se ejecutan automáticamente antes del lanzamiento de cualquier nueva versión, con el objetivo de garantizar el correcto funcionamiento de la aplicación y preservar la integridad y exactitud de la información a lo largo de todo el proceso.

## Pruebas generales

Partiendo de un negocio ficticio, se valida que:

- Las alertas y errores asociados a las **verificaciones de extracción de información** se generen correctamente.
- Las alertas y errores asociados a las **verificaciones del archivo de segmentación** funcionen como se espera.
- Las cifras reales y estimadas contenidas en los siguientes archivos y rutas sean siempre **consistentes** entre sí:
    - :material-folder: `data/raw`
    - :material-folder: `data/processed`
    - :material-folder: `plantillas`
    - :material-folder: `output/resultados`
    - :material-file: `output/resultados.xlsx`
    - :material-file: `output/informe_ar.xlsx`
- Todas las funciones de la plantilla cumplan con los comportamientos explicados en la [documentación](https://sebastobone.github.io/app-analisis-siniestralidad/uso/funciones_plantilla/).

## Pruebas por negocio

Estas pruebas simulan un flujo completo de estimación para cada negocio, y validan que:

- El archivo de segmentación esté correctamente estructurado.
- Las consultas estén correctamente construidas y parametrizadas.
- El proceso de cuadre contable se ejecute de forma adecuada.
- Se generen correctamente los **controles de información** y las **evidencias** asociadas.

## Ejecutar las pruebas

Para ejecutar las pruebas, primero debe instalar las librerías de desarrollo. Abra una terminal en la carpeta del proyecto y ejecute:

```sh
uv sync --all-groups
```

Para hacer la extraccion de queries, se creará un archivo para almacenar sus credenciales de Teradata. Pegue lo siguiente en la terminal:

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

El archivo :material-file: `.env.private` queda almacenado dentro de la carpeta principal. Para editarlo, ábralo en el bloc de notas.

Para correr todas las pruebas, ejecute el siguiente comando:

```sh
uv run pytest
```

### Pruebas sin conexión a Teradata

Si desea ejecutar únicamente las pruebas que **no requieren extracción de información** (es decir, que no dependen de conexión a Teradata), utilice el siguiente comando:

```sh
uv run pytest -m "not teradata"
```

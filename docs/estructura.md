<!--markdownlint-disable MD007-->

# Estructura del proyecto

Al instalar la aplicación, dentro de la carpeta principal encontrará una estructura de carpetas y archivos. Las rutas y archivos relevantes para el análisis se describen a continuación.

- :material-folder: `data/`
    - :material-folder: `afo/`: Almacena los archivos de Excel con conexión a SAP por medio del complemento Analysis for Office (AFO).
    - :material-folder: `controles_informacion/`: Almacena los controles de información y las evidencias correspondientes generadas.
        - :material-folder: `pre_cuadre_contable/`: Almacena los controles de la información tal cual como sale de Teradata.
        - :material-folder: `post_cuadre_contable/`: Almacena los controles de la información después de realizar el proceso del cuadre contable. Si se decide no hacerlo, esta carpeta quedará vacía.
        - :material-folder: `post_ajustes_fraude/`: Almacena los controles de la información después de realizar el proceso de limpieza de fraude (sólo aplica para SOAT).
    - :material-folder: `db/`: Almacena las fórmulas y valores de los análisis realizados en las plantillas de Excel.
    - :material-folder: `processed/`: Almacena las bases de datos que utiliza la plantilla internamente para construir los triángulos y las hojas de las plantillas de Excel.
    - :material-folder: `queries/`: Almacena los queries de siniestros, primas, y expuestos para cada negocio.
    - :material-folder: `raw/`: Almacena la información que sale de los queries de siniestros, primas, y expuestos; así como esta misma información después de realizar procesos de cuadre contable.
    - :material-file: `segmentacion_{negocio}/`: Solamente debe modificar el de su negocio. Define las aperturas del análisis, así como las aperturas donde se van a asignar las diferencias contables.

- :material-folder: `docs/`: Almacena la documentación del proyecto. No se necesita para el análisis.
- :material-folder: `logs/`: Almacena los "logs" que se van generando a medida que el usuario ejecuta el proceso (fecha y hora de los eventos, registro de errores y avance de procesos). Es la misma información que se reporta en la sección "Estado" del frontend de la aplicación.
- :material-folder: `output/`: Almacena los resultados de los análisis de siniestralidad históricos.
    - :material-folder: `resultados/`: Almacena los resultados del análisis de siniestralidad obtenidos con cada una de las plantillas, en cada una de las fechas de corte.
- :material-folder: `plantillas/`: Almacena las plantillas usadas para el análisis.
- :material-folder: `src/`: Almacena todo el código de la aplicación.
- :material-folder: `tests/`: Almacena los códigos que realizan los testeos automáticos de la aplicación.
- :material-file: `.env.private`: Almacena las credenciales de Teradata del usuario.
- :material-file: `.env.public`: Almacena la dirección para conectarse a Teradata remotamente.
- :material-file: `pyproject.toml`: Especifica las versiones de los paquetes necesarias para instalar la aplicación.
- :material-file: `uv.lock`: Especifica las versiones de los paquetes necesarias para instalar la aplicación.
- :material-file: `.python-version`: Especifica la versión de Python para instalar la aplicación.
- :material-file: `run.py`: Código que ejecuta la aplicación.

El resto de los archivos son de configuración del desarrollo de la aplicación, y por lo tanto, no son necesarios para el análisis.

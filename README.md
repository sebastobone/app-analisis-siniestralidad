# Aplicación para análisis de siniestralidad

Click [aquí](https://sebastobone.github.io/app-analisis-siniestralidad/) para acceder a la documentación completa en línea.

<!--docs-intro-start-->

Esta aplicación permite realizar **análisis de siniestralidad última** mediante metodologías de triángulos y entremés.

## Funcionalidades

- **Extraer información** de siniestros, primas, y expuestos desde **Teradata**.
- **Cargar manualmente información** de siniestros, primas, y expuestos.
- **Cuadrar contablemente** siniestros y primas.
- **Generar automáticamente** controles y reportes SOX.
- **Estimar la frecuencia, severidad, y siniestralidad** por metodologías de **triángulos** o **entremés** desde plantillas de Excel.
- **Almacenar y garantizar la trazabilidad** de criterios y justificaciones del análisis.
- **Consolidar resultados y generar informes**.

## Requisitos

- **[Git](https://git-scm.com/):** para descargar y actualizar la aplicación.
- **[uv](https://docs.astral.sh/uv/getting-started/installation/):** para la gestión de librerías.
- **Microsoft Excel:** para crear las plantillas y realizar los análisis.
- **Acceso a Teradata:** necesario si se desea extraer información directamente de esta fuente.

!!! tip
    Git y uv no requieren permisos de administrador para ser instalados.

## Descargar la app

1. Elija una **carpeta de trabajo** donde quiera guardar los análisis. Haga clic derecho sobre ella y seleccione **Abrir en Terminal**:

    ![Abrir terminal](docs/assets/abrir_terminal.png)

2. En la terminal, copie y ejecute el siguiente comando:

    ```sh
    git clone https://github.com/sebastobone/app-analisis-siniestralidad.git
    ```

    Se descargará una nueva carpeta llamada `app-analisis-siniestralidad`, que es la **carpeta de la aplicación**.

3. Cierre la terminal.

¡Eso es todo! Consulte ahora la [guía de configuración de análisis](docs/config/segmentacion.md) y la [guía de uso de la app](docs/uso/ejecutar_app.md).

<!--markdownlint-disable MD046-->

# Instalación

## Prerrequisitos

Para instalar la aplicación y empezarla a usar en un nuevo análisis, debe tener instalado previamente en su equipo [Git](https://git-scm.com/), [uv](https://docs.astral.sh/uv/getting-started/installation/), y Excel.

## Instrucciones

1. Entre a la carpeta donde va a almacenar el análisis.
2. Abra una terminal en esta carpeta. Para ello, seleccione la carpeta, presione click derecho, y seleccione abrir nueva terminal.
3. En la terminal, copie lo siguiente y presione enter:

      ```sh
      git clone https://github.com/sebastobone/app-analisis-siniestralidad.git
      cd app-analisis-siniestralidad
      uv sync --no-dev
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

Este archivo queda almacenado dentro de la carpeta principal. Si necesita editarlo, puede abrirlo en el bloc de notas.

Para hacer la comparación entre las cifras de Teradata y SAP, se deben descargar los archivos de Excel que permiten conectarse con SAP a través del complemento Analysis for Office (AFO). [Descargue los archivos](https://suramericana-my.sharepoint.com/:f:/g/personal/sebastiantobon_sura_com_co/ErrqzjH-aIRMsAgGij4ptPABWbknTTpJMxfBjFJPU6YIWQ?e=1dPTF6) y péguelos en la carpeta `data/afo`.

¡Eso es todo! Ahora puede proceder con la configuración del análisis. No cierre la terminal, la usará para ejecutar el proceso después.

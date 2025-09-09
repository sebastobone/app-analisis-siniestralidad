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
      uv sync
      ```

Para hacer la comparación entre las cifras de Teradata y SAP, se deben descargar los archivos de Excel que permiten conectarse con SAP a través del complemento Analysis for Office (AFO). [Descargue los archivos](https://suramericana-my.sharepoint.com/:f:/g/personal/sebastiantobon_sura_com_co/ErrqzjH-aIRMsAgGij4ptPABWbknTTpJMxfBjFJPU6YIWQ?e=1dPTF6) y péguelos en la carpeta :material-folder: `data/afo`.

¡Eso es todo! Ahora puede proceder con la configuración del análisis. No cierre la terminal, la usará para ejecutar el proceso después.

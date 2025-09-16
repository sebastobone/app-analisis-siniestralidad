<!--markdownlint-disable MD024-->

# Preguntas y problemas frecuentes

## 1. Error de _hardlink_

![Error de hardlink](assets/faq/hardlink.png)

### Descripción del problema

Este error ocurre porque los requisitos de paquetes para la instalación de la aplicación en la nube no coinciden con los que quedaron almacenados en la memoria del sistema a partir de instalaciones anteriores.

### Pasos para resolverlo

Limpie la memoria caché del administrador de paquetes ejecutando el siguiente comando:

```sh
uv cache clean
```

Una vez limpia la memoria, intente ejecutar nuevamente el comando que le generó el error.

!!! tip
    Independientemente de que se genere este error o no, limpiar la memoria caché del administrador de paquetes es una buena práctica para borrar información obsoleta.

## 2. Error de OneDrive/Sharepoint

![Error de Onedrive/Sharepoint](assets/faq/onedrive.png)

### Descripción del problema

Este error ocurre cuando el **autoguardado** está activado en una plantilla de Excel almacenada en OneDrive o SharePoint. Las funcionalidades de la aplicación dependen en gran medida de la **ruta local del archivo**, pero al activarse el autoguardado, dicha ruta se transforma en una **URL**, lo que puede generar conflictos con ciertos comandos.

### Pasos para resolverlo

1. Desactive el autoguardado en la barra superior de Excel.
2. Presione el botón "guardar" en esta misma ubicación.
3. Cierre el archivo.

Una vez realizados estos pasos, intente ejecutar nuevamente el comando que estaba utilizando.

!!! warning "Advertencia"
    Recuerde guardar manualmente el archivo cada vez que realice cambios importantes en la estructura de la plantilla.

## 3. Error de módulo no encontrado

![Error de módulo no encontrado](assets/faq/module_not_found.png)

### Descripción del problema

Este error se genera cuando las librerías quedaron mal instaladas.

### Pasos para resolverlo

En la terminal, ejecute los siguientes comandos:

```sh
uv venv
uv sync
```

Si le sale una opción de diálogo como la que se muestra en la imagen, presione la tecla **y (_yes_)**:

![Error de módulo no encontrado](assets/faq/uv_venv_yes.png)

Una vez ejecutados, intente ejecutar nuevamente el comando que estaba utilizando.

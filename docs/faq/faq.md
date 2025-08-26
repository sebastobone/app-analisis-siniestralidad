# Preguntas frecuentes

## 1. Error de _hardlink_

![Error de hardlink](assets/hardlink.png)

Este error ocurre porque los requisitos de paquetes para la instalación de la aplicación en la nube no coinciden con los que quedaron almacenados en la memoria del sistema a partir de instalaciones anteriores.

Para resolverlo, limpie la memoria caché del administrador de paquetes ejecutando el siguiente comando:

```sh
uv cache clean
```

Una vez limpia la memoria, intente ejecutar nuevamente el comando que le generó el error.

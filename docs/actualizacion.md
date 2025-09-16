<!--markdownlint-disable MD046-->

# Actualización de la aplicación

## Versionamiento

En [el repositorio oficial](https://github.com/sebastobone/app-analisis-siniestralidad/releases) encontrará:

- El historial completo de versiones publicadas.
- El detalle de cambios de cada versión.

### ¿Cómo verifico mi versión actual?

1. Abra la **carpeta de la aplicación** en una terminal.
2. Escriba el siguiente comando y presione _enter_:

```sh
git describe --tags
```

!!! tip "Recibir notificaciones de nuevas versiones"
    Si quiere recibir un correo cada vez que se publique una nueva versión:

    1. Ingrese al [repositorio](https://github.com/sebastobone/app-analisis-siniestralidad).
    2. Presione el botón **Watch** (arriba a la derecha).
    3. Seleccione **Custom**.
    4. Marque la opción **Releases** y presione **Apply**.

## Pasos para actualizar

1. Abra la **carpeta de la aplicación** en una terminal.
2. Copie estos comandos y presione _enter_:

    ```sh
    git reset --hard HEAD
    git pull
    ```

3. Si tenía la aplicación corriendo en alguna terminal, cierre esa terminal y [vuelva a ejecutar la app](uso/ejecutar_app.md) para que los cambios tomen efecto.

!!! warning "Advertencia"
    La actualización **sobrescribe todos los archivos** que se encuentren dentro del repositorio. Antes de actualizar, asegúrese de guardar una copia de:

    - Su archivo de segmentación :material-file: `data/segmentacion_{negocio}.xlsx`
    - Sus _queries_ :material-file: `data/queries/{negocio}`

    Después de actualizar, vuelva a cargarlos en la interfaz web o péguelos manualmente en las rutas correspondientes.

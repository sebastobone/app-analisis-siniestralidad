# Actualización de la aplicación

## Versionamiento

En [este enlace](https://github.com/sebastobone/app-analisis-siniestralidad/releases) encontrará el historial completo de versiones, junto con el detalle de cambios de cada una.

Para comprobar qué versión tiene instalada en su aplicación, abra la carpeta en una terminal, ejecute el siguiente comando y presione _enter_:

```sh
git describe --tags
```

### Notificaciones de nuevas versiones

Si desea recibir un correo cada vez que se publique una nueva versión, siga estos pasos:

1. Ingrese al [repositorio](https://github.com/sebastobone/app-analisis-siniestralidad).
2. Presione el botón **"Watch"**.
3. En el menú desplegable, seleccione **"Custom"**.
4. Marque la opción **"Releases"** y luego haga clic en **"Apply"**.

¡Esto es todo! A partir de ahora recibirá un correo electrónico cada vez que se cree una nueva versión.

## Pasos para actualizar

Para actualizar la aplicación a la versión más reciente, abra la carpeta en una terminal, copie lo siguiente y presione _enter_:

```sh
git reset --hard HEAD
git pull
```

Si tiene una instancia de la aplicación abierta en la terminal, debe cerrarla y volver a ejecutar la aplicación para que los cambios tomen efecto.

¡Eso es todo! Ahora la aplicación quedará actualizada. Tenga en cuenta que esto sobrescribe todos los archivos que se encuentren dentro del repositorio de GitHub, por lo que se recomienda guardar una copia de su archivo de segmentación :material-file: `data/segmentacion_{negocio}.xlsx` y pegarlo nuevamente en la ruta después de que termine la actualización.

# Actualización de la aplicación

## Versionamiento

En [este link](https://github.com/sebastobone/app-analisis-siniestralidad/tags) puede encontrar la lista completa de versiones, junto con la lista completa de cambios por versión.

Para verificar en qué versión se encuentra su aplicación, abra la carpeta en una terminal, copie lo siguiente y presione _enter_:

```sh
git tag
```

## Pasos para actualizar

Para actualizar la aplicación a la versión más reciente, abra la carpeta en una terminal, copie lo siguiente y presione _enter_:

```sh
git reset --hard HEAD
git pull
```

Si tiene una instancia de la aplicación abierta en la terminal, debe cerrarla y volver a ejecutar la aplicación para que los cambios tomen efecto.

¡Eso es todo! Ahora la aplicación quedará actualizada. Tenga en cuenta que esto sobrescribe todos los archivos que se encuentren dentro del repositorio de GitHub, por lo que se recomienda guardar una copia de su archivo de segmentación y pegarlo nuevamente en la ruta después de que termine la actualización.

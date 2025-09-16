# Extracción de información

Cuando presione **"Guardar parámetros"**, aparecerá la sección **"Extracción de información"**:

![Extraccion](../../assets/frontend/extraccion.png)

## Pasos

1. Verifique que esté conectado a la **VPN o red +SURA**.
2. **(Opcional)** Cargue las consultas que construyó según la [guía de construcción de consultas](../../config/queries.md).

    - Si no carga ninguna consulta, se usarán las más recientes que haya cargado, almacenadas en :material-folder: `data/queries/{negocio}`.
    - Si nunca ha cargado, se usarán las consultas por defecto de la [configuración del cierre contable](../../cierre/configuraciones.md).

3. Ingrese su usuario y contraseña de Teradata.
4. Ejecute los queries.

    !!! tip
        Puede ejecutar varios queries al mismo tiempo. El estado de avance de cada uno se muestra en la sección **Estado**.

## Validación de tablas de segmentación

Si sus consultas utilizan **tablas de segmentación** definidas en el [archivo de segmentación](../../config/segmentacion.md), la aplicación valida automáticamente:

- Que las tablas estén nombradas correctamente, según el estándar descrito en [la guía de construcción de queries](../../config/queries.md#desde-el-archivo-segmentacion-camino-complejo).
- Que haya el número de tablas requerido por la consulta.
- Que cada tabla tenga el número correcto de columnas.
- Que las columnas coincidan con los tipos de datos definidos en la consulta.
- Que no existan registros duplicados. Si los hay, se eliminan y se genera una alerta.
- Que no haya valores nulos.

!!! warning "Errores en validación"
    Si alguna validación falla, el sistema generará un error y se lo mostrará en la sección **Estado**. Tendrá que corregir la tabla correspondiente y volver a ejecutar la consulta.

## Validaciones sobre salidas

El resultado de cada consulta se somete a las [validaciones comunes](validaciones.md) sobre insumos de siniestros, primas, y expuestos.

Los datos extraídos de Teradata se almacenan en :material-folder: `data/raw`.

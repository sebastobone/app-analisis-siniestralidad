# Consultas o _queries_

Para realizar un análisis de triángulos o entremés y calcular una siniestralidad última, necesitamos contar con información de siniestros, primas, y expuestos. La principal forma de obtener esta información es a través de consultas de Teradata, las cuales serán almacenadas en la carpeta `data/queries`.

Los queries contenidos por defecto en esta carpeta son los que utiliza actualmente cada negocio para su proceso mensual de cierre de reservas, pero se pueden modificar libremente en caso de que se quiera realizar un análisis diferente.

A continuación, se presenta una descripción general de la lógica de funcionamiento de cada consulta, así como la estructura mínima que cada una debe contener. Sin embargo, pueden existir particularidades en el entendimiento de cada negocio que modifiquen la lógica de sus consultas.

## Siniestros

Esta consulta recupera la información de los siniestros pagados y avisados (retenidos y brutos) por cada agrupación de reservas (según corresponda en cada negocio) con su respectiva fecha de siniestro y fecha de registro, así como el conteo incurrido, bruto y desistido.

Existen diferentes modelos de datos para las áreas de Seguros, ARL y EPS, así como un modelo de datos particular para la póliza de salud. En las tablas de Seguros, los insumos principales son las vistas MDB_SEGUROS_COLOMBIA.V_EVENTO_SINIESTRO_COBERTURA (para información bruta) y MDB_SEGUROS_COLOMBIA.V_EVENTO_REASEGURO_SINI_COB (para información cedida), las cuales traen un registro para cada movimiento que se hace por siniestro, sea de pago o de reserva de aviso.

El conteo de siniestros se realiza considerando la primera fecha de movimiento, sea de pago o de aviso. En ese orden de ideas, para el conteo de pagos se utiliza la primera fecha de pago, mientras que para el conteo incurrido se utiliza la fecha mínima entre el primer pago y el primer movimiento de aviso. Para el caso del conteo de desistidos, se consideran los siniestros que tuvieron reserva de aviso en algún momento del tiempo, pero para los cuales nunca se realizó un pago. La fecha de desistimiento se considera como la fecha en la que se libera completamente la reserva de aviso. Cabe destacar que el conteo de siniestros es el mismo para el bruto y el retenido.

### Estructura siniestros

| **Nombre de la columna** | **Descripción**                                                   |
| ------------------------ | ----------------------------------------------------------------- |
| codigo_op                | Código de la compañía (01 para Generales, 02 para Vida).          |
| codigo_ramo_op           | Código del ramo.                                                  |
| "columnas de aperturas"  | Por ejemplo: apertura_canal, apertura_amparo, tipo_vehiculo, etc. |
| atipico                  | Binaria: 1 si es un siniestro atípico, 0 si es típico.            |
| fecha_siniestro          | Fecha de ocurrencia del siniestro.                                |
| fecha_registro           | Fecha de movimiento (pago o aviso).                               |
| conteo_pago              | Conteo de siniestros pagados.                                     |
| conteo_incurrido         | Conteo de siniestros incurridos.                                  |
| conteo_desistido         | Conteo de siniestros desistidos.                                  |
| pago_bruto               | Valor del pago bruto.                                             |
| pago_retenido            | Valor del pago retenido.                                          |
| aviso_bruto              | Valor del aviso bruto (movimiento, no saldo).                     |
| aviso_retenido           | Valor del aviso retenido (movimiento, no saldo.)                  |

## Primas

Esta consulta recupera la información de la producción de la compañía. Esto es las primas tanto brutas como retenidas (emitidas y devengadas) por cada agrupación de reservas (según corresponda en cada negocio) con su respectiva fecha de registro. En el modelo de datos de Seguros, el insumo principal es la vista MDB_SEGUROS_COLOMBIA.V_RT_DETALLE_COBERTURA, la cual trae la información de todos los rubros del PyG que conforman el resultado técnico, desglosado a nivel de póliza certificado y amparo.

### Estructura primas

| **Nombre de la columna** | **Descripción**                                                        |
| ------------------------ | ---------------------------------------------------------------------- |
| codigo_op                | Código de la compañía (01 para Generales, 02 para Vida).               |
| codigo_ramo_op           | Código del ramo.                                                       |
| "columnas de aperturas"  | Pueden diferir de las columnas de aperturas de siniestros y expuestos. |
| fecha_registro           | Fecha de movimiento.                                                   |
| prima_bruta              | Valor de la prima bruta.                                               |
| prima_retenida           | Valor de la prima retenida.                                            |
| prima_bruta_devengada    | Valor de la prima bruta devengada.                                     |
| prima_retenida_devengada | Valor de la prima retenida devengada.                                  |

## Expuestos

Esta consulta recupera la información de los expuestos y vigentes por cada agrupación de reservas, según corresponda en cada negocio. Para ello, parte de una tabla de meses con días iniciales y días finales, y de una tabla con la información de las pólizas y amparos suscritos en el rango temporal deseado, con sus fechas de inicio y fin de vigencia. Estas dos tablas se comparan a través de un CROSS JOIN simplificado a través de un INNER JOIN, para encontrar la exposición de cada póliza-amparo en cada uno de los meses necesarios. Finalmente, la información se agrupa a nivel de apertura de reservas, para hallar la suma total de expuestos y vigentes en cada mes de la ventana temporal.

En el modelo de datos de Seguros, los insumos principales son las vistas MDB_SEGUROS_COLOMBIA.V_HIST_POLCERT_COBERTURA (si se necesita a nivel de amparo) y MDB_SEGUROS_COLOMBIA.V_POLIZA_CERTIFICADO (si no se necesita a nivel de amparo).

### Estructura expuestos

| **Nombre de la columna** | **Descripción**                                                     |
| ------------------------ | ------------------------------------------------------------------- |
| codigo_op                | Código de la compañía (01 para Generales, 02 para Vida).            |
| codigo_ramo_op           | Código del ramo.                                                    |
| "columnas de aperturas"  | Pueden diferir de las columnas de aperturas de siniestros y primas. |
| fecha_registro           | Fecha de exposición.                                                |
| expuestos                | Número de expuestos del periodo.                                    |
| vigentes                 | Número de vigentes del periodo.                                     |

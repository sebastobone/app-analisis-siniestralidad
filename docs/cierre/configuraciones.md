<!--markdownlint-disable MD007-->
<!--markdownlint-disable MD024-->

# Configuración de los cierres contables

Los [archivos de segmentación](https://github.com/sebastobone/app-analisis-siniestralidad/tree/main/data) y los [_queries_](https://github.com/sebastobone/app-analisis-siniestralidad/tree/main/data/queries) incluidos por defecto en el repositorio corresponden a la configuración utilizada por cada negocio para su proceso de cierre contable mensual.

Estas configuraciones constituyen la **base estandarizada** de la aplicación y siempre están validadas según las [pruebas de funcionamiento](../pruebas.md).

## Lógica de consultas

A continuación, se describe la lógica general de las consultas contenidas en el repositorio para el cierre contable, así como los modelos de datos en Teradata sobre los que se apoyan.

!!! note "Nota"
    Pueden existir [particularidades](particularidades.md) en el entendimiento de cada negocio que modifiquen la lógica de sus consultas.

### Siniestros

#### Modelos de datos

Existen diferentes modelos de datos para las áreas de Seguros, ARL y Salud, así como un modelo de datos particular para la póliza de salud.

- **Seguros**
    - `MDB_SEGUROS_COLOMBIA.V_EVENTO_SINIESTRO_COBERTURA`: registros de movimientos brutos de pago y reservas de aviso para todos los siniestros de Vida y Generales (excluye ARL).
    - `MDB_SEGUROS_COLOMBIA.V_EVENTO_REASEGURO_SINI_COB`: registros de movimientos cedidos de pago y reservas de aviso para todos los siniestros de Vida y Generales (excluye ARL).
- **ARL**
    - `MDB_ARP_COLOMBIA.VE_ARL_CONCEPTO_EXPEDIENTE`: pagos por **prestaciones asistenciales**.
    - `MDB_ARP_COLOMBIA.VE_ARL_IT`: pagos por **incapacidad temporal (IT)**.
    - `MDB_ARP_COLOMBIA.VE_ARL_IPP`: pagos por **incapacidad permanente parcial (IPP)**.
    - `MDB_ARP_COLOMBIA.VE_ARL_AUXILIO_FUNERARIO`: pagos por **auxilio funerario (AF)**.
    - `MDB_ARP_COLOMBIA.VE_ARL_PENSION`: fechas de ocurrencia de siniestros asociados a **pensiones de invalidez o sobrevivencia**.
    - `MDB_ARP_COLOMBIA.VE_ARL_RESERVAS_TIPOLOGIA`: saldos de reservas de aviso en todas las prestaciones.
- **Salud**
    - `MDB_SALUD_COLOMBIA.VS_EVENTO_SIN_ORDEN_PAGO`: permite cruzar con la tabla de autorizaciones para traer la **fecha de autorización**.

#### Reglas de conteo de siniestros

- **Pagos**: se toma como referencia la **primera fecha de pago**.
- **Incurridos**: se utiliza la **fecha mínima entre el primer pago y el primer movimiento de aviso**.
- **Desistidos**: siniestros con reserva de aviso en algún momento, pero sin pagos asociados. La fecha de desistimiento corresponde a la **liberación completa de la reserva de aviso**.

!!! note "Nota"
    El conteo de siniestros es el mismo para el bruto y el retenido.

### Primas

#### Modelos de datos

En el modelo de datos de Seguros, el insumo principal es la vista:

- `MDB_SEGUROS_COLOMBIA.V_RT_DETALLE_COBERTURA`; contiene la información de todos los rubros del PyG que conforman el resultado técnico, desglosado a nivel de póliza, certificado y amparo.

### Expuestos

#### Lógica

El cálculo de expuestos sigue una lógica especial:

1. Construcción de una tabla de meses con **días iniciales y finales**.
2. Extracción de pólizas y amparos vigentes en el rango temporal deseado (con fechas de inicio y fin de vigencia).
3. Cruce de ambas tablas mediante un **INNER JOIN que emula un CROSS JOIN filtrado**, asignando la exposición de cada póliza-amparo a cada mes.
4. Agrupación por mes para obtener la suma total de **expuestos** y **vigentes**.

#### Modelos de datos

En el modelo de datos de Seguros, los insumos principales son las vistas:

- `MDB_SEGUROS_COLOMBIA.V_HIST_POLCERT_COBERTURA`: información a nivel de póliza-certificado-amparo.
- `MDB_SEGUROS_COLOMBIA.V_POLIZA_CERTIFICADO`: información a nivel de póliza y certificado (sin desglose por amparo).

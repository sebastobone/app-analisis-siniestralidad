# Fuentes de información y permisos necesarios

| **Información**                                   | **Negocio** | **Origen**  | **¿Requiere permiso?** |
|---------------------------------------------------|-------------|-------------|------------------------|
| Siniestros históricos                             | Todos       | Teradata    | Sí                     |
| Movimientos de pagos y aviso                      | Todos       | Teradata    | Sí                     |
| Primas por periodo de exposición                  | Todos       | Teradata    | Sí                     |
| Expuestos por periodo de exposición               | Todos       | Teradata    | Sí                     |
| Fechas de ocurrencia de pensiones                 | ARL         | Teradata    | Sí                     |
| Retroactivo de pensiones                          | ARL         | Diógenes    | Sí                     |
| Primera reserva matemática de pensiones           | ARL         | Osiris      | Sí                     |
| Estados financieros                               | Todos       | SAP         | Sí                     |

## Observaciones

1. Para realizar los procesos de extracción de información es necesario estar siempre conectado a la VPN.

2. Para extraer la información se debe tener acceso a las siguientes bases de datos:
    - **MDB_SEGUROS_COLOMBIA**: Pagos y reservas de Movilidad, Vida, Salud, Empresariales.
    - **MDB_ARP_COLOMBIA**: Pagos y reservas de ARL.
    - **MDB_SALUD_COLOMBIA**: Pagos y reservas de Salud.

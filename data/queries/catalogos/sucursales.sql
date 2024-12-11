SELECT
    sucu.sucursal_id
    , sucu.nombre_sucursal
    , canal.canal_comercial_id
    , canal.nombre_canal_comercial
    , gcanal.grupo_canal_comercial_id
    , gcanal.nombre_grupo_canal_comercial
FROM mdb_seguros_colombia.v_sucursal AS sucu
INNER JOIN
    mdb_seguros_colombia.v_canal_comercial AS canal
    ON sucu.canal_comercial_id = canal.canal_comercial_id
INNER JOIN
    mdb_seguros_colombia.v_grupo_canal_comercial AS gcanal
    ON canal.grupo_canal_comercial_id = gcanal.grupo_canal_comercial_id

ORDER BY sucu.sucursal_id

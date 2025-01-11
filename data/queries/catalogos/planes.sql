SELECT
    pind.plan_individual_id
    , pind.nombre_tecnico
    , pro.producto_id
    , pro.producto_desc
    , ramo.ramo_id
    , ramo.ramo_desc
    , cia.compania_id
    , cia.codigo_op
    , TRIM(ramo.codigo_ramo_op) AS codigo_ramo_op
FROM mdb_seguros_colombia.v_plan_individual_mstr AS pind
INNER JOIN
    mdb_seguros_colombia.v_producto AS pro
    ON pind.producto_id = pro.producto_id
INNER JOIN mdb_seguros_colombia.v_ramo AS ramo ON pro.ramo_id = ramo.ramo_id
INNER JOIN
    mdb_seguros_colombia.v_compania AS cia
    ON pro.compania_id = cia.compania_id
ORDER BY pind.plan_individual_id

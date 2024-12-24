EXPLAIN
WITH base2 AS (
    SELECT
        rtdc.mes_id
        , CASE
            WHEN
                rtdc.ramo_id = 78
                AND rtdc.amparo_id NOT IN (18647, 641, 930, 64082, 61296, -1)
                THEN 'AAV'
            ELSE ramo.codigo_ramo_op
        END AS codigo_ramo_op
        , pro.compania_id
        , rtdc.ramo_id
        , rtdc.poliza_id
        , sucu.sucursal_id
        , sucu.canal_comercial_id
        ,rtdc.valor_indicador

    FROM mdb_seguros_colombia.v_rt_detalle_cobertura AS rtdc
    INNER JOIN
        mdb_seguros_colombia.v_rt_nivel_indicador_cinco AS n5
        ON (rtdc.nivel_indicador_cinco_id = n5.nivel_indicador_cinco_id)
    INNER JOIN
        mdb_seguros_colombia.v_plan_individual AS pind
        ON (rtdc.plan_individual_id = pind.plan_individual_id)
    INNER JOIN
        mdb_seguros_colombia.v_producto AS pro
        ON (pind.producto_id = pro.producto_id)
    INNER JOIN
        mdb_seguros_colombia.v_ramo AS ramo
        ON (rtdc.ramo_id = ramo.ramo_id)
    INNER JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON (rtdc.poliza_id = poli.poliza_id)
    INNER JOIN
        mdb_seguros_colombia.v_sucursal AS sucu
        ON (poli.sucursal_id = sucu.sucursal_id)

    WHERE rtdc.ramo_id IN (54835, 274, 78, 57074, 140, 107, 271, 297, 204)
    AND rtdc.mes_id BETWEEN 201401 AND 202411
    AND pro.compania_id IN (3, 4)
    AND n5.nivel_indicador_uno_id IN (1, 2, 5)

    UNION ALL

    SELECT
        rtdc.mes_id
        , CASE
            WHEN
                rtdc.ramo_id = 78
                AND rtdc.amparo_id NOT IN (18647, 641, 930, 64082, 61296, -1)
                THEN 'AAV'
            ELSE ramo.codigo_ramo_op
        END AS codigo_ramo_op
        , rtdc.ramo_id
        , rtdc.compania_origen_id AS compania_id
        , rtdc.poliza_id
        , rtdc.sucursal_id
        , sucu.canal_comercial_id
        , rtdc.valor_indicador

    FROM mdb_seguros_colombia.v_rt_ramo_sucursal AS rtdc
    INNER JOIN
        mdb_seguros_colombia.v_rt_nivel_indicador_cinco AS n5
        ON (rtdc.nivel_indicador_cinco_id = n5.nivel_indicador_cinco_id)
    INNER JOIN
        mdb_seguros_colombia.v_ramo AS ramo
        ON (rtdc.ramo_id = ramo.ramo_id)
    INNER JOIN
        mdb_seguros_colombia.v_sucursal AS sucu
        ON (rtdc.sucursal_id = sucu.sucursal_id)

    WHERE rtdc.ramo_id IN (54835, 274, 78, 57074, 140, 107, 271, 297, 204)
    AND rtdc.mes_id BETWEEN 201401 AND 202411
    AND rtdc.compania_origen_id IN (3, 4)
    AND n5.nivel_indicador_uno_id IN (1, 2, 5)
)


SELECT
    base.mes_id
    , cia.codigo_op
    , base.codigo_ramo_op
    , SUM(valor_indicador)

FROM base2 AS base
INNER JOIN mdb_seguros_colombia.v_compania AS cia on base.compania_id = cia.compania_id

GROUP BY 1, 2, 3

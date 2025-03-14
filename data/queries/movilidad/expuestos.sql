CREATE MULTISET VOLATILE TABLE fechas AS (
    SELECT
        mes_id
        , MIN(dia_dt) AS primer_dia_mes
        , MAX(dia_dt) AS ultimo_dia_mes
        , CAST(
            (ultimo_dia_mes - primer_dia_mes + 1) * 1.00 AS DECIMAL(18, 0)) AS
        num_dias_mes
    FROM mdb_seguros_colombia.v_dia
    WHERE
        mes_id BETWEEN CAST('{mes_primera_ocurrencia}' AS INTEGER)
        AND CAST('{mes_corte}' AS INTEGER)
    GROUP BY 1
) WITH DATA PRIMARY INDEX (mes_id) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON fechas INDEX (mes_id);  -- noqa:


CREATE MULTISET VOLATILE TABLE base_expuestos AS
(
    SELECT
        pc.poliza_certificado_id
        , '040' AS codigo_ramo_op
        , CASE
            WHEN
                fas.clase_tarifa_cd IN (3, 4, 5, 6)
                AND pol.sucursal_id IN (21170919, 20056181, 52915901)
                THEN 'MOTOS SUFI'
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) THEN 'MOTOS RESTO'
            WHEN
                vpc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                THEN 'RC'
            WHEN vpc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
            ELSE 'PARCIALES'
        END AS cobertura_general_desc
        , pc.fecha_cancelacion
        , MIN(vpc.fecha_inclusion_cobertura) AS fecha_inclusion_cobertura
        , MAX(vpc.fecha_exclusion_cobertura) AS fecha_exclusion_cobertura

    FROM mdb_seguros_colombia.v_hist_polcert_cobertura AS vpc
    LEFT JOIN mdb_seguros_colombia.v_poliza_certificado AS pc
        ON
            vpc.poliza_certificado_id = pc.poliza_certificado_id
            AND vpc.plan_individual_id = pc.plan_individual_id
    LEFT JOIN mdb_seguros_colombia.v_plan_individual AS plan
        ON vpc.plan_individual_id = plan.plan_individual_id
    LEFT JOIN mdb_seguros_colombia.v_producto AS pro
        ON plan.producto_id = pro.producto_id
    LEFT JOIN mdb_seguros_colombia.v_poliza AS pol
        ON pc.poliza_id = pol.poliza_id
    LEFT JOIN mdb_seguros_colombia.v_vehiculo AS vehi
        ON pc.bien_id = vehi.bien_id
    LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas
        ON vehi.fasecolda_cd = fas.fasecolda_cd

    WHERE
        pro.ramo_id = 168
        AND pro.compania_id = 4

    GROUP BY 1, 2, 3, 4
    HAVING
        MAX(vpc.fecha_exclusion_cobertura)
        >= (DATE '{fecha_primera_ocurrencia}')

) WITH DATA PRIMARY INDEX (
    poliza_certificado_id
    , cobertura_general_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE expuestos AS (
    SELECT
        primer_dia_mes
        , codigo_ramo_op
        , cobertura_general_desc
        , SUM(expuestos) AS expuestos
        , SUM(vigentes) AS vigentes
    FROM (
        SELECT
            fechas.primer_dia_mes
            , vpc.codigo_ramo_op
            , vpc.cobertura_general_desc
            , GREATEST(
                vpc.fecha_inclusion_cobertura
                , fechas.primer_dia_mes
            ) AS fecha_inicio
            , LEAST(
                vpc.fecha_exclusion_cobertura
                , fechas.ultimo_dia_mes
                , COALESCE(vpc.fecha_cancelacion, (DATE '3000-01-01'))
            ) AS fecha_fin
            , SUM(
                CAST(fecha_fin - fecha_inicio + 1 AS FLOAT)
                / fechas.num_dias_mes
            ) AS expuestos
            , SUM(1) AS vigentes
        FROM fechas
        INNER JOIN base_expuestos AS vpc
            ON
                fechas.ultimo_dia_mes >= vpc.fecha_inclusion_cobertura
                AND COALESCE(
                    vpc.fecha_cancelacion
                    , vpc.fecha_exclusion_cobertura
                    , (DATE '3000-01-01')
                ) >= fechas.primer_dia_mes
        GROUP BY 1, 2, 3, 4, 5
    ) AS base

    GROUP BY 1, 2, 3

) WITH DATA PRIMARY INDEX (
    primer_dia_mes
    , codigo_ramo_op
    , cobertura_general_desc
) ON COMMIT PRESERVE ROWS;



SELECT
    '01' AS codigo_op
    , '040' AS codigo_ramo_op
    , cobertura_general_desc
    , primer_dia_mes AS fecha_registro
    , SUM(expuestos) AS expuestos
    , SUM(vigentes) AS vigentes
FROM expuestos
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
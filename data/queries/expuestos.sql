CREATE MULTISET VOLATILE TABLE polizas
(
    ramo_id INTEGER NOT NULL
    , codigo_ramo_op VARCHAR(3) NOT NULL
    , numero_poliza VARCHAR(20) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
) PRIMARY INDEX (codigo_ramo_op, numero_poliza) ON COMMIT PRESERVE ROWS;
INSERT INTO POLIZAS VALUES (?, ?, ?, ?); -- noqa: 


CREATE MULTISET VOLATILE TABLE sucursales
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(2) NOT NULL
    , codigo_ramo_op VARCHAR(3) NOT NULL
    , sucursal_id INTEGER NOT NULL
    , nombre_sucursal VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
) PRIMARY INDEX (
    codigo_op, codigo_ramo_op, sucursal_id
) ON COMMIT PRESERVE ROWS;
INSERT INTO SUCURSALES VALUES (?, ?, ?, ?, ?, ?); -- noqa: 


CREATE MULTISET VOLATILE TABLE amparos
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(2) NOT NULL
    , codigo_ramo_op VARCHAR(3) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
    , amparo_id INTEGER NOT NULL
    , amparo_desc VARCHAR(100) NOT NULL
    , apertura_amparo_desc VARCHAR(100) NOT NULL
) PRIMARY INDEX (amparo_desc) ON COMMIT PRESERVE ROWS;
INSERT INTO AMPAROS VALUES (?, ?, ?, ?, ?, ?, ?); -- noqa: 


CREATE MULTISET VOLATILE TABLE fechas AS
(
    SELECT DISTINCT
        mes_id
        , MIN(dia_dt) OVER (PARTITION BY mes_id) AS primer_dia_mes
        , MAX(dia_dt) OVER (PARTITION BY mes_id) AS ultimo_dia_mes
        , CAST(ultimo_dia_mes - primer_dia_mes + 1 AS FLOAT)
            AS num_dias_mes
    FROM mdb_seguros_colombia.v_dia
    WHERE mes_id BETWEEN '{mes_primera_ocurrencia}' AND '{mes_corte}'
) WITH DATA PRIMARY INDEX (mes_id) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE base_expuestos AS
(
    SELECT
        pc.poliza_certificado_id
        , CASE
            WHEN
                pro.ramo_id = 78
                AND vpc.amparo_id NOT IN (18647, 641, 930, 64082, 61296)
                THEN 'AAV'
            ELSE ramo.codigo_ramo_op
        END AS codigo_ramo_aux
        , CASE
            WHEN codigo_ramo_aux = 'AAV'
                THEN 'AMPAROS ADICIONALES DE VIDA'
            ELSE ramo.ramo_desc
        END AS ramo_desc
        , cia.codigo_op
        , COALESCE(
            p.apertura_canal_desc
            , COALESCE(
                s.apertura_canal_desc
                , CASE
                    WHEN
                        pro.ramo_id IN (78, 274)
                        AND pro.compania_id = 3
                        THEN 'NO BANCA'
                    ELSE 'RESTO'
                END
            )
        ) AS apertura_canal_aux
        , COALESCE(amparo.apertura_amparo_desc, 'RESTO') AS apertura_amparo_desc
        , pc.fecha_cancelacion
        , MIN(vpc.fecha_inclusion_cobertura) AS fecha_inclusion_cobertura
        , MAX(vpc.fecha_exclusion_cobertura) AS fecha_exclusion_cobertura

    FROM mdb_seguros_colombia.v_hist_polcert_cobertura AS vpc
    LEFT JOIN
        mdb_seguros_colombia.v_poliza_certificado AS pc
        ON
            (
                vpc.poliza_certificado_id = pc.poliza_certificado_id
                AND vpc.plan_individual_id = pc.plan_individual_id
            )
    LEFT JOIN
        mdb_seguros_colombia.v_plan_individual AS pind
        ON (vpc.plan_individual_id = pind.plan_individual_id)
    LEFT JOIN
        mdb_seguros_colombia.v_producto AS pro
        ON (pind.producto_id = pro.producto_id)
    LEFT JOIN
        mdb_seguros_colombia.v_compania AS cia
        ON (pro.compania_id = cia.compania_id)
    LEFT JOIN
        mdb_seguros_colombia.v_ramo AS ramo
        ON (pro.ramo_id = ramo.ramo_id)
    LEFT JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON (pc.poliza_id = poli.poliza_id)
    LEFT JOIN
        polizas AS p
        ON
            (
                poli.numero_poliza = p.numero_poliza
                AND pro.ramo_id = p.ramo_id
            )
    LEFT JOIN sucursales AS s
        ON (
            codigo_ramo_aux = s.codigo_ramo_op
            AND poli.sucursal_id = s.sucursal_id
            AND pro.compania_id = s.compania_id
        )
    LEFT JOIN amparos AS amparo
        ON (
            codigo_ramo_aux = amparo.codigo_ramo_op
            AND apertura_canal_aux = amparo.apertura_canal_desc
            AND vpc.amparo_id = amparo.amparo_id
            AND pro.compania_id = amparo.compania_id
        )

    WHERE
        pro.ramo_id IN (54835, 57074, 78, 274, 140, 107, 271)
        AND pro.compania_id IN (3, 4)

    GROUP BY 1, 2, 3, 4, 5, 6, 7
    HAVING
        MAX(vpc.fecha_exclusion_cobertura)
        >= (DATE '{fecha_primera_ocurrencia}')

) WITH DATA PRIMARY INDEX (
    poliza_certificado_id
    , apertura_amparo_desc
    , fecha_inclusion_cobertura
    , fecha_exclusion_cobertura
    , fecha_cancelacion
) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE expuestos AS
(
    SELECT
        primer_dia_mes
        , codigo_op
        , codigo_ramo_op
        , ramo_desc
        , apertura_canal_desc
        , apertura_amparo_desc
        , SUM(expuestos) AS expuestos
        , SUM(vigentes) AS vigentes
    FROM (
        SELECT
            fechas.primer_dia_mes
            , vpc.codigo_op
            , vpc.codigo_ramo_aux AS codigo_ramo_op
            , vpc.ramo_desc
            , vpc.apertura_canal_aux AS apertura_canal_desc
            , vpc.apertura_amparo_desc
            , GREATEST(vpc.fecha_inclusion_cobertura, fechas.primer_dia_mes)
                AS fecha_inicio
            , LEAST(
                vpc.fecha_exclusion_cobertura
                , fechas.ultimo_dia_mes
                , COALESCE(vpc.fecha_cancelacion, (DATE '3000-01-01'))
            ) AS fecha_fin
            , SUM(
                CAST((fecha_fin - fecha_inicio + 1) AS FLOAT)
                / fechas.num_dias_mes
            ) AS expuestos
            , SUM(1) AS vigentes

        FROM fechas AS fechas
        INNER JOIN base_expuestos AS vpc
            ON (
                fechas.ultimo_dia_mes >= vpc.fecha_inclusion_cobertura
                AND COALESCE(vpc.fecha_cancelacion, (DATE '3000-01-01'))
                >= fechas.primer_dia_mes
                AND COALESCE(vpc.fecha_exclusion_cobertura, (DATE '3000-01-01'))
                >= fechas.primer_dia_mes
            )

        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    ) AS base

    GROUP BY 1, 2, 3, 4, 5, 6
) WITH DATA PRIMARY INDEX (
    primer_dia_mes, codigo_ramo_op, apertura_amparo_desc
) ON COMMIT PRESERVE ROWS;


SELECT
    base.codigo_op
    , base.codigo_ramo_op
    , base.ramo_desc
    , base.primer_dia_mes AS fecha_registro
    , COALESCE(base.apertura_canal_desc, '-1') AS apertura_canal_desc
    , COALESCE(base.apertura_amparo_desc, '-1') AS apertura_amparo_desc
    , ZEROIFNULL(SUM(base.expuestos)) AS expuestos
    , ZEROIFNULL(SUM(base.vigentes)) AS vigentes

FROM expuestos AS base

GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6

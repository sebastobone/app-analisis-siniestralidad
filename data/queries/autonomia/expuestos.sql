CREATE MULTISET VOLATILE TABLE canal_poliza
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , ramo_id INTEGER NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , poliza_id BIGINT NOT NULL
    , numero_poliza VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
) PRIMARY INDEX (
    poliza_id, codigo_ramo_op, compania_id
) ON COMMIT PRESERVE ROWS;
INSERT INTO canal_poliza VALUES (?, ?, ?, ?, ?, ?, ?);  -- noqa:
COLLECT STATISTICS ON canal_poliza INDEX (poliza_id, codigo_ramo_op, compania_id);  -- noqa:


CREATE MULTISET VOLATILE TABLE canal_canal
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , canal_comercial_id BIGINT NOT NULL
    , codigo_canal_comercial_op VARCHAR(20) NOT NULL
    , nombre_canal_comercial VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
) PRIMARY INDEX (canal_comercial_id, codigo_ramo_op, compania_id) ON COMMIT PRESERVE ROWS;
INSERT INTO canal_canal VALUES (?, ?, ?, ?, ?, ?, ?);  -- noqa:
COLLECT STATISTICS ON canal_canal INDEX (canal_comercial_id, codigo_ramo_op, compania_id);  -- noqa:


CREATE MULTISET VOLATILE TABLE canal_sucursal
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , sucursal_id BIGINT NOT NULL
    , codigo_sucural_op VARCHAR(10) NOT NULL
    , nombre_sucursal VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
) PRIMARY INDEX (sucursal_id, codigo_ramo_op, compania_id) ON COMMIT PRESERVE ROWS;
INSERT INTO canal_sucursal VALUES (?, ?, ?, ?, ?, ?, ?);  -- noqa:
COLLECT STATISTICS ON canal_sucursal INDEX (sucursal_id, codigo_ramo_op, compania_id);  -- noqa:


CREATE MULTISET VOLATILE TABLE amparos
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
    , amparo_id BIGINT NOT NULL
    , amparo_desc VARCHAR(100) NOT NULL
    , apertura_amparo_desc VARCHAR(100) NOT NULL
) PRIMARY INDEX (
    amparo_id, codigo_ramo_op, compania_id, apertura_canal_desc
) ON COMMIT PRESERVE ROWS;
INSERT INTO AMPAROS VALUES (?, ?, ?, ?, ?, ?, ?);  -- noqa:
COLLECT STATISTICS ON amparos INDEX (amparo_id, codigo_ramo_op, compania_id, apertura_canal_desc);  -- noqa:


CREATE MULTISET VOLATILE TABLE fechas AS
(
    SELECT
        mes_id
        , MIN(dia_dt) AS primer_dia_mes
        , MAX(dia_dt) AS ultimo_dia_mes
        , CAST(ultimo_dia_mes - primer_dia_mes + 1 AS FLOAT)
            AS num_dias_mes
    FROM mdb_seguros_colombia.v_dia
    WHERE
        mes_id BETWEEN CAST('{mes_primera_ocurrencia}' AS INTEGER) AND CAST(
            '{mes_corte}' AS INTEGER
        )
    GROUP BY 1
) WITH DATA PRIMARY INDEX (mes_id) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON FECHAS COLUMN (Mes_Id);  -- noqa:



CREATE MULTISET VOLATILE TABLE base_expuestos
(
    poliza_certificado_id INTEGER NOT NULL
    , codigo_ramo_aux VARCHAR(3) NOT NULL
    , codigo_op VARCHAR(2) NOT NULL
    , apertura_canal_aux VARCHAR(100) NOT NULL
    , apertura_amparo_desc VARCHAR(100) NOT NULL
    , fecha_cancelacion DATE
    , fecha_inclusion_cobertura DATE
    , fecha_exclusion_cobertura DATE
) PRIMARY INDEX (
    poliza_certificado_id
    , apertura_amparo_desc
    , fecha_inclusion_cobertura
    , fecha_exclusion_cobertura
    , fecha_cancelacion
) ON COMMIT PRESERVE ROWS;

INSERT INTO BASE_EXPUESTOS
SELECT
    pc.poliza_certificado_id
    , CASE
        WHEN
            pro.ramo_id = 78
            AND vpc.amparo_id NOT IN (18647, 641, 930, 64082, 61296, -1)
            THEN 'AAV'
        ELSE ramo.codigo_ramo_op
    END AS codigo_ramo_aux
    , cia.codigo_op
    , COALESCE(
        p.apertura_canal_desc, c.apertura_canal_desc, s.apertura_canal_desc
        , CASE
            WHEN
                pro.ramo_id IN (78, 274) AND pro.compania_id = 3
                THEN 'Otros Banca'
            WHEN
                pro.ramo_id IN (274) AND pro.compania_id = 4
                THEN 'Otros'
            ELSE 'Resto'
        END
    ) AS apertura_canal_aux
    , COALESCE(amparo.apertura_amparo_desc, 'RESTO') AS apertura_amparo_desc
    , pc.fecha_cancelacion
    , MIN(vpc.fecha_inclusion_cobertura) AS fecha_inclusion_cobertura
    , MAX(vpc.fecha_exclusion_cobertura) AS fecha_exclusion_cobertura

FROM mdb_seguros_colombia.v_hist_polcert_cobertura AS vpc
LEFT JOIN
    mdb_seguros_colombia.v_poliza_certificado AS pc
    ON
        vpc.poliza_certificado_id = pc.poliza_certificado_id
        AND vpc.plan_individual_id = pc.plan_individual_id
LEFT JOIN
    mdb_seguros_colombia.v_plan_individual AS pind
    ON vpc.plan_individual_id = pind.plan_individual_id
LEFT JOIN
    mdb_seguros_colombia.v_producto AS pro
    ON pind.producto_id = pro.producto_id
LEFT JOIN
    mdb_seguros_colombia.v_compania AS cia
    ON pro.compania_id = cia.compania_id
LEFT JOIN mdb_seguros_colombia.v_ramo AS ramo ON pro.ramo_id = ramo.ramo_id
LEFT JOIN
    mdb_seguros_colombia.v_poliza AS poli
    ON pc.poliza_id = poli.poliza_id
LEFT JOIN
    mdb_seguros_colombia.v_amparo AS ampa
    ON vpc.amparo_id = ampa.amparo_id
LEFT JOIN
    mdb_seguros_colombia.v_sucursal AS sucu
    ON poli.sucursal_id = sucu.sucursal_id
INNER JOIN
    mdb_seguros_colombia.v_canal_comercial AS canal
    ON sucu.canal_comercial_id = canal.canal_comercial_id
LEFT JOIN
    canal_poliza AS p
    ON
        vpc.poliza_id = p.poliza_id
        AND codigo_ramo_aux = p.codigo_ramo_op
        AND cia.compania_id = p.compania_id
LEFT JOIN
    canal_canal AS c
    ON
        codigo_ramo_aux = c.codigo_ramo_op
        AND sucu.canal_comercial_id = c.canal_comercial_id
        AND cia.compania_id = c.compania_id
LEFT JOIN
    canal_sucursal AS s
    ON
        codigo_ramo_aux = s.codigo_ramo_op
        AND sucu.sucursal_id = s.sucursal_id
        AND cia.compania_id = s.compania_id
LEFT JOIN
    amparos AS amparo
    ON
        codigo_ramo_aux = amparo.codigo_ramo_op
        AND vpc.amparo_id = amparo.amparo_id
        AND apertura_canal_aux = amparo.apertura_canal_desc
        AND cia.compania_id = amparo.compania_id

WHERE pro.ramo_id IN (54835, 78, 274, 57074, 140, 107, 271, 297, 204)
    AND pro.compania_id IN (3, 4)
    AND vpc.fecha_exclusion_cobertura >= (DATE '{fecha_primera_ocurrencia}')

GROUP BY 1, 2, 3, 4, 5, 6
;

COLLECT STATISTICS ON base_expuestos INDEX (
    poliza_certificado_id
    , apertura_amparo_desc
    , fecha_inclusion_cobertura
    , fecha_exclusion_cobertura
    , fecha_cancelacion
);



CREATE MULTISET VOLATILE TABLE expuestos AS
(
    WITH base AS (
        SELECT
            fechas.primer_dia_mes
            , vpc.codigo_op
            , vpc.codigo_ramo_aux AS codigo_ramo_op
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
                CAST(fecha_fin - fecha_inicio + 1 AS FLOAT)
                / fechas.num_dias_mes
            ) AS expuestos
            , SUM(1) AS vigentes

        FROM fechas
        INNER JOIN base_expuestos AS vpc
            ON
                fechas.ultimo_dia_mes >= vpc.fecha_inclusion_cobertura
                AND COALESCE(vpc.fecha_cancelacion, (DATE '3000-01-01'))
                >= fechas.primer_dia_mes
                AND COALESCE(vpc.fecha_exclusion_cobertura, (DATE '3000-01-01'))
                >= fechas.primer_dia_mes

        GROUP BY 1, 2, 3, 4, 5, 6, 7
    )

    SELECT
        primer_dia_mes
        , codigo_op
        , codigo_ramo_op
        , apertura_canal_desc
        , apertura_amparo_desc
        , SUM(expuestos) AS expuestos
        , SUM(vigentes) AS vigentes
    FROM base
    GROUP BY 1, 2, 3, 4, 5

) WITH DATA PRIMARY INDEX (
    primer_dia_mes, codigo_ramo_op, apertura_amparo_desc
) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON EXPUESTOS INDEX (
    primer_dia_mes, codigo_ramo_op, apertura_amparo_desc
);


SELECT
    base.codigo_op
    , base.codigo_ramo_op
    , CASE
        WHEN base.codigo_ramo_op = 'AAV' THEN 'ANEXOS VI'
        ELSE ramo.ramo_desc
    END AS ramo_desc
    , base.primer_dia_mes AS fecha_registro
    , COALESCE(base.apertura_canal_desc, '-1') AS apertura_canal_desc
    , COALESCE(base.apertura_amparo_desc, '-1') AS apertura_amparo_desc
    , ZEROIFNULL(SUM(base.expuestos)) AS expuestos
    , ZEROIFNULL(SUM(base.vigentes)) AS vigentes

FROM expuestos AS base
LEFT JOIN
    mdb_seguros_colombia.v_ramo AS ramo
    ON base.codigo_ramo_op = ramo.codigo_ramo_op

GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6
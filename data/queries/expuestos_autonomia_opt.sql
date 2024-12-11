CREATE MULTISET VOLATILE TABLE canal_poliza
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , ramo_id INTEGER NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , poliza_id BIGINT NOT NULL
    , numero_poliza VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
    , apertura_canal_cd VARCHAR(100) NOT NULL
) PRIMARY INDEX (numero_poliza) ON COMMIT PRESERVE ROWS;
INSERT INTO CANAL_POLIZA VALUES (?, ?, ?, ?, ?, ?, ?, ?);  -- noqa:


CREATE MULTISET VOLATILE TABLE canal_canal
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , canal_comercial_id BIGINT NOT NULL
    , codigo_canal_comercial_op VARCHAR(20) NOT NULL
    , nombre_canal_comercial VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
    , apertura_canal_cd VARCHAR(100) NOT NULL
) PRIMARY INDEX (canal_comercial_id) ON COMMIT PRESERVE ROWS;
INSERT INTO CANAL_CANAL VALUES (?, ?, ?, ?, ?, ?, ?, ?);  -- noqa:


CREATE MULTISET VOLATILE TABLE canal_sucursal
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , sucursal_id BIGINT NOT NULL
    , codigo_sucural_op VARCHAR(10) NOT NULL
    , nombre_sucursal VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
    , apertura_canal_cd VARCHAR(100) NOT NULL
) PRIMARY INDEX (sucursal_id) ON COMMIT PRESERVE ROWS;
INSERT INTO CANAL_SUCURSAL VALUES (?, ?, ?, ?, ?, ?, ?, ?);  -- noqa:


CREATE MULTISET VOLATILE TABLE amparos
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
    , amparo_id BIGINT NOT NULL
    , amparo_desc VARCHAR(100) NOT NULL
    , apertura_amparo_desc VARCHAR(100) NOT NULL
) PRIMARY INDEX (amparo_desc) ON COMMIT PRESERVE ROWS;
INSERT INTO AMPAROS VALUES (?, ?, ?, ?, ?, ?, ?);  -- noqa:


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

EXPLAIN
WITH base AS (
    SELECT
        vpc.poliza_id
        , vpc.poliza_certificado_id
        , CASE
            WHEN
                pro.ramo_id = 78
                AND vpc.amparo_id NOT IN (18647, 641, 930, 64082, 61296, -1)
                THEN 'AAV'
            ELSE ramo.codigo_ramo_op
        END AS codigo_ramo_aux
        , pro.ramo_id
        , pro.compania_id
        , cia.codigo_op
        , sucu.sucursal_id
        , sucu.canal_comercial_id
        , vpc.amparo_id
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
        mdb_seguros_colombia.v_plan_individual AS plan
        ON (vpc.plan_individual_id = plan.plan_individual_id)
    LEFT JOIN
        mdb_seguros_colombia.v_producto AS pro
        ON (plan.producto_id = pro.producto_id)
    LEFT JOIN
        mdb_seguros_colombia.v_compania AS cia
        ON (pro.compania_id = cia.compania_id)
    LEFT JOIN mdb_seguros_colombia.v_ramo AS ramo ON (pro.ramo_id = ramo.ramo_id)
    LEFT JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON (pc.poliza_id = poli.poliza_id)
    LEFT JOIN
        mdb_seguros_colombia.v_sucursal AS sucu
        ON (poli.sucursal_id = sucu.sucursal_id)

    WHERE
        pro.ramo_id IN (54835, 78, 274, 57074, 140, 107, 271, 297, 204)
        AND pro.compania_id IN (3, 4)

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    HAVING
        MAX(vpc.fecha_exclusion_cobertura) >= (DATE '{fecha_primera_ocurrencia}')    
)

SELECT
    base.poliza_certificado_id
    , base.codigo_ramo_aux
    , base.codigo_op
    , COALESCE(
        p.apertura_canal_desc, c.apertura_canal_desc, s.apertura_canal_desc
        , CASE
            WHEN
                base.ramo_id IN (78, 274)
                AND base.compania_id = 3
                THEN 'Otros Banca'
            WHEN
                base.ramo_id = 78 AND base.compania_id = 4
                THEN 'Otros'
            ELSE 'Resto'
        END
    ) AS apertura_canal_aux
    , COALESCE(amparo.apertura_amparo_desc, 'RESTO') AS apertura_amparo_desc
    , base.fecha_cancelacion
    , MIN(base.fecha_inclusion_cobertura) AS fecha_inclusion_cobertura
    , MAX(base.fecha_exclusion_cobertura) AS fecha_exclusion_cobertura

FROM base
LEFT JOIN
    canal_poliza AS p
    ON
        base.poliza_id = p.poliza_id
        AND base.codigo_ramo_aux = p.codigo_ramo_op
        AND base.compania_id = p.compania_id
LEFT JOIN
    canal_canal AS c
    ON (
        base.codigo_ramo_aux = c.codigo_ramo_op
        AND base.canal_comercial_id = c.canal_comercial_id
        AND base.compania_id = c.compania_id
    )
LEFT JOIN
    canal_sucursal AS s
    ON (
        base.codigo_ramo_aux = s.codigo_ramo_op
        AND base.sucursal_id = s.sucursal_id
        AND base.compania_id = s.compania_id
    )
LEFT JOIN
    amparos AS amparo
    ON (
        base.codigo_ramo_aux = amparo.codigo_ramo_op
        AND base.amparo_id = amparo.amparo_id
        AND apertura_canal_aux = amparo.apertura_canal_desc
        AND base.compania_id = amparo.compania_id
    )

GROUP BY 1, 2, 3, 4, 5, 6
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
INSERT INTO canal_poliza VALUES (?, ?, ?, ?, ?, ?, ?, ?);  -- noqa:


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
INSERT INTO canal_canal VALUES (?, ?, ?, ?, ?, ?, ?, ?);  -- noqa:


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
INSERT INTO canal_sucursal VALUES (?, ?, ?, ?, ?, ?, ?, ?);  -- noqa:


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
INSERT INTO amparos VALUES (?, ?, ?, ?, ?, ?, ?);  -- noqa:


CREATE MULTISET VOLATILE TABLE atipicos
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
    , apertura_amparo_desc VARCHAR(100) NOT NULL
    , siniestro_id VARCHAR(100) NOT NULL
    , atipico INTEGER NOT NULL
) PRIMARY INDEX (siniestro_id) ON COMMIT PRESERVE ROWS;
INSERT INTO atipicos VALUES (?, ?, ?, ?, ?, ?, ?);  -- noqa:


-- CREATE MULTISET VOLATILE TABLE base_cedido
-- (
--     fecha_siniestro DATE
--     , fecha_registro DATE
--     , numero_poliza VARCHAR(20)
--     , cliente_id INTEGER
--     , nombre_tecnico VARCHAR(100)
--     , codigo_op VARCHAR(2)
--     , codigo_ramo_aux VARCHAR(3)
--     , siniestro_id VARCHAR(20)
--     , atipico SMALLINT NOT NULL
--     , tipo_estado_siniestro_cd VARCHAR(3)
--     , apertura_canal_aux VARCHAR(100)
--     , nombre_canal_comercial VARCHAR(100)
--     , nombre_sucursal VARCHAR(100)
--     , apertura_amparo_desc VARCHAR(100)
--     , pago_cedido FLOAT
--     , aviso_cedido FLOAT
-- ) PRIMARY INDEX (
--     fecha_siniestro
--     , fecha_registro
--     , numero_poliza
--     , cliente_id
--     , nombre_tecnico
--     , codigo_op
--     , codigo_ramo_aux
--     , siniestro_id
--     , atipico
--     , tipo_estado_siniestro_cd
--     , apertura_canal_aux
--     , nombre_canal_comercial
--     , nombre_sucursal
--     , apertura_amparo_desc
-- ) ON COMMIT PRESERVE ROWS;
-- COLLECT STATISTICS ON BASE_CEDIDO COLUMN (Fecha_Siniestro, Fecha_Registro, Numero_Poliza, Cliente_Id, Nombre_Tecnico, Codigo_Op, Codigo_Ramo_Aux, Siniestro_Id, Atipico, Tipo_Estado_Siniestro_Cd, Apertura_Canal_Aux, Nombre_Canal_Comercial, Nombre_Sucursal, Apertura_Amparo_Desc);  -- noqa:

-- INSERT INTO base_cedido
EXPLAIN

WITH base AS (
    SELECT
        esc.fecha_registro
        , poli.poliza_id
        , poli.numero_poliza
        , cli.cliente_id
        , pind.nombre_tecnico
        , cia.compania_id
        , cia.codigo_op
        , esc.ramo_id
        , ramo.codigo_ramo_op
        , esc.amparo_id
        , esc.siniestro_id
        , sini.tipo_estado_siniestro_cd
        , canal.canal_comercial_id
        , canal.nombre_canal_comercial
        , sucu.sucursal_id
        , sucu.nombre_sucursal
        , LEAST(sini.fecha_siniestro, esc.fecha_registro) AS fecha_siniestro
        , CASE
            WHEN
                esc.ramo_id = 78
                AND esc.amparo_id NOT IN (
                    930, 641, 64082, 61296, 18647, -1
                )
                THEN 'AAV'
            ELSE ramo.codigo_ramo_op
        END AS codigo_ramo_aux
        , SUM(
            CASE
                WHEN
                    esc.tipo_oper_siniestro_cd <> '130'
                    THEN esc.valor_siniestro * esc.valor_tasa
                ELSE 0
            END
        ) AS pago_bruto
        -- Aviso corregido
        , SUM(
            CASE
                WHEN
                    esc.tipo_oper_siniestro_cd = '130'
                    AND sini.fecha_siniestro >= (DATE '2010-01-01')
                    THEN esc.valor_siniestro * esc.valor_tasa
                ELSE 0
            END
        ) AS aviso_bruto

    FROM mdb_seguros_colombia.v_evento_siniestro_cobertura AS esc
    INNER JOIN
        mdb_seguros_colombia.v_plan_individual_mstr AS pind
        ON (esc.plan_individual_id = pind.plan_individual_id)
    INNER JOIN
        mdb_seguros_colombia.v_producto AS pro
        ON (pind.producto_id = pro.producto_id)
    INNER JOIN
        mdb_seguros_colombia.v_ramo AS ramo
        ON esc.ramo_id = ramo.ramo_id
    INNER JOIN
        mdb_seguros_colombia.v_siniestro AS sini
        ON (esc.siniestro_id = sini.siniestro_id)
    INNER JOIN
        mdb_seguros_colombia.v_compania AS cia
        ON (pro.compania_id = cia.compania_id)
    INNER JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON (esc.poliza_id = poli.poliza_id)
    INNER JOIN
        mdb_seguros_colombia.v_sucursal AS sucu
        ON (poli.sucursal_id = sucu.sucursal_id)
    INNER JOIN
        mdb_seguros_colombia.v_canal_comercial AS canal
        ON (sucu.canal_comercial_id = canal.canal_comercial_id)
    INNER JOIN
        mdb_seguros_colombia.v_poliza_certificado_etl AS pcetl
        ON (esc.poliza_certificado_id = pcetl.poliza_certificado_id)
    LEFT JOIN
        mdb_seguros_colombia.v_cliente AS cli
        ON (pcetl.asegurado_id = cli.cliente_id)

    WHERE
        esc.ramo_id IN (
            54835, 140, 107, 78, 274, 57074, 140, 107, 271, 297, 204
        )
        AND pro.compania_id IN (3, 4)
        AND esc.valor_siniestro <> 0
        AND esc.mes_id BETWEEN 201401 AND 202411

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
)

SELECT
    base.fecha_siniestro
    , base.fecha_registro
    , base.numero_poliza
    , base.cliente_id
    , base.nombre_tecnico
    , base.codigo_op
    , base.codigo_ramo_aux
    , base.siniestro_id
    , ZEROIFNULL(atip.atipico) AS atipico
    , base.tipo_estado_siniestro_cd
    , base.nombre_canal_comercial
    , base.nombre_sucursal
    , COALESCE(
        p.apertura_canal_desc, c.apertura_canal_desc, s.apertura_canal_desc
        , CASE
            WHEN
                base.ramo_id IN (78, 274) AND base.compania_id = 3
                THEN 'Otros Banca'
            WHEN
                base.ramo_id IN (274) AND base.compania_id = 4
                THEN 'Otros'
            ELSE 'Resto'
        END
    ) AS apertura_canal_aux
    , COALESCE(amparo.apertura_amparo_desc, 'RESTO') AS apertura_amparo_desc
    , SUM(base.pago_bruto) AS pago_bruto
    , SUM(base.aviso_bruto) AS aviso_bruto

FROM base
LEFT JOIN
    canal_poliza AS p
    ON
        (
            base.poliza_id = p.poliza_id
            AND base.codigo_ramo_aux = p.codigo_ramo_op
            AND base.compania_id = p.compania_id
        )
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
LEFT JOIN
    atipicos AS atip
    ON (
        base.compania_id = atip.compania_id
        AND base.codigo_ramo_aux = atip.codigo_ramo_op
        AND base.siniestro_id = atip.siniestro_id
        AND COALESCE(amparo.apertura_amparo_desc, 'RESTO')
        = atip.apertura_amparo_desc
    )

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14

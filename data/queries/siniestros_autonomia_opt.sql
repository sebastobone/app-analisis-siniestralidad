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


CREATE MULTISET VOLATILE TABLE incurridos_cedidos_atipicos
(
    fecha_aviso DATE NOT NULL
    , siniestro_id VARCHAR(100) NOT NULL
    , numero_poliza VARCHAR(100) NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , amparo_id INTEGER NOT NULL
    , amparo_desc VARCHAR(100) NOT NULL
    , sucursal_id INTEGER NOT NULL
    , nombre_sucursal VARCHAR(100) NOT NULL
    , canal_comercial_id INTEGER NOT NULL
    , nombre_canal_comercial VARCHAR(100) NOT NULL
    , pago_cedido FLOAT NOT NULL
    , aviso_cedido FLOAT NOT NULL
) PRIMARY INDEX (siniestro_id, amparo_id) ON COMMIT PRESERVE ROWS;
INSERT INTO incurridos_cedidos_atipicos VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);  -- noqa:


CREATE MULTISET VOLATILE TABLE cifras_sap
(
    codigo_op VARCHAR(2) NOT NULL
    , codigo_ramo_op VARCHAR(3) NOT NULL
    , mes_mov INTEGER NOT NULL
    , pago_cedido FLOAT NOT NULL
    , aviso_cedido FLOAT NOT NULL
) PRIMARY INDEX (codigo_op, codigo_ramo_op) ON COMMIT PRESERVE ROWS;
INSERT INTO cifras_sap VALUES (?, ?, ?, ?, ?);  -- noqa:




EXPLAIN
-- WITH base AS (
    SELECT
        ersc.fecha_registro
        , poli.poliza_id
        , poli.numero_poliza
        , pcetl.asegurado_id
        , pind.nombre_tecnico
        , cia.compania_id
        , cia.codigo_op
        , pro.ramo_id
        , ramo.codigo_ramo_op
        , ersc.amparo_id
        , ersc.siniestro_id
        , sini.tipo_estado_siniestro_cd
        , canal.canal_comercial_id
        , canal.nombre_canal_comercial
        , sucu.sucursal_id
        , sucu.nombre_sucursal
        , sini.fecha_siniestro
        , SUM(
            CASE
                WHEN
                    ers.numero_documento <> '-1'
                    THEN ersc.valor_siniestro_cedido * ersc.valor_tasa
                ELSE 0
            END
        ) AS pago_cedido
        --Aviso corregido
        , SUM(
            CASE
                WHEN
                    ers.numero_documento = '-1'
                    AND sini.fecha_siniestro >= (DATE '2010-01-01')
                    THEN ersc.valor_siniestro_cedido * ersc.valor_tasa
                ELSE 0
            END
        ) AS aviso_cedido

    FROM mdb_seguros_colombia.v_evento_reaseguro_sini_cob AS ersc
    INNER JOIN
        mdb_seguros_colombia.v_evento_reaseguro_sini AS ers
        ON (ersc.evento_id = ers.evento_id)
    INNER JOIN
        mdb_seguros_colombia.v_plan_individual_mstr AS pind
        ON (ersc.plan_individual_id = pind.plan_individual_id)
    INNER JOIN
        mdb_seguros_colombia.v_producto AS pro
        ON (pind.producto_id = pro.producto_id)
    INNER JOIN
        mdb_seguros_colombia.v_ramo AS ramo
        ON pro.ramo_id = ramo.ramo_id
    INNER JOIN
        mdb_seguros_colombia.v_siniestro AS sini
        ON (ersc.siniestro_id = sini.siniestro_id)
    INNER JOIN
        mdb_seguros_colombia.v_compania AS cia
        ON (pro.compania_id = cia.compania_id)
    INNER JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON (ersc.poliza_id = poli.poliza_id)
    INNER JOIN
        mdb_seguros_colombia.v_sucursal AS sucu
        ON (poli.sucursal_id = sucu.sucursal_id)
    INNER JOIN
        mdb_seguros_colombia.v_canal_comercial AS canal
        ON (sucu.canal_comercial_id = canal.canal_comercial_id)
    INNER JOIN
        mdb_seguros_colombia.v_poliza_certificado_etl AS pcetl
        ON (ersc.poliza_certificado_id = pcetl.poliza_certificado_id)

    WHERE
        pro.ramo_id IN (
            54835, 140, 107, 78, 274, 57074, 140, 107, 271, 297, 204
        )
        AND pro.compania_id IN (3, 4)
        AND ersc.valor_siniestro_cedido <> 0
        AND ersc.mes_id BETWEEN 201401 AND 202411

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
-- )

-- SELECT
--     base.fecha_siniestro
--     , base.fecha_registro
--     , base.numero_poliza
--     , base.asegurado_id
--     , base.nombre_tecnico
--     , base.codigo_op
--     , base.codigo_ramo_aux
--     , base.siniestro_id
--     , ZEROIFNULL(atip.atipico) AS atipico
--     , base.tipo_estado_siniestro_cd
--     , base.nombre_canal_comercial
--     , base.nombre_sucursal
--     , COALESCE(
--         p.apertura_canal_desc, c.apertura_canal_desc, s.apertura_canal_desc
--         , CASE
--             WHEN
--                 base.ramo_id IN (78, 274) AND base.compania_id = 3
--                 THEN 'Otros Banca'
--             WHEN
--                 base.ramo_id IN (274) AND base.compania_id = 4
--                 THEN 'Otros'
--             ELSE 'Resto'
--         END
--     ) AS apertura_canal_aux
--     , COALESCE(amparo.apertura_amparo_desc, 'RESTO') AS apertura_amparo_desc
--     , SUM(base.pago_cedido) AS pago_cedido
--     , SUM(base.aviso_cedido) AS aviso_cedido

-- FROM base
-- LEFT JOIN
--     canal_poliza AS p
--     ON
--         (
--             base.poliza_id = p.poliza_id
--             AND base.codigo_ramo_aux = p.codigo_ramo_op
--             AND base.compania_id = p.compania_id
--         )
-- LEFT JOIN
--     canal_canal AS c
--     ON (
--         base.codigo_ramo_aux = c.codigo_ramo_op
--         AND base.canal_comercial_id = c.canal_comercial_id
--         AND base.compania_id = c.compania_id
--     )
-- LEFT JOIN
--     canal_sucursal AS s
--     ON (
--         base.codigo_ramo_aux = s.codigo_ramo_op
--         AND base.sucursal_id = s.sucursal_id
--         AND base.compania_id = s.compania_id
--     )
-- LEFT JOIN
--     amparos AS amparo
--     ON (
--         base.codigo_ramo_aux = amparo.codigo_ramo_op
--         AND base.amparo_id = amparo.amparo_id
--         AND apertura_canal_aux = amparo.apertura_canal_desc
--         AND base.compania_id = amparo.compania_id
--     )
-- LEFT JOIN
--     atipicos AS atip
--     ON (
--         base.compania_id = atip.compania_id
--         AND base.codigo_ramo_aux = atip.codigo_ramo_op
--         AND base.siniestro_id = atip.siniestro_id
--         AND COALESCE(amparo.apertura_amparo_desc, 'RESTO')
--         = atip.apertura_amparo_desc
--     )

-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
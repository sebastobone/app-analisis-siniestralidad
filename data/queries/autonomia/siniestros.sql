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
    compania_id, codigo_ramo_op, amparo_id, apertura_canal_desc
) ON COMMIT PRESERVE ROWS;
INSERT INTO amparos VALUES (?, ?, ?, ?, ?, ?, ?);  -- noqa:
COLLECT STATISTICS ON amparos INDEX (compania_id, codigo_ramo_op, amparo_id, apertura_canal_desc);  -- noqa:


CREATE MULTISET VOLATILE TABLE atipicos
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
    , apertura_amparo_desc VARCHAR(100) NOT NULL
    , siniestro_id VARCHAR(100) NOT NULL
    , atipico INTEGER NOT NULL
) PRIMARY INDEX (
    compania_id, codigo_ramo_op, siniestro_id, apertura_amparo_desc
) ON COMMIT PRESERVE ROWS;
INSERT INTO atipicos VALUES (?, ?, ?, ?, ?, ?, ?);  -- noqa:
COLLECT STATISTICS ON amparos INDEX (compania_id, codigo_ramo_op, siniestro_id, apertura_amparo_desc);  -- noqa:


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



CREATE MULTISET VOLATILE TABLE base_cedido AS
(
    SELECT
        sini.fecha_siniestro
        , ersc.fecha_registro
        , poli.numero_poliza
        , pcetl.asegurado_id
        , plan.nombre_tecnico
        , cia.codigo_op
        , CASE
            WHEN
                esc.ramo_id = 78
                AND esc.amparo_id NOT IN (
                    930, 641, 64082, 61296, 18647, -1
                )
                THEN 'AAV'
            ELSE ramo.codigo_ramo_op
        END AS codigo_ramo_aux
        , ersc.siniestro_id
        , ZEROIFNULL(atip.atipico) AS atipico
        , sini.tipo_estado_siniestro_cd
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
        , canal.nombre_canal_comercial
        , sucu.nombre_sucursal
        , COALESCE(amparo.apertura_amparo_desc, 'RESTO') AS apertura_amparo_desc
        , SUM(
            CASE
                WHEN
                    ers.numero_documento <> '-1'
                    THEN ersc.valor_siniestro_cedido * ersc.valor_tasa
                ELSE 0
            END
        ) AS pago_cedido
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
    LEFT JOIN
        mdb_seguros_colombia.v_evento_reaseguro_sini AS ers
        ON ersc.evento_id = ers.evento_id
    LEFT JOIN
        mdb_seguros_colombia.v_plan_individual_mstr AS plan
        ON ersc.plan_individual_id = plan.plan_individual_id
    LEFT JOIN
        mdb_seguros_colombia.v_producto AS pro
        ON plan.producto_id = pro.producto_id
    LEFT JOIN
        mdb_seguros_colombia.v_ramo AS ramo
        ON pro.ramo_id = ramo.ramo_id
    LEFT JOIN
        mdb_seguros_colombia.v_siniestro AS sini
        ON ersc.siniestro_id = sini.siniestro_id
    LEFT JOIN
        mdb_seguros_colombia.v_compania AS cia
        ON pro.compania_id = cia.compania_id
    LEFT JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON ersc.poliza_id = poli.poliza_id
    LEFT JOIN
        mdb_seguros_colombia.v_sucursal AS sucu
        ON poli.sucursal_id = sucu.sucursal_id
    LEFT JOIN
        mdb_seguros_colombia.v_canal_comercial AS canal
        ON sucu.canal_comercial_id = canal.canal_comercial_id
    LEFT JOIN
        mdb_seguros_colombia.v_poliza_certificado_etl AS pcetl
        ON ersc.poliza_certificado_id = pcetl.poliza_certificado_id
    LEFT JOIN
        canal_poliza AS p
        ON
            ersc.poliza_id = p.poliza_id
            AND codigo_ramo_aux = p.codigo_ramo_op
            AND pro.compania_id = p.compania_id
    LEFT JOIN
        canal_canal AS c
        ON
            codigo_ramo_aux = c.codigo_ramo_op
            AND sucu.canal_comercial_id = c.canal_comercial_id
            AND pro.compania_id = c.compania_id
    LEFT JOIN
        canal_sucursal AS s
        ON
            codigo_ramo_aux = s.codigo_ramo_op
            AND poli.sucursal_id = s.sucursal_id
            AND pro.compania_id = s.compania_id
    LEFT JOIN
        amparos AS amparo
        ON
            codigo_ramo_aux = amparo.codigo_ramo_op
            AND ersc.amparo_id = amparo.amparo_id
            AND apertura_canal_aux = amparo.apertura_canal_desc
            AND pro.compania_id = amparo.compania_id
    LEFT JOIN
        atipicos AS atip
        ON
            pro.compania_id = atip.compania_id
            AND codigo_ramo_aux = atip.codigo_ramo_op
            AND ersc.siniestro_id = atip.siniestro_id
            AND COALESCE(amparo.apertura_amparo_desc, 'RESTO')
            = atip.apertura_amparo_desc

    WHERE
        pro.ramo_id IN (78, 274, 57074, 140, 107, 271, 297, 204)
        AND pro.compania_id IN (3, 4)
        AND ersc.mes_id BETWEEN CAST(
            '{mes_primera_ocurrencia}' AS INTEGER
        ) AND CAST('{mes_corte}' AS INTEGER)

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
    HAVING NOT (pago_cedido = 0 AND aviso_cedido = 0)

) PRIMARY INDEX (
    fecha_siniestro
    , fecha_registro
    , numero_poliza
    , asegurado_id
    , codigo_op
    , codigo_ramo_aux
    , siniestro_id
    , apertura_amparo_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE base_bruto AS
(
    SELECT
        sini.fecha_siniestro
        , esc.fecha_registro
        , poli.numero_poliza
        , pcetl.asegurado_id
        , plan.nombre_tecnico
        , cia.codigo_op
        , CASE
            WHEN
                esc.ramo_id = 78
                AND esc.amparo_id NOT IN (
                    930, 641, 64082, 61296, 18647, -1
                )
                THEN 'AAV'
            ELSE ramo.codigo_ramo_op
        END AS codigo_ramo_aux
        , esc.siniestro_id
        , ZEROIFNULL(atip.atipico) AS atipico
        , sini.tipo_estado_siniestro_cd
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
        , canal.nombre_canal_comercial
        , sucu.nombre_sucursal
        , COALESCE(amparo.apertura_amparo_desc, 'RESTO') AS apertura_amparo_desc
        , SUM(
            CASE
                WHEN
                    esc.tipo_oper_siniestro_cd <> '130'
                    THEN esc.valor_siniestro * esc.valor_tasa
                ELSE 0
            END
        ) AS pago_bruto
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
    LEFT JOIN
        mdb_seguros_colombia.v_siniestro AS sini
        ON esc.siniestro_id = sini.siniestro_id
    LEFT JOIN mdb_seguros_colombia.v_ramo AS ramo ON esc.ramo_id = ramo.ramo_id
    LEFT JOIN
        mdb_seguros_colombia.v_plan_individual AS plan
        ON esc.plan_individual_id = plan.plan_individual_id
    LEFT JOIN
        mdb_seguros_colombia.v_producto AS pro
        ON plan.producto_id = pro.producto_id
    LEFT JOIN
        mdb_seguros_colombia.v_compania AS cia
        ON pro.compania_id = cia.compania_id
    LEFT JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON esc.poliza_id = poli.poliza_id
    LEFT JOIN
        mdb_seguros_colombia.v_sucursal AS sucu
        ON poli.sucursal_id = sucu.sucursal_id
    LEFT JOIN
        mdb_seguros_colombia.v_canal_comercial AS canal
        ON sucu.canal_comercial_id = canal.canal_comercial_id
    LEFT JOIN
        mdb_seguros_colombia.v_poliza_certificado_etl AS pcetl
        ON esc.poliza_certificado_id = pcetl.poliza_certificado_id
    LEFT JOIN
        canal_poliza AS p
        ON
            esc.poliza_id = p.poliza_id
            AND codigo_ramo_aux = p.codigo_ramo_op
            AND pro.compania_id = p.compania_id
    LEFT JOIN
        canal_canal AS c
        ON
            codigo_ramo_aux = c.codigo_ramo_op
            AND sucu.canal_comercial_id = c.canal_comercial_id
            AND pro.compania_id = c.compania_id
    LEFT JOIN
        canal_sucursal AS s
        ON
            codigo_ramo_aux = s.codigo_ramo_op
            AND poli.sucursal_id = s.sucursal_id
            AND pro.compania_id = s.compania_id
    LEFT JOIN
        amparos AS amparo
        ON
            codigo_ramo_aux = amparo.codigo_ramo_op
            AND esc.amparo_id = amparo.amparo_id
            AND apertura_canal_aux = amparo.apertura_canal_desc
            AND pro.compania_id = amparo.compania_id
    LEFT JOIN
        atipicos AS atip
        ON
            pro.compania_id = atip.compania_id
            AND codigo_ramo_aux = atip.codigo_ramo_op
            AND esc.siniestro_id = atip.siniestro_id
            AND COALESCE(amparo.apertura_amparo_desc, 'RESTO')
            = atip.apertura_amparo_desc

    WHERE
        esc.ramo_id IN (78, 274, 57074, 140, 107, 271, 297, 204)
        AND pro.compania_id IN (3, 4)
        AND esc.mes_id BETWEEN CAST(
            '{mes_primera_ocurrencia}' AS INTEGER
        ) AND CAST('{mes_corte}' AS INTEGER)

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
    HAVING NOT (pago_bruto = 0 AND aviso_bruto = 0)

) PRIMARY INDEX (
    fecha_siniestro
    , fecha_registro
    , numero_poliza
    , asegurado_id
    , codigo_op
    , codigo_ramo_aux
    , siniestro_id
    , apertura_amparo_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE base_incurrido_prelim AS
(
    WITH base AS (
        SELECT
            fecha_siniestro
            , fecha_registro
            , asegurado_id
            , numero_poliza
            , nombre_tecnico
            , codigo_op
            , codigo_ramo_aux
            , siniestro_id
            , atipico
            , tipo_estado_siniestro_cd
            , apertura_canal_aux
            , nombre_canal_comercial
            , nombre_sucursal
            , apertura_amparo_desc
            , pago_bruto
            , CAST(0 AS FLOAT) AS pago_cedido
            , aviso_bruto
            , CAST(0 AS FLOAT) AS aviso_cedido
        FROM base_bruto

        UNION ALL

        SELECT
            fecha_siniestro
            , fecha_registro
            , asegurado_id
            , numero_poliza
            , nombre_tecnico
            , codigo_op
            , codigo_ramo_aux
            , siniestro_id
            , atipico
            , tipo_estado_siniestro_cd
            , apertura_canal_aux
            , nombre_canal_comercial
            , nombre_sucursal
            , apertura_amparo_desc
            , CAST(0 AS FLOAT) AS pago_bruto
            , pago_cedido
            , CAST(0 AS FLOAT) AS aviso_bruto
            , aviso_cedido
        FROM base_bruto
    )

    SELECT
        COALESCE(fecha_siniestro, (DATE '1990-01-01')) AS fecha_siniestro
        , fecha_registro
        , COALESCE(asegurado_id, -1) AS asegurado_id
        , COALESCE(numero_poliza, '-1') AS numero_poliza
        , nombre_tecnico
        , codigo_op
        , codigo_ramo_aux AS codigo_ramo_op
        , siniestro_id
        , atipico
        , tipo_estado_siniestro_cd
        , apertura_canal_aux AS apertura_canal_desc
        , nombre_canal_comercial
        , nombre_sucursal
        , apertura_amparo_desc
        , SUM(pago_bruto) AS pago_bruto
        , SUM(pago_cedido) AS pago_cedido
        , SUM(pago_bruto - pago_cedido) AS pago_retenido
        , SUM(aviso_bruto) AS aviso_bruto
        , SUM(aviso_cedido) AS aviso_cedido
        , SUM(aviso_bruto - aviso_cedido) AS aviso_retenido
        , SUM(pago_bruto + aviso_bruto) AS incurrido_bruto
        , SUM(pago_cedido + aviso_cedido) AS incurrido_cedido
        , SUM(pago_bruto - pago_cedido + aviso_bruto - aviso_cedido) AS incurrido_retenido
    FROM base
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_op, siniestro_id
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE porcentajes_retencion AS
(
    SELECT
        codigo_op
        , codigo_ramo_op
        , apertura_canal_desc
        , apertura_amparo_desc
        , atipico
        , LEAST(SUM(incurrido_retenido) / SUM(incurrido_bruto), 1)
            AS porcentaje_retencion
    FROM base_incurrido_prelim
    WHERE
        NOT (codigo_ramo_op = '083' AND codigo_op = '02')
        AND atipico = 0
        AND fecha_registro BETWEEN
        ADD_MONTHS(LAST_DAY((DATE '{fecha_mes_corte}')), -13)
        AND ADD_MONTHS(LAST_DAY((DATE '{fecha_mes_corte}')), -1)

    GROUP BY 1, 2, 3, 4, 5

) WITH DATA PRIMARY INDEX (
    codigo_op, codigo_ramo_op, apertura_canal_desc, apertura_amparo_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE vigencias_contrato_083 AS
(
    SELECT DISTINCT
        ano_id AS vigencia_contrato
        , MIN(dia_dt) OVER (PARTITION BY ano_id) AS primer_dia_ano
        , MAX(dia_dt) OVER (PARTITION BY ano_id) AS ultimo_dia_ano
        , ADD_MONTHS(primer_dia_ano, 6) AS inicio_vigencia_contrato
        , ADD_MONTHS(inicio_vigencia_contrato, 12)
        - INTERVAL '1' DAY AS fin_vigencia_contrato
        ,CASE WHEN vigencia_contrato < 2022 THEN 200E6 ELSE 250E6 END AS prioridad
    FROM mdb_seguros_colombia.v_dia
) WITH DATA PRIMARY INDEX (vigencia_contrato) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE inc_atip AS (
    WITH aux_fecha AS (
        SELECT
            siniestro_id
            , apertura_amparo_desc
            , MAX(fecha_registro) AS fecha_registro
        FROM base_incurrido_prelim GROUP BY 1, 2
    )

    SELECT
        inc_atip.codigo_op
        , inc_atip.codigo_ramo_op
        , inc_atip.siniestro_id
        , COALESCE(
            p.apertura_canal_desc, c.apertura_canal_desc, s.apertura_canal_desc
            , CASE
                WHEN
                    inc_atip.codigo_ramo_op IN ('081', 'AAV')
                    THEN 'Otros Banca'
                WHEN
                    inc_atip.codigo_ramo_op IN ('083')
                    AND inc_atip.codigo_op = '02'
                    THEN 'No Banca'
                ELSE 'Resto'
            END
        ) AS apertura_canal_aux
        , COALESCE(amparo.apertura_amparo_desc, 'RESTO') AS apertura_amparo_desc
        , aux_fecha.fecha_registro
        , SUM(inc_atip.pago_cedido) AS pago_cedido
        , SUM(inc_atip.aviso_cedido) AS aviso_cedido
        , SUM(inc_atip.pago_cedido + inc_atip.aviso_cedido) AS incurrido_cedido
    FROM incurridos_cedidos_atipicos AS inc_atip
    LEFT JOIN
        canal_poliza AS p
        ON
            inc_atip.numero_poliza = p.numero_poliza
            AND inc_atip.codigo_ramo_op = p.codigo_ramo_op
            AND inc_atip.codigo_op = p.codigo_op
    LEFT JOIN
        canal_canal AS c
        ON
            inc_atip.codigo_ramo_op = c.codigo_ramo_op
            AND inc_atip.nombre_canal_comercial = c.nombre_canal_comercial
            AND inc_atip.codigo_op = c.codigo_op
    LEFT JOIN
        canal_sucursal AS s
        ON
            inc_atip.codigo_ramo_op = s.codigo_ramo_op
            AND inc_atip.nombre_sucursal = s.nombre_sucursal
            AND inc_atip.codigo_op = s.codigo_op
    LEFT JOIN
        amparos AS amparo
        ON
            inc_atip.codigo_ramo_op = amparo.codigo_ramo_op
            AND inc_atip.amparo_desc = amparo.amparo_desc
            AND amparo.apertura_canal_desc = apertura_canal_aux
            AND inc_atip.codigo_op = amparo.codigo_op
    LEFT JOIN
        aux_fecha
        ON
            inc_atip.siniestro_id = aux_fecha.siniestro_id
            AND aux_fecha.apertura_amparo_desc
            = COALESCE(amparo.apertura_amparo_desc, 'RESTO')
    GROUP BY 1, 2, 3, 4, 5, 6
) WITH DATA PRIMARY INDEX (
    codigo_op, codigo_ramo_op, siniestro_id, apertura_amparo_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE base_incurrido_no_083 AS (
    SELECT
        base.fecha_siniestro
        , base.fecha_registro
        , base.asegurado_id
        , base.numero_poliza
        , base.nombre_tecnico
        , base.codigo_op
        , base.codigo_ramo_aux
        , base.siniestro_id
        , base.atipico
        , base.tipo_estado_siniestro_cd
        , base.apertura_canal_aux
        , base.nombre_canal_comercial
        , base.nombre_sucursal
        , base.apertura_amparo_desc
        , base.pago_bruto
        , base.aviso_bruto
        , CASE
            WHEN
                CAST('{aproximar_reaseguro}' AS INTEGER) = 0
                OR LAST_DAY(base.fecha_registro)
                <> LAST_DAY((DATE '{fecha_mes_corte}'))
                THEN base.pago_retenido
            WHEN base.atipico = 0
                THEN base.pago_bruto * COALESCE(pct_ret.porcentaje_retencion, 1)
            WHEN base.atipico = 1
                THEN base.pago_bruto - COALESCE(inc_atip.pago_cedido, 0)
        END AS pago_retenido
        , CASE
            WHEN
                CAST('{aproximar_reaseguro}' AS INTEGER) = 0
                OR LAST_DAY(base.fecha_registro)
                <> LAST_DAY((DATE '{fecha_mes_corte}'))
                THEN base.aviso_retenido
            WHEN base.atipico = 0
                THEN
                    base.aviso_bruto * COALESCE(pct_ret.porcentaje_retencion, 1)
            WHEN base.atipico = 1
                THEN base.aviso_bruto - COALESCE(inc_atip.aviso_cedido, 0)
        END AS aviso_retenido

    FROM base_incurrido_prelim AS base
    LEFT JOIN porcentajes_retencion AS pct_ret
        ON
            base.codigo_op = pct_ret.codigo_op
            AND base.codigo_ramo_op = pct_ret.codigo_ramo_op
            AND base.apertura_canal_desc = pct_ret.apertura_canal_desc
            AND base.apertura_amparo_desc = pct_ret.apertura_amparo_desc
            AND base.atipico = pct_ret.atipico
    LEFT JOIN inc_atip
        ON
            base.codigo_op = inc_atip.codigo_op
            AND base.codigo_ramo_op = inc_atip.codigo_ramo_op
            AND base.apertura_amparo_desc = inc_atip.apertura_amparo_desc
            AND base.siniestro_id = inc_atip.siniestro_id
            AND base.fecha_registro = inc_atip.fecha_registro

    WHERE NOT (base.codigo_ramo_op = '083' AND base.codigo_op = '02')
) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_op, siniestro_id
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE base_incurrido_083 AS (
    SELECT
        base.fecha_siniestro
        , base.fecha_registro
        , base.asegurado_id
        , base.numero_poliza
        , base.nombre_tecnico
        , base.codigo_op
        , base.codigo_ramo_aux
        , base.siniestro_id
        , base.atipico
        , base.tipo_estado_siniestro_cd
        , base.apertura_canal_aux
        , base.nombre_canal_comercial
        , base.nombre_sucursal
        , base.apertura_amparo_desc
        , base.pago_bruto
        , base.aviso_bruto
        , SUM(base.pago_bruto)
            OVER (
                PARTITION BY
                    base.asegurado_id
                    , vig.vigencia_contrato
                ORDER BY base.fecha_registro ROWS UNBOUNDED PRECEDING
            )
            AS pago_bruto_acum
        , SUM(base.aviso_bruto)
            OVER (
                PARTITION BY
                    base.asegurado_id
                    , vig.vigencia_contrato
                ORDER BY base.fecha_registro ROWS UNBOUNDED PRECEDING
            )
            AS aviso_bruto_acum
        , SUM(base.incurrido_bruto)
            OVER (
                PARTITION BY
                    base.asegurado_id
                    , vig.vigencia_contrato
                ORDER BY base.fecha_registro ROWS UNBOUNDED PRECEDING
            )
            AS incurrido_bruto_acum
        , CASE
            WHEN
                CAST('{aproximar_reaseguro}' AS INTEGER) = 0
                OR LAST_DAY(base.fecha_registro)
                <> LAST_DAY((DATE '{fecha_mes_corte}'))
                THEN base.pago_retenido
            WHEN base.atipico = 1
                THEN base.pago_bruto - COALESCE(inc_atip.pago_cedido, 0)
            WHEN
                base.numero_poliza = '083004273427'
                THEN base.pago_bruto * 0.2
            WHEN
                base.nombre_tecnico = 'PILOTOS'
                THEN base.pago_bruto * 0.4
            WHEN
                base.nombre_tecnico = 'INPEC'
                THEN base.pago_bruto * 0.25
            WHEN
                base.apertura_canal_desc = 'Banco Agrario'
                THEN base.pago_bruto * 0.1
            WHEN
                base.apertura_canal_desc = 'Banco-L'
                AND base.fecha_siniestro BETWEEN (
                    DATE '2017-11-01'
                ) AND (
                    DATE '2021-10-31'
                )
                THEN base.pago_bruto * 0.1
            WHEN
                pago_bruto_acum - base.pago_bruto < vig.prioridad
                THEN
                    LEAST(
                        base.pago_bruto
                        , vig.prioridad
                        - (pago_bruto_acum - base.pago_bruto)
                    )
            WHEN
                pago_bruto_acum - base.pago_bruto >= vig.prioridad
                THEN 0
        END AS pago_retenido_aprox

        , CASE
            WHEN
                CAST('{aproximar_reaseguro}' AS INTEGER) = 0
                OR LAST_DAY(base.fecha_registro)
                <> LAST_DAY((DATE '{fecha_mes_corte}'))
                THEN base.aviso_retenido
            WHEN base.atipico = 1
                THEN base.aviso_bruto - COALESCE(inc_atip.aviso_cedido, 0)
            WHEN
                base.numero_poliza = '083004273427'
                THEN base.aviso_bruto * 0.2
            WHEN
                base.nombre_tecnico = 'PILOTOS'
                THEN base.aviso_bruto * 0.4
            WHEN
                base.nombre_tecnico = 'INPEC'
                THEN base.aviso_bruto * 0.25
            WHEN
                base.apertura_canal_desc = 'Banco Agrario'
                THEN base.aviso_bruto * 0.1
            WHEN
                base.apertura_canal_desc = 'Banco-L'
                AND base.fecha_siniestro BETWEEN (
                    DATE '2017-11-01'
                ) AND (
                    DATE '2021-10-31'
                )
                THEN base.aviso_bruto * 0.1
            WHEN
                incurrido_bruto_acum
                - incurrido_bruto
                < vig.prioridad
                AND base.aviso_bruto >= 0
                THEN
                    LEAST(
                        base.aviso_bruto
                        , vig.prioridad
                        - (
                            incurrido_bruto_acum
                            - incurrido_bruto
                        )
                    )
            WHEN
                incurrido_bruto_acum
                - incurrido_bruto
                < vig.prioridad
                AND base.aviso_bruto < 0
                THEN base.aviso_bruto
            WHEN
                incurrido_bruto_acum
                - incurrido_bruto
                >= vig.prioridad
                AND base.aviso_bruto >= 0
                THEN 0
            WHEN
                incurrido_bruto_acum
                - incurrido_bruto
                >= vig.prioridad
                AND base.aviso_bruto < 0
                AND LEAST(
                    base.aviso_bruto
                    - (
                        vig.prioridad
                        - (
                            incurrido_bruto_acum
                            - incurrido_bruto
                        )
                    )
                    , 0
                )
                = 0
                THEN -pago_retenido_aprox
            WHEN
                incurrido_bruto_acum
                - incurrido_bruto
                >= 0
                AND base.aviso_bruto < 0
                THEN
                    LEAST(
                        base.aviso_bruto
                        - (
                            vig.prioridad
                            - (
                                incurrido_bruto_acum
                                - incurrido_bruto
                            )
                        )
                        , 0
                    )
        END AS aviso_retenido_aprox

    FROM base_incurrido_prelim AS base
    INNER JOIN
        vigencias_contrato_083 AS vig
        ON
            base.fecha_siniestro BETWEEN vig.inicio_vigencia_contrato AND vig.fin_vigencia_contrato
    LEFT JOIN inc_atip
        ON
            base.codigo_op = inc_atip.codigo_op
            AND base.codigo_ramo_op = inc_atip.codigo_ramo_op
            AND base.apertura_amparo_desc = inc_atip.apertura_amparo_desc
            AND base.siniestro_id = inc_atip.siniestro_id
            AND base.fecha_registro = inc_atip.fecha_registro

    WHERE base.codigo_ramo_op = '083' AND base.codigo_op = '02'
) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_op, siniestro_id
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE base_incurrido_reaseguro_aprox AS (
    SELECT
        fecha_siniestro
        , fecha_registro
        , asegurado_id
        , numero_poliza
        , nombre_tecnico
        , codigo_op
        , codigo_ramo_aux
        , siniestro_id
        , atipico
        , tipo_estado_siniestro_cd
        , apertura_canal_aux
        , nombre_canal_comercial
        , nombre_sucursal
        , apertura_amparo_desc
        , pago_bruto
        , aviso_bruto
        , pago_retenido
        , aviso_retenido
        , pago_bruto - pago_retenido AS pago_cedido
        , aviso_bruto - aviso_retenido AS aviso_cedido
        , pago_bruto + aviso_bruto - pago_retenido - aviso_retenido AS incurrido_cedido
    FROM base_incurrido_no_083
    UNION ALL
    SELECT
        fecha_siniestro
        , fecha_registro
        , asegurado_id
        , numero_poliza
        , nombre_tecnico
        , codigo_op
        , codigo_ramo_aux
        , siniestro_id
        , atipico
        , tipo_estado_siniestro_cd
        , apertura_canal_aux
        , nombre_canal_comercial
        , nombre_sucursal
        , apertura_amparo_desc
        , pago_bruto
        , aviso_bruto
        , pago_retenido_aprox AS pago_retenido
        , aviso_retenido_aprox AS aviso_retenido
        , pago_bruto - pago_retenido_aprox AS pago_cedido
        , aviso_bruto - aviso_retenido_aprox AS aviso_cedido
        , pago_bruto + aviso_bruto - pago_retenido_aprox - aviso_retenido_aprox AS incurrido_cedido
    FROM base_incurrido_083
) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_op, siniestro_id
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE sap_sin_atipicos AS (
    WITH inc_atip AS (
        SELECT
            codigo_op
            , codigo_ramo_op
            , SUM(pago_cedido) AS pago_cedido_atipicos
            , SUM(incurrido_cedido) AS incurrido_cedido_atipicos
        FROM incurridos_cedidos_atipicos
        GROUP BY 1, 2
    )

    SELECT
        sap.codigo_op
        , sap.codigo_ramo_op
        , sap.pago_cedido
        - ZEROIFNULL(inc_atip.pago_cedido_atipicos) AS pago_cedido_tipicos
        , sap.pago_cedido
        + sap.aviso_cedido
        - ZEROIFNULL(inc_atip.incurrido_cedido_atipicos)
            AS incurrido_cedido_tipicos

    FROM cifras_sap AS sap
    LEFT JOIN inc_atip
        ON
            sap.codigo_op = inc_atip.codigo_op
            AND sap.codigo_ramo_op = inc_atip.codigo_ramo_op

) WITH DATA PRIMARY INDEX (codigo_op, codigo_ramo_op) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE factor_cuadre_sap AS
(
    WITH tera AS (
        SELECT
            codigo_op
            , codigo_ramo_op

            -- En 083 asumimos que toda la diferencia SAP vs Tera esta contenida en estos canales
            , SUM(CASE
                WHEN
                    codigo_op = '02'
                    AND codigo_ramo_op = '083'
                    AND nombre_canal_comercial NOT IN (
                        'SUCURSALES'
                        , 'PROMOTORAS'
                        , 'CORPORATIVO'
                        , 'GRAN EMPRESA'
                    )
                    AND nombre_sucursal NOT IN (
                        'BANCOLOMBIA DEUDORES CONSUMO Y OTROS'
                    )
                    THEN 0
                ELSE pago_cedido
            END) AS pago_cedido_tipicos_cuadrables
            , SUM(CASE
                WHEN
                    codigo_op = '02'
                    AND codigo_ramo_op = '083'
                    AND nombre_canal_comercial NOT IN (
                        'SUCURSALES'
                        , 'PROMOTORAS'
                        , 'CORPORATIVO'
                        , 'GRAN EMPRESA'
                    )
                    AND nombre_sucursal NOT IN (
                        'BANCOLOMBIA DEUDORES CONSUMO Y OTROS'
                    )
                    THEN 0
                ELSE incurrido_cedido
            END) AS incurrido_cedido_tipicos_cuadrables
            , SUM(CASE
                WHEN
                    codigo_op = '02'
                    AND codigo_ramo_op = '083'
                    AND nombre_canal_comercial NOT IN (
                        'SUCURSALES'
                        , 'PROMOTORAS'
                        , 'CORPORATIVO'
                        , 'GRAN EMPRESA'
                    )
                    AND nombre_sucursal NOT IN (
                        'BANCOLOMBIA DEUDORES CONSUMO Y OTROS'
                    )
                    THEN pago_cedido
                ELSE 0
            END) AS pago_cedido_tipicos_no_cuadrables
            , SUM(
                CASE
                    WHEN
                        codigo_op = '02'
                        AND codigo_ramo_op = '083'
                        AND nombre_canal_comercial NOT IN (
                            'SUCURSALES'
                            , 'PROMOTORAS'
                            , 'CORPORATIVO'
                            , 'GRAN EMPRESA'
                        )
                        AND nombre_sucursal NOT IN (
                            'BANCOLOMBIA DEUDORES CONSUMO Y OTROS'
                        )
                        THEN incurrido_cedido
                    ELSE 0
                END
            ) AS incurrido_cedido_tipicos_no_cuadrables

        FROM base_incurrido_reaseguro_aprox
        WHERE LAST_DAY(fecha_registro) = LAST_DAY((DATE '{fecha_mes_corte}'))
        AND atipico = 0
        GROUP BY 1, 2
    )

    SELECT
        sap.codigo_op
        , sap.codigo_ramo_op
        , (sap.pago_cedido_tipicos - ZEROIFNULL(tera.pago_cedido_tipicos_no_cuadrables))
        / CASE
            WHEN
                tera.pago_cedido_tipicos_cuadrables <> 0
                THEN tera.pago_cedido_tipicos_cuadrables
            ELSE 1
        END AS factor_escala_pago
        , (
            sap.incurrido_cedido_tipicos
            - ZEROIFNULL(tera.incurrido_cedido_tipicos_no_cuadrables)
        )
        / CASE
            WHEN
                tera.incurrido_cedido_tipicos_cuadrables <> 0
                THEN tera.incurrido_cedido_tipicos_cuadrables
            ELSE 1
        END AS factor_escala_incurrido

    FROM sap_sin_atipicos AS sap
    INNER JOIN
        tera
        ON
            sap.codigo_op = tera.codigo_op
            AND sap.codigo_ramo_op = tera.codigo_ramo_op

) WITH DATA PRIMARY INDEX (codigo_op, codigo_ramo_op) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE base_incurrido AS
(
    SELECT
        fecha_siniestro
        , fecha_registro
        , asegurado_id
        , numero_poliza
        , nombre_tecnico
        , codigo_op
        , codigo_ramo_aux
        , siniestro_id
        , atipico
        , tipo_estado_siniestro_cd
        , apertura_canal_aux
        , nombre_canal_comercial
        , nombre_sucursal
        , apertura_amparo_desc
        , pago_bruto
        , aviso_bruto
        , CASE
            WHEN
                LAST_DAY(base.fecha_registro)
                <> LAST_DAY((DATE '{fecha_mes_corte}'))
                OR CAST('{aproximar_reaseguro}' AS INTEGER) = 0
                OR base.atipico = 1
                THEN base.pago_cedido
            WHEN
                base.codigo_op = '02'
                AND base.codigo_ramo_op = '083'
                AND base.nombre_canal_comercial NOT IN (
                    'SUCURSALES', 'PROMOTORAS', 'CORPORATIVO', 'GRAN EMPRESA'
                )
                AND base.nombre_sucursal NOT IN (
                    'BANCOLOMBIA DEUDORES CONSUMO Y OTROS'
                )
                THEN base.pago_cedido
            ELSE
                base.pago_cedido * COALESCE(escala.factor_escala_pago, 1)
        END AS pago_cedido_cuadrado
        , CASE
            WHEN
                LAST_DAY(base.fecha_registro)
                <> LAST_DAY((DATE '{fecha_mes_corte}'))
                OR CAST('{aproximar_reaseguro}' AS INTEGER) = 0
                OR base.atipico = 1
                THEN base.incurrido_cedido
            WHEN
                base.codigo_op = '02'
                AND base.codigo_ramo_op = '083'
                AND base.nombre_canal_comercial NOT IN (
                    'SUCURSALES', 'PROMOTORAS', 'CORPORATIVO', 'GRAN EMPRESA'
                )
                AND base.nombre_sucursal NOT IN (
                    'BANCOLOMBIA DEUDORES CONSUMO Y OTROS'
                )
                THEN base.incurrido_cedido
            ELSE
                base.incurrido_cedido
                * COALESCE(escala.factor_escala_incurrido, 1)
        END AS incurrido_cedido_cuadrado
        , incurrido_cedido_cuadrado
        - pago_cedido_cuadrado AS aviso_cedido_cuadrado
        , pago_bruto - pago_cedido_cuadrado AS pago_retenido
        , aviso_bruto - aviso_cedido_cuadrado AS aviso_retenido

    FROM base_incurrido_reaseguro_aprox AS base
    LEFT JOIN factor_cuadre_sap AS escala
        ON
            base.codigo_op = escala.codigo_op
            AND base.codigo_ramo_op = escala.codigo_ramo_op

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_op, siniestro_id
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE conteo_pago_bruto AS
(
    SELECT
        fecha_siniestro
        , codigo_op
        , codigo_ramo_op
        , apertura_canal_desc
        , apertura_amparo_desc
        , fecha_registro
        , atipico
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_pago

    FROM
        (
            SELECT
                fecha_siniestro
                , codigo_op
                , codigo_ramo_op
                , apertura_canal_desc
                , apertura_amparo_desc
                , siniestro_id
                , atipico
                , MIN(fecha_registro) AS fecha_registro

            FROM base_incurrido
            WHERE
                tipo_estado_siniestro_cd NOT IN ('N', 'O', 'D', 'C')
                AND ABS(pago_bruto) > 1000
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            HAVING SUM(pago_bruto) > 1000
        ) AS fmin
    GROUP BY 1, 2, 3, 4, 5, 6, 7

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_op
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE conteo_incurrido_bruto AS
(
    SELECT
        fecha_siniestro
        , codigo_op
        , codigo_ramo_op
        , fecha_registro
        , apertura_canal_desc
        , apertura_amparo_desc
        , atipico
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_incurrido

    FROM
        (
            SELECT
                fecha_siniestro
                , codigo_op
                , codigo_ramo_op
                , siniestro_id
                , apertura_canal_desc
                , apertura_amparo_desc
                , atipico
                , MIN(fecha_registro) AS fecha_registro

            FROM base_incurrido
            WHERE
                tipo_estado_siniestro_cd NOT IN ('N', 'O', 'D', 'C')
                AND NOT (ABS(pago_bruto) < 1000 AND ABS(aviso_bruto) < 1000)
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            HAVING NOT (SUM(pago_bruto) < 1000 AND SUM(aviso_bruto) < 1000)
        ) AS fmin
    GROUP BY 1, 2, 3, 4, 5, 6, 7

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_op
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE sin_pagos_bruto AS
(
    SELECT
        base.fecha_siniestro
        , base.codigo_op
        , base.codigo_ramo_op
        , base.siniestro_id
        , base.tipo_estado_siniestro_cd
        , base.apertura_canal_desc
        , base.apertura_amparo_desc
        , base.atipico
        , ZEROIFNULL(rva.en_reserva) AS en_rva
        , ZEROIFNULL(MAX(ABS(base.pago_bruto))) AS pago_max

    FROM base_incurrido AS base
    LEFT JOIN
        (
            SELECT
                fecha_siniestro
                , codigo_op
                , codigo_ramo_op
                , siniestro_id
                , tipo_estado_siniestro_cd
                , apertura_canal_desc
                , apertura_amparo_desc
                , atipico
                , 1 AS en_reserva
                , SUM(aviso_bruto) AS aviso_bruto

            FROM base_incurrido
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
            HAVING SUM(aviso_bruto) > 1000
        ) AS rva
        ON
            (base.fecha_siniestro = rva.fecha_siniestro)
            AND (base.codigo_ramo_op = rva.codigo_ramo_op)
            AND (base.codigo_op = rva.codigo_op)
            AND (base.siniestro_id = rva.siniestro_id)
            AND (base.tipo_estado_siniestro_cd = rva.tipo_estado_siniestro_cd)
            AND (base.apertura_canal_desc = rva.apertura_canal_desc)
            AND (base.apertura_amparo_desc = rva.apertura_amparo_desc)
            AND (base.atipico = rva.atipico)

    WHERE en_rva = 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    HAVING pago_max < 1000

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, siniestro_id, codigo_ramo_op
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE conteo_desistido_bruto AS
(
    SELECT
        fecha_siniestro
        , codigo_op
        , codigo_ramo_op
        , fecha_registro
        , apertura_canal_desc
        , apertura_amparo_desc
        , atipico
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_desistido

    FROM
        (
            SELECT
                base.fecha_siniestro
                , base.codigo_op
                , base.codigo_ramo_op
                , base.siniestro_id
                , base.apertura_canal_desc
                , base.apertura_amparo_desc
                , base.atipico
                , MAX(base.fecha_registro) AS fecha_registro

            FROM base_incurrido AS base
            INNER JOIN sin_pagos_bruto AS pag
                ON
                    (base.fecha_siniestro = pag.fecha_siniestro)
                    AND (base.codigo_ramo_op = pag.codigo_ramo_op)
                    AND (base.codigo_op = pag.codigo_op)
                    AND (base.siniestro_id = pag.siniestro_id)
                    AND (
                        base.tipo_estado_siniestro_cd
                        = pag.tipo_estado_siniestro_cd
                    )
                    AND (base.apertura_canal_desc = pag.apertura_canal_desc)
                    AND (base.apertura_amparo_desc = pag.apertura_amparo_desc)
                    AND (base.atipico = pag.atipico)
            WHERE
                base.tipo_estado_siniestro_cd NOT IN ('N', 'O', 'D', 'C')
                AND NOT (pago_bruto = 0 AND aviso_bruto = 0)
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            HAVING NOT (SUM(pago_bruto) < 1000 AND SUM(aviso_bruto) < 1000)
        ) AS fecha

    GROUP BY 1, 2, 3, 4, 5, 6, 7

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_op
) ON COMMIT PRESERVE ROWS;


-- DROP TABLE BASE_PAGOS_AVISO
CREATE MULTISET VOLATILE TABLE base_pagos_aviso AS
(
    SELECT
        fecha_siniestro
        , codigo_op
        , codigo_ramo_op
        , fecha_registro
        , apertura_canal_desc
        , apertura_amparo_desc
        , atipico
        , ZEROIFNULL(SUM(pago_bruto)) AS pago_bruto
        , ZEROIFNULL(SUM(pago_retenido)) AS pago_retenido
        , ZEROIFNULL(SUM(aviso_bruto)) AS aviso_bruto
        , ZEROIFNULL(SUM(aviso_retenido)) AS aviso_retenido

    FROM base_incurrido
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_op
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE fechas AS
(
    SELECT DISTINCT
        dia_dt
        , MIN(dia_dt) OVER (PARTITION BY mes_id) AS primer_dia_mes
    FROM mdb_seguros_colombia.v_dia
) WITH DATA PRIMARY INDEX (dia_dt) ON COMMIT PRESERVE ROWS;



SELECT
    base.codigo_op
    , base.codigo_ramo_op
    , base.atipico
    , base.apertura_canal_desc
    , base.apertura_amparo_desc
    , GREATEST((DATE '{fecha_primera_ocurrencia}'), focurr.primer_dia_mes)
        AS fecha_siniestro
    , GREATEST((DATE '{fecha_primera_ocurrencia}'), fmov.primer_dia_mes)
        AS fecha_registro
    , COALESCE(SUM(contp.conteo_pago), 0) AS conteo_pago
    , COALESCE(SUM(conti.conteo_incurrido), 0) AS conteo_incurrido
    , COALESCE(SUM(contd.conteo_desistido), 0) AS conteo_desistido
    , COALESCE(SUM(base.pago_bruto), 0) AS pago_bruto
    , COALESCE(SUM(base.pago_retenido), 0) AS pago_retenido
    , COALESCE(SUM(base.aviso_bruto), 0) AS aviso_bruto
    , COALESCE(SUM(base.aviso_retenido), 0) AS aviso_retenido

FROM base_pagos_aviso AS base
LEFT JOIN conteo_pago_bruto AS contp
    ON
        base.fecha_siniestro = contp.fecha_siniestro
        AND base.fecha_registro = contp.fecha_registro
        AND base.codigo_ramo_op = contp.codigo_ramo_op
        AND base.ramo_desc = contp.ramo_desc
        AND base.codigo_op = contp.codigo_op
        AND base.apertura_amparo_desc = contp.apertura_amparo_desc
        AND base.apertura_canal_desc = contp.apertura_canal_desc
        AND base.atipico = contp.atipico
LEFT JOIN conteo_incurrido_bruto AS conti
    ON
        base.fecha_siniestro = conti.fecha_siniestro
        AND base.fecha_registro = conti.fecha_registro
        AND base.codigo_op = conti.codigo_op
        AND base.codigo_ramo_op = conti.codigo_ramo_op
        AND base.ramo_desc = conti.ramo_desc
        AND base.apertura_amparo_desc = conti.apertura_amparo_desc
        AND base.apertura_canal_desc = conti.apertura_canal_desc
        AND base.atipico = conti.atipico
LEFT JOIN conteo_desistido_bruto AS contd
    ON
        base.fecha_siniestro = contd.fecha_siniestro
        AND base.fecha_registro = contd.fecha_registro
        AND base.codigo_ramo_op = contd.codigo_ramo_op
        AND base.ramo_desc = contd.ramo_desc
        AND base.codigo_op = contd.codigo_op
        AND base.apertura_amparo_desc = contd.apertura_amparo_desc
        AND base.apertura_canal_desc = contd.apertura_canal_desc
        AND base.atipico = contd.atipico

INNER JOIN fechas AS focurr ON base.fecha_siniestro = focurr.dia_dt
INNER JOIN fechas AS fmov ON base.fecha_registro = fmov.dia_dt

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

HAVING
    NOT (
        COALESCE(SUM(contp.conteo_pago), 0) = 0
        AND COALESCE(SUM(conti.conteo_incurrido), 0) = 0
        AND COALESCE(SUM(contd.conteo_desistido), 0) = 0
        AND COALESCE(SUM(base.pago_bruto), 0) = 0
        AND COALESCE(SUM(base.aviso_bruto), 0) = 0
        AND COALESCE(SUM(base.pago_retenido), 0) = 0
        AND COALESCE(SUM(base.aviso_retenido), 0) = 0
    )

ORDER BY 1, 2, 3, 4, 5, 6, 7, 8

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


CREATE MULTISET VOLATILE TABLE atipicos
(
    codigo_ramo_op VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
    , apertura_amparo_desc VARCHAR(100) NOT NULL
    , siniestro_id VARCHAR(100) NOT NULL
    , atipico INTEGER NOT NULL
) PRIMARY INDEX (siniestro_id, apertura_amparo_desc) ON COMMIT PRESERVE ROWS;
INSERT INTO ATIPICOS VALUES (?, ?, ?, ?, ?); -- noqa: 


CREATE MULTISET VOLATILE TABLE base_cedido AS
(
    SELECT
        LEAST(sini.fecha_siniestro, ersc.fecha_registro) AS fecha_siniestro
        , ersc.fecha_registro
        , cia.codigo_op
        , CASE
            WHEN
                pro.ramo_id = 78
                AND ersc.amparo_id NOT IN (930, -1)
                THEN 'AAV'
            ELSE ramo.codigo_ramo_op
        END AS codigo_ramo_aux
        , CASE
            WHEN codigo_ramo_aux = 'AAV' THEN 'AMPAROS ADICIONALES DE VIDA'
            ELSE ramo.ramo_desc
        END AS ramo_desc
        , ersc.siniestro_id
        , ZEROIFNULL(atip.atipico) AS atipico
        , COALESCE(
            p.apertura_canal_desc
            , s.apertura_canal_desc
            , CASE
                WHEN
                    pro.ramo_id IN (78, 274)
                    AND pro.compania_id = 3
                    THEN 'NO BANCA'
                ELSE 'RESTO'
            END
        ) AS apertura_canal_aux
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
        ON (pro.ramo_id = ramo.ramo_id)
    INNER JOIN
        mdb_seguros_colombia.v_siniestro AS sini
        ON (ersc.siniestro_id = sini.siniestro_id)
    INNER JOIN
        mdb_seguros_colombia.v_compania AS cia
        ON (pro.compania_id = cia.compania_id)
    INNER JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON (ersc.poliza_id = poli.poliza_id)
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
            AND ersc.amparo_id = amparo.amparo_id
            AND pro.compania_id = amparo.compania_id
        )
    LEFT JOIN
        atipicos AS atip
        ON
            (ersc.siniestro_id = atip.siniestro_id)
            AND (
                atip.apertura_amparo_desc
                = COALESCE(amparo.apertura_amparo_desc, 'RESTO')
            )

    WHERE
        pro.ramo_id IN (54835, 57074, 78, 274, 140, 107, 271)
        AND pro.compania_id IN (3, 4)
        AND ersc.mes_id BETWEEN '{mes_primera_ocurrencia}' AND '{mes_corte}'

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    HAVING NOT (pago_cedido = 0 AND aviso_cedido = 0)

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_aux, siniestro_id
) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE base_bruto AS
(
    SELECT
        LEAST(sini.fecha_siniestro, esc.fecha_registro) AS fecha_siniestro
        , esc.fecha_registro
        , cia.codigo_op
        , CASE
            WHEN
                esc.ramo_id = 78
                AND esc.amparo_id NOT IN (930, -1)
                THEN 'AAV'
            ELSE ramo.codigo_ramo_op
        END AS codigo_ramo_aux
        , CASE
            WHEN codigo_ramo_aux = 'AAV' THEN 'AMPAROS ADICIONALES DE VIDA'
            ELSE ramo.ramo_desc
        END AS ramo_desc
        , esc.siniestro_id
        , ZEROIFNULL(atip.atipico) AS atipico
        , COALESCE(
            p.apertura_canal_desc
            , COALESCE(
                s.apertura_canal_desc
                , CASE
                    WHEN
                        esc.ramo_id IN (78, 274)
                        AND pro.compania_id = 3
                        THEN 'NO BANCA'
                    ELSE 'RESTO'
                END
            )
        ) AS apertura_canal_aux
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
                    THEN esc.valor_siniestro * esc.valor_tasa
                ELSE 0
            END
        ) AS aviso_bruto

    FROM mdb_seguros_colombia.v_evento_siniestro_cobertura AS esc
    INNER JOIN
        mdb_seguros_colombia.v_siniestro AS sini
        ON esc.siniestro_id = sini.siniestro_id
    INNER JOIN mdb_seguros_colombia.v_ramo AS ramo ON esc.ramo_id = ramo.ramo_id
    INNER JOIN
        mdb_seguros_colombia.v_plan_individual AS pind
        ON (esc.plan_individual_id = pind.plan_individual_id)
    INNER JOIN
        mdb_seguros_colombia.v_producto AS pro
        ON (pind.producto_id = pro.producto_id)
    INNER JOIN
        mdb_seguros_colombia.v_compania AS cia
        ON (pro.compania_id = cia.compania_id)
    INNER JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON (esc.poliza_id = poli.poliza_id)
    LEFT JOIN
        polizas AS p
        ON
            (
                poli.numero_poliza = p.numero_poliza
                AND esc.ramo_id = p.ramo_id
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
            AND esc.amparo_id = amparo.amparo_id
            AND pro.compania_id = amparo.compania_id
        )
    LEFT JOIN
        atipicos AS atip
        ON
            (esc.siniestro_id = atip.siniestro_id)
            AND (
                atip.apertura_amparo_desc
                = COALESCE(amparo.apertura_amparo_desc, 'RESTO')
            )

    WHERE
        esc.ramo_id IN (54835, 57074, 78, 274, 140, 107, 271)
        AND pro.compania_id IN (3, 4)
        AND esc.mes_id BETWEEN '{mes_primera_ocurrencia}' AND '{mes_corte}'

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    HAVING NOT (pago_bruto = 0 AND aviso_bruto = 0)

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_aux, siniestro_id
) ON COMMIT PRESERVE ROWS;


-- DROP TABLE BASE_INCURRIDO
CREATE MULTISET VOLATILE TABLE base_incurrido AS
(
    SELECT
        COALESCE(brt.fecha_siniestro, ced.fecha_siniestro) AS fecha_siniestro
        , COALESCE(brt.fecha_registro, ced.fecha_registro) AS fecha_registro
        , COALESCE(brt.codigo_op, ced.codigo_op) AS codigo_op
        , COALESCE(brt.codigo_ramo_aux, ced.codigo_ramo_aux) AS codigo_ramo_op
        , COALESCE(brt.ramo_desc, ced.ramo_desc) AS ramo_desc
        , COALESCE(brt.siniestro_id, ced.siniestro_id) AS siniestro_id
        , COALESCE(brt.atipico, ced.atipico) AS atipico
        , COALESCE(brt.apertura_canal_aux, ced.apertura_canal_aux)
            AS apertura_canal_desc
        , COALESCE(brt.apertura_amparo_desc, ced.apertura_amparo_desc)
            AS apertura_amparo_desc
        , ZEROIFNULL(brt.pago_bruto) AS pago_bruto
        , ZEROIFNULL(ced.pago_cedido) AS pago_cedido
        , ZEROIFNULL(brt.pago_bruto)
        - ZEROIFNULL(ced.pago_cedido) AS pago_retenido
        , ZEROIFNULL(brt.aviso_bruto) AS aviso_bruto
        , ZEROIFNULL(ced.aviso_cedido) AS aviso_cedido
        , ZEROIFNULL(brt.aviso_bruto)
        - ZEROIFNULL(ced.aviso_cedido) AS aviso_retenido

    FROM base_bruto AS brt
    FULL OUTER JOIN base_cedido
        AS ced ON (brt.fecha_siniestro = ced.fecha_siniestro)
    AND (brt.fecha_registro = ced.fecha_registro)
    AND (brt.codigo_op = ced.codigo_op)
    AND (brt.codigo_ramo_aux = ced.codigo_ramo_aux)
    AND (brt.siniestro_id = ced.siniestro_id)
    AND (brt.atipico = ced.atipico)
    AND (brt.apertura_canal_aux = ced.apertura_canal_aux)
    AND (brt.apertura_amparo_desc = ced.apertura_amparo_desc)

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_op, siniestro_id
) ON COMMIT PRESERVE ROWS;


-- DROP TABLE CONTEO_PAGO_BRUTO
CREATE MULTISET VOLATILE TABLE conteo_pago_bruto AS
(
    WITH fmin AS (
        SELECT
            fecha_siniestro
            , codigo_op
            , codigo_ramo_op
            , ramo_desc
            , apertura_canal_desc
            , apertura_amparo_desc
            , siniestro_id
            , atipico
            , MIN(fecha_registro) AS fecha_registro

        FROM base_incurrido
        WHERE ABS(pago_bruto) > 1000
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        HAVING SUM(pago_bruto) > 1000
    )

    SELECT
        fecha_siniestro
        , codigo_op
        , codigo_ramo_op
        , ramo_desc
        , apertura_canal_desc
        , apertura_amparo_desc
        , fecha_registro
        , atipico
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_pago

    FROM fmin
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_op
) ON COMMIT PRESERVE ROWS;


-- DROP TABLE CONTEO_INCURRIDO_BRUTO
CREATE MULTISET VOLATILE TABLE conteo_incurrido_bruto AS
(
    WITH fmin AS (
        SELECT
            fecha_siniestro
            , codigo_op
            , codigo_ramo_op
            , ramo_desc
            , siniestro_id
            , apertura_canal_desc
            , apertura_amparo_desc
            , atipico
            , MIN(fecha_registro) AS fecha_registro

        FROM base_incurrido
        WHERE NOT (ABS(pago_bruto) < 1000 AND ABS(aviso_bruto) < 1000)
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        HAVING NOT (SUM(pago_bruto) < 1000 AND SUM(aviso_bruto) < 1000)
    )

    SELECT
        fecha_siniestro
        , codigo_op
        , codigo_ramo_op
        , ramo_desc
        , fecha_registro
        , apertura_canal_desc
        , apertura_amparo_desc
        , atipico
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_incurrido

    FROM fmin
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, fecha_registro, codigo_ramo_op
) ON COMMIT PRESERVE ROWS;


-- DROP TABLE SIN_PAGOS_BRUTO
CREATE MULTISET VOLATILE TABLE sin_pagos_bruto AS
(
    WITH en_reserva AS (
        SELECT
            fecha_siniestro
            , codigo_op
            , codigo_ramo_op
            , ramo_desc
            , siniestro_id
            , apertura_canal_desc
            , apertura_amparo_desc
            , atipico
            , 1 AS en_reserva
            , SUM(aviso_bruto) AS aviso_bruto

        FROM base_incurrido
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        HAVING SUM(aviso_bruto) > 1000
    )

    SELECT
        base.fecha_siniestro
        , base.codigo_op
        , base.codigo_ramo_op
        , base.ramo_desc
        , base.siniestro_id
        , base.apertura_canal_desc
        , base.apertura_amparo_desc
        , base.atipico
        , ZEROIFNULL(rva.en_reserva) AS en_reserva
        , ZEROIFNULL(MAX(ABS(base.pago_bruto))) AS pago_max

    FROM base_incurrido AS base
    LEFT JOIN en_reserva AS rva
        ON
            (base.fecha_siniestro = rva.fecha_siniestro)
            AND (base.codigo_ramo_op = rva.codigo_ramo_op)
            AND (base.codigo_op = rva.codigo_op)
            AND (base.siniestro_id = rva.siniestro_id)
            AND (base.apertura_canal_desc = rva.apertura_canal_desc)
            AND (base.apertura_amparo_desc = rva.apertura_amparo_desc)
            AND (base.atipico = rva.atipico)

    WHERE en_reserva = 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    HAVING pago_max < 1000

) WITH DATA PRIMARY INDEX (
    fecha_siniestro, siniestro_id, codigo_ramo_op
) ON COMMIT PRESERVE ROWS;


-- DROP TABLE CONTEO_DESISTIDO_BRUTO
CREATE MULTISET VOLATILE TABLE conteo_desistido_bruto AS
(
    SELECT
        fecha_siniestro
        , codigo_op
        , codigo_ramo_op
        , ramo_desc
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
                , base.ramo_desc
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
                    AND (base.apertura_canal_desc = pag.apertura_canal_desc)
                    AND (base.apertura_amparo_desc = pag.apertura_amparo_desc)
                    AND (base.atipico = pag.atipico)
            WHERE NOT (base.pago_bruto = 0 AND base.aviso_bruto = 0)
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
            HAVING
                NOT (
                    SUM(base.pago_bruto) < 1000 AND SUM(base.aviso_bruto) < 1000
                )
        ) AS fecha

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

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
        , ramo_desc
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
        , MIN(dia_dt) OVER (PARTITION BY mes_id) AS mes_id
    FROM mdb_seguros_colombia.v_dia
) WITH DATA PRIMARY INDEX (dia_dt) ON COMMIT PRESERVE ROWS;

SELECT
    base.codigo_op
    , base.codigo_ramo_op
    , base.ramo_desc
    , base.atipico
    , COALESCE(base.apertura_canal_desc, '-1') AS apertura_canal_desc
    , COALESCE(base.apertura_amparo_desc, '-1') AS apertura_amparo_desc
    , GREATEST((DATE '{fecha_primera_ocurrencia}'), focurr.mes_id)
        AS fecha_siniestro
    , GREATEST((DATE '{fecha_primera_ocurrencia}'), fmov.mes_id)
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
        (base.fecha_siniestro = contp.fecha_siniestro)
        AND (base.fecha_registro = contp.fecha_registro)
        AND (base.codigo_ramo_op = contp.codigo_ramo_op)
        AND (base.codigo_op = contp.codigo_op)
        AND (base.apertura_amparo_desc = contp.apertura_amparo_desc)
        AND (base.apertura_canal_desc = contp.apertura_canal_desc)
        AND (base.atipico = contp.atipico)
LEFT JOIN conteo_incurrido_bruto AS conti
    ON
        (base.fecha_siniestro = conti.fecha_siniestro)
        AND (base.fecha_registro = conti.fecha_registro)
        AND (base.codigo_op = conti.codigo_op)
        AND (base.codigo_ramo_op = conti.codigo_ramo_op)
        AND (base.apertura_amparo_desc = conti.apertura_amparo_desc)
        AND (base.apertura_canal_desc = conti.apertura_canal_desc)
        AND (base.atipico) = conti.atipico
LEFT JOIN conteo_desistido_bruto AS contd
    ON
        (base.fecha_siniestro = contd.fecha_siniestro)
        AND (base.fecha_registro = contd.fecha_registro)
        AND (base.codigo_ramo_op = contd.codigo_ramo_op)
        AND (base.codigo_op = contd.codigo_op)
        AND (base.apertura_amparo_desc = contd.apertura_amparo_desc)
        AND (base.apertura_canal_desc = contd.apertura_canal_desc)
        AND (base.atipico = contd.atipico)

INNER JOIN fechas AS focurr ON (base.fecha_siniestro = focurr.dia_dt)
INNER JOIN fechas AS fmov ON (base.fecha_registro = fmov.dia_dt)

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

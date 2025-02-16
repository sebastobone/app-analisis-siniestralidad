--Se cambio la tabla V_CONTRATO_MSTR por V_CONTRATO
--ya que no estaba trayendo el nombre a unas sucursales)

CREATE MULTISET VOLATILE TABLE sucursales
(
    codigo_ramo_op VARCHAR(100) NOT NULL
    , nombre_canal_comercial VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
    , apertura_canal_cd VARCHAR(100) NOT NULL
) PRIMARY INDEX (nombre_canal_comercial) ON COMMIT PRESERVE ROWS;
INSERT INTO sucursales VALUES (?, ?, ?, ?);  -- noqa:
COLLECT STATISTICS ON sucursales INDEX (nombre_canal_comercial);  -- noqa:


CREATE MULTISET VOLATILE TABLE amparos
(
    codigo_ramo_op VARCHAR(100) NOT NULL
    , amparo_desc VARCHAR(100) NOT NULL
    , apertura_amparo_desc VARCHAR(100) NOT NULL
    , apertura_amparo_cd VARCHAR(100) NOT NULL
) PRIMARY INDEX (amparo_desc) ON COMMIT PRESERVE ROWS;
INSERT INTO amparos VALUES (?, ?, ?, ?);  -- noqa:
COLLECT STATISTICS ON amparos INDEX (amparo_desc);  -- noqa:


CREATE MULTISET VOLATILE TABLE clases
(
    codigo_tarifa_cd VARCHAR(3) NOT NULL
    , descuento VARCHAR(2) NOT NULL
    , tipo_vehiculo VARCHAR(100) NOT NULL
    , tipo_vehiculo_cd VARCHAR(100) NOT NULL
) PRIMARY INDEX (codigo_tarifa_cd, descuento) ON COMMIT PRESERVE ROWS;
INSERT INTO clases VALUES (?, ?, ?, ?);  -- noqa:
COLLECT STATISTICS ON clases INDEX (codigo_tarifa_cd, descuento);  -- noqa:



CREATE MULTISET VOLATILE TABLE base_bruto AS
(
    SELECT
        LAST_DAY(sini.fecha_siniestro) AS fecha_siniestro
        --sini.Fecha_Siniestro
        , LAST_DAY(esc.fecha_registro) AS fecha_registro
        , '041' AS codigo_ramo_op
        , esc.siniestro_id
        , CASE
            WHEN cont.sucursal_id = 38360140 THEN 'CERRADO' -- INCOLMOTOS YAMAHA
            -- SUZUKI DE COLOMBIA
            WHEN cont.sucursal_id = 35031773 THEN 'CERRADO'
            -- AES
            WHEN
                cont.agente_lider_id IN (
                    33245824
                    , 37520330
                    , 36619257
                    , 36633042
                    , 36511605
                    , 36511604
                    , 36511601
                    , 36407369
                    , 36511600
                    , 36668273
                    , 36737406
                    , 36668272
                    , 38018881
                    , 39229989
                )
                THEN 'CERRADO'
            WHEN cont.agente_lider_id IN (20639675) THEN 'CERRADO' -- GRANSEGURO
            -- IVESUR
            WHEN
                cont.agente_lider_id IN (
                    27978737, 32080527, 22226341, 27987289, 27987290, 27987291
                )
                THEN 'CERRADO'
            -- TERPEL      
            WHEN
                (
                    cont.sucursal_id IN (14253584, 21713084)
                    AND cont.agente_lider_id NOT IN (
                        27978737
                        , 32080527
                        , 22226341
                        , 27987289
                        , 27987290
                        , 27987291
                        , 26692364
                        , 14299847
                        , 15697002
                        , 21787861
                        , -1
                        , 5046579
                        , 21303702
                        , 22420561
                        , 22936588
                        , 32090183
                        , 24996224
                        , 28618273
                        , 15697015
                        , 15697019
                        , 33245824
                        , 33538473
                        , 35040028
                        , 36633042
                        , 36407369
                        , 36511600
                        , 36511604
                        , 36511605
                        , 36668273
                        , 38604366
                        , 28669735
                    )
                )
                THEN 'CERRADO'
            ELSE COALESCE(s.apertura_canal_desc, 'RESTO')
        END AS apertura_canal_desc
        , COALESCE(amparo.apertura_amparo_desc, '-1') AS apertura_amparo_desc
        , COALESCE(vehi.tipo_vehiculo, 'MOTO') AS tipo_vehiculo

        , SUM(
            CASE
                WHEN
                    esc.tipo_oper_siniestro_cd <> '130'
                    THEN esc.valor_siniestro * esc.valor_tasa
                ELSE 0
            END
        ) AS pago_bruto
        , CAST(0 AS FLOAT) AS pago_cedido
        , ZEROIFNULL(pago_bruto) - ZEROIFNULL(pago_cedido) AS pago_retenido

        , SUM(
            CASE
                WHEN
                    esc.tipo_oper_siniestro_cd = '130'
                    THEN esc.valor_siniestro * esc.valor_tasa
                ELSE 0
            END
        ) AS aviso_bruto
        , CAST(0 AS FLOAT) AS aviso_cedido
        , ZEROIFNULL(aviso_bruto) - ZEROIFNULL(aviso_cedido) AS aviso_retenido

    FROM mdb_seguros_colombia.v_evento_siniestro_cobertura AS esc
    INNER JOIN
        mdb_seguros_colombia.v_siniestro AS sini
        ON esc.siniestro_id = sini.siniestro_id
    INNER JOIN mdb_seguros_colombia.v_ramo AS ramo ON esc.ramo_id = ramo.ramo_id
    INNER JOIN
        mdb_seguros_colombia.v_plan_individual AS plan
        ON (esc.plan_individual_id = plan.plan_individual_id)
    INNER JOIN
        mdb_seguros_colombia.v_producto AS pro
        ON (plan.producto_id = pro.producto_id)
    INNER JOIN
        mdb_seguros_colombia.v_amparo AS ampa
        ON (esc.amparo_id = ampa.amparo_id)
    INNER JOIN
        mdb_seguros_colombia.v_compania AS cia
        ON (pro.compania_id = cia.compania_id)
    INNER JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON (esc.poliza_id = poli.poliza_id)
    LEFT JOIN
        (
            SELECT
                contrato_id
                , sucursal_id
                , agente_lider_id
            FROM mdb_seguros_colombia.v_contrato QUALIFY
                ROW_NUMBER()
                    OVER (
                        PARTITION BY contrato_id
                        ORDER BY fecha_ultima_actualizacion DESC
                    )
                = 1
        ) AS cont
        ON (poli.poliza_id = cont.contrato_id)
    INNER JOIN
        mdb_seguros_colombia.v_sucursal AS sucu
        ON (cont.sucursal_id = sucu.sucursal_id)
    LEFT JOIN
        mdb_seguros_colombia.v_canal_comercial AS cacomer
        ON sucu.canal_comercial_id = cacomer.canal_comercial_id
    LEFT JOIN
        (SELECT
            sp.*
            , CASE
                WHEN
                    sp.codigo_tarifa_cd IN (
                        '100'
                        , '110'
                        , '120'
                        , '140'
                        , '150'
                        , '711'
                        , '712'
                        , '72'
                        , '721'
                        , '722'
                        , '73'
                        , '731'
                        , '732'
                        , '81'
                        , '810'
                        , '91'
                        , '910'
                        , '92'
                        , '920'
                    )
                    AND sp.fecha_expedicion >= '2022/12/19'
                    AND sp.fecha_inicio >= '2022/12/19'
                    THEN 'Si'
                ELSE 'No'
            END AS descuento
        FROM mdb_seguros_colombia.vsoat_poliza AS sp) AS spoli
        ON poli.poliza_id = spoli.poliza_id

    LEFT JOIN
        (SELECT DISTINCT
            codigo_ramo_op
            , nombre_canal_comercial
            , apertura_canal_desc
        FROM sucursales) AS s
        ON
            ramo.codigo_ramo_op = s.codigo_ramo_op
            AND cacomer.nombre_canal_comercial = s.nombre_canal_comercial
    LEFT JOIN
        (SELECT DISTINCT
            codigo_ramo_op
            , amparo_desc
            , apertura_amparo_desc
        FROM amparos) AS amparo
        ON
            ramo.codigo_ramo_op = amparo.codigo_ramo_op
            AND ampa.amparo_desc = amparo.amparo_desc
    LEFT JOIN
        (SELECT DISTINCT
            codigo_tarifa_cd
            , descuento
            , tipo_vehiculo
        FROM clases) AS vehi
        ON
            spoli.codigo_tarifa_cd = vehi.codigo_tarifa_cd
            AND spoli.descuento = vehi.descuento

    WHERE
        ramo.codigo_ramo_op IN ('041')
        AND esc.mes_id BETWEEN CAST(
            '{mes_primera_ocurrencia}' AS INTEGER
        ) AND CAST('{mes_corte}' AS INTEGER)

    GROUP BY 1, 2, 3, 4, 5, 6, 7
    HAVING NOT (pago_bruto = 0 AND aviso_bruto = 0)
)
WITH DATA PRIMARY INDEX
(
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , siniestro_id
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
) ON COMMIT PRESERVE ROWS;


-- drop table BASE_INCURRIDO;
CREATE MULTISET VOLATILE TABLE base_incurrido AS
(
    SELECT
        base.fecha_siniestro
        , base.fecha_registro
        , base.codigo_ramo_op
        , base.siniestro_id
        , base.apertura_canal_desc
        , base.apertura_amparo_desc
        , CASE
            WHEN base.apertura_canal_desc = 'CERRADO' THEN 'MOTO'
            ELSE COALESCE(base.tipo_vehiculo, '-1')
        END AS tipo_vehiculo
        , 0 AS atipico
        , SUM(base.pago_bruto) AS pago_bruto
        , SUM(base.pago_retenido) AS pago_retenido
        , SUM(base.aviso_bruto) AS aviso_bruto
        , SUM(base.aviso_retenido) AS aviso_retenido

    FROM base_bruto AS base
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
WITH DATA PRIMARY INDEX
(
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , siniestro_id
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
) ON COMMIT PRESERVE ROWS;


-- DROP TABLE CONTEO_PAGO_BRUTO;
CREATE MULTISET VOLATILE TABLE conteo_pago_bruto AS
(
    SELECT
        fecha_siniestro
        , codigo_ramo_op
        , apertura_canal_desc
        , apertura_amparo_desc
        , tipo_vehiculo
        , fecha_registro
        , atipico
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_pago

    FROM
        (
            SELECT
                fecha_siniestro
                , codigo_ramo_op
                , apertura_canal_desc
                , apertura_amparo_desc
                , tipo_vehiculo
                , siniestro_id
                , atipico
                , MIN(fecha_registro) AS fecha_registro

            FROM base_incurrido
            WHERE pago_bruto <> 0
            GROUP BY 1, 2, 3, 4, 5, 6, 7
        ) AS fmin
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)
WITH DATA PRIMARY INDEX
(
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
) ON COMMIT PRESERVE ROWS;


-- DROP TABLE CONTEO_INCURRIDO_BRUTO;
CREATE MULTISET VOLATILE TABLE conteo_incurrido_bruto AS
(
    SELECT
        fecha_siniestro
        , codigo_ramo_op
        , fecha_registro
        , apertura_canal_desc
        , apertura_amparo_desc
        , tipo_vehiculo
        , atipico
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_incurrido

    FROM
        (
            SELECT
                fecha_siniestro
                , codigo_ramo_op
                , siniestro_id
                , apertura_canal_desc
                , apertura_amparo_desc
                , tipo_vehiculo
                , atipico
                , MIN(fecha_registro) AS fecha_registro

            FROM base_incurrido
            WHERE NOT (pago_bruto = 0 AND aviso_bruto = 0)
            GROUP BY 1, 2, 3, 4, 5, 6, 7
        ) AS fmin
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)
WITH DATA PRIMARY INDEX
(
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
) ON COMMIT PRESERVE ROWS;


-- DROP TABLE SIN_PAGOS_BRUTO;
CREATE MULTISET VOLATILE TABLE sin_pagos_bruto AS
(
    SELECT
        base.fecha_siniestro
        , base.codigo_ramo_op
        , base.siniestro_id
        , base.apertura_canal_desc
        , base.apertura_amparo_desc
        , base.tipo_vehiculo
        , base.atipico
        , ZEROIFNULL(rva.en_reserva) AS en_rva
        , ZEROIFNULL(SUM(base.pago_bruto)) AS pago

    FROM base_incurrido AS base
    LEFT JOIN
        (
            SELECT
                fecha_siniestro
                , codigo_ramo_op
                , siniestro_id
                , apertura_canal_desc
                , apertura_amparo_desc
                , tipo_vehiculo
                , atipico
                , 1 AS en_reserva
                , SUM(aviso_bruto) AS aviso_bruto

            FROM base_incurrido
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            HAVING SUM(aviso_bruto) > 1000
        ) AS rva
        ON
            (base.fecha_siniestro = rva.fecha_siniestro)
            AND (base.codigo_ramo_op = rva.codigo_ramo_op)
            AND (base.siniestro_id = rva.siniestro_id)
            AND (base.apertura_canal_desc = rva.apertura_canal_desc)
            AND (base.apertura_amparo_desc = rva.apertura_amparo_desc)
            AND (base.tipo_vehiculo = rva.tipo_vehiculo)
            AND (base.atipico = rva.atipico)

    WHERE en_rva = 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    HAVING pago = 0
)
WITH DATA PRIMARY INDEX
(
    fecha_siniestro
    , codigo_ramo_op
    , siniestro_id
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
) ON COMMIT PRESERVE ROWS;


-- DROP TABLE CONTEO_DESISTIDO_BRUTO;
CREATE MULTISET VOLATILE TABLE conteo_desistido_bruto AS
(
    SELECT
        fecha_siniestro
        , codigo_ramo_op
        , fecha_registro
        , apertura_canal_desc
        , apertura_amparo_desc
        , tipo_vehiculo
        , atipico
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_desistido

    FROM
        (
            SELECT
                base.fecha_siniestro
                , base.codigo_ramo_op
                , base.siniestro_id
                , base.apertura_canal_desc
                , base.apertura_amparo_desc
                , base.tipo_vehiculo
                , base.atipico
                , MAX(base.fecha_registro) AS fecha_registro

            FROM base_incurrido AS base
            INNER JOIN sin_pagos_bruto AS pag
                ON
                    (base.fecha_siniestro = pag.fecha_siniestro)
                    AND (base.codigo_ramo_op = pag.codigo_ramo_op)
                    AND (base.siniestro_id = pag.siniestro_id)
                    AND (base.apertura_canal_desc = pag.apertura_canal_desc)
                    AND (base.apertura_amparo_desc = pag.apertura_amparo_desc)
                    AND (base.tipo_vehiculo = pag.tipo_vehiculo)
                    AND (base.atipico = pag.atipico)
            WHERE base.aviso_bruto <> 0
            GROUP BY 1, 2, 3, 4, 5, 6, 7
        ) AS fecha

    GROUP BY 1, 2, 3, 4, 5, 6, 7
)
WITH DATA PRIMARY INDEX
(
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
) ON COMMIT PRESERVE ROWS;


-- DROP TABLE BASE_PAGOS_AVISO;
CREATE MULTISET VOLATILE TABLE base_pagos_aviso AS
(
    SELECT
        fecha_siniestro
        , codigo_ramo_op
        , fecha_registro
        , apertura_canal_desc
        , apertura_amparo_desc
        , tipo_vehiculo
        , atipico
        , ZEROIFNULL(SUM(pago_bruto)) AS pago_bruto
        , ZEROIFNULL(SUM(pago_retenido)) AS pago_retenido
        , ZEROIFNULL(SUM(aviso_bruto)) AS aviso_bruto
        , ZEROIFNULL(SUM(aviso_retenido)) AS aviso_retenido

    FROM base_incurrido
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)
WITH DATA PRIMARY INDEX
(
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE fechas AS
(
    SELECT DISTINCT
        dia_dt
        , MIN(dia_dt) OVER (PARTITION BY mes_id) AS mes_id
    FROM mdb_seguros_colombia.v_dia
) WITH DATA PRIMARY INDEX (dia_dt) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE siniestros_final AS (
    SELECT
        '01' AS codigo_op
        , '041' AS codigo_ramo_op
        , 'AUTOS OBLIGATORIO' AS ramo_desc
        , COALESCE(base.apertura_canal_desc, '-1') AS apertura_canal_desc
        , COALESCE(base.apertura_amparo_desc, '-1') AS apertura_amparo_desc
        , COALESCE(base.tipo_vehiculo, '-1') AS tipo_vehiculo
        , base.atipico

        , GREATEST(CAST('{fecha_primera_ocurrencia}' AS DATE), focurr.mes_id)
            AS fecha_siniestro
        , GREATEST(CAST('{fecha_primera_ocurrencia}' AS DATE), fmov.mes_id)
            AS fecha_registro
        , ZEROIFNULL(SUM(contp.conteo_pago)) AS conteo_pago
        , ZEROIFNULL(SUM(conti.conteo_incurrido)) AS conteo_incurrido
        , ZEROIFNULL(SUM(contd.conteo_desistido)) AS conteo_desistido
        , ZEROIFNULL(SUM(base.pago_bruto)) AS pago_bruto
        , ZEROIFNULL(SUM(base.pago_retenido)) AS pago_retenido
        , ZEROIFNULL(SUM(base.aviso_bruto)) AS aviso_bruto
        , ZEROIFNULL(SUM(base.aviso_retenido)) AS aviso_retenido

    FROM base_pagos_aviso AS base
    LEFT JOIN conteo_pago_bruto AS contp
        ON
            (base.fecha_siniestro = contp.fecha_siniestro)
            AND (base.fecha_registro = contp.fecha_registro)
            AND (base.codigo_ramo_op = contp.codigo_ramo_op)
            AND (base.apertura_amparo_desc = contp.apertura_amparo_desc)
            AND (base.apertura_canal_desc = contp.apertura_canal_desc)
            AND (base.tipo_vehiculo = contp.tipo_vehiculo)
            AND (base.atipico = contp.atipico)
    LEFT JOIN conteo_incurrido_bruto AS conti
        ON
            (base.fecha_siniestro = conti.fecha_siniestro)
            AND (base.fecha_registro = conti.fecha_registro)
            AND (base.codigo_ramo_op = conti.codigo_ramo_op)
            AND (base.apertura_amparo_desc = conti.apertura_amparo_desc)
            AND (base.apertura_canal_desc = conti.apertura_canal_desc)
            AND (base.tipo_vehiculo = conti.tipo_vehiculo)
            AND (base.atipico) = conti.atipico
    LEFT JOIN conteo_desistido_bruto AS contd
        ON
            (base.fecha_siniestro = contd.fecha_siniestro)
            AND (base.fecha_registro = contd.fecha_registro)
            AND (base.codigo_ramo_op = contd.codigo_ramo_op)
            AND (base.apertura_amparo_desc = contd.apertura_amparo_desc)
            AND (base.apertura_canal_desc = contd.apertura_canal_desc)
            AND (base.tipo_vehiculo = contd.tipo_vehiculo)
            AND (base.atipico = contd.atipico)

    LEFT JOIN
        mdb_seguros_colombia.v_ramo AS ramo
        ON (base.codigo_ramo_op = ramo.codigo_ramo_op)
    INNER JOIN fechas AS focurr ON (base.fecha_siniestro = focurr.dia_dt)
    INNER JOIN fechas AS fmov ON (base.fecha_registro = fmov.dia_dt)

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

    HAVING NOT (
        ZEROIFNULL(SUM(contp.conteo_pago)) = 0
        AND ZEROIFNULL(SUM(conti.conteo_incurrido)) = 0
        AND ZEROIFNULL(SUM(contd.conteo_desistido)) = 0
        AND ZEROIFNULL(SUM(base.pago_bruto)) = 0
        AND ZEROIFNULL(SUM(base.aviso_bruto)) = 0
        AND ZEROIFNULL(SUM(base.pago_retenido)) = 0
        AND ZEROIFNULL(SUM(base.aviso_retenido)) = 0
    )

) WITH DATA PRIMARY INDEX (
    ramo_desc
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
    , fecha_siniestro
    , fecha_registro
) ON COMMIT PRESERVE ROWS;

SELECT * FROM siniestros_final ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9  -- noqa:

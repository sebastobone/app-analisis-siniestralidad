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


CREATE MULTISET VOLATILE TABLE fechas AS
(
    SELECT DISTINCT
        mes_id
        , MIN(dia_dt) OVER (PARTITION BY mes_id) AS primer_dia_mes
        , MAX(dia_dt) OVER (PARTITION BY mes_id) AS ultimo_dia_mes
        , CAST((ultimo_dia_mes - primer_dia_mes + 1) * 1.00 AS DECIMAL(18, 0))
            AS num_dias_mes
    FROM mdb_seguros_colombia.v_dia
    WHERE
        mes_id BETWEEN CAST('{mes_primera_ocurrencia}' AS INTEGER)
        - 100 AND CAST('{mes_corte}' AS INTEGER)
) WITH DATA PRIMARY INDEX (mes_id) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON fechas INDEX (mes_id);  -- noqa:


CREATE MULTISET VOLATILE TABLE primas AS
(
    SELECT
        fechas.primer_dia_mes
        , YEAR(fechas.primer_dia_mes) * 100
        + MONTH(fechas.primer_dia_mes) AS mes_id
        , '041' AS codigo_ramo_op
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
        , 'Total' AS apertura_amparo_desc
        , COALESCE(vehi.tipo_vehiculo, 'MOTO') AS tipo_vehiculo
        , COALESCE(spoli.descuento, 'No') AS descuento

        , SUM(evpro.valor_prima * evpro.valor_tasa) AS prima_emitida

    FROM mdb_seguros_colombia.v_evento_produccion AS evpro
    INNER JOIN fechas ON (evpro.mes_id = fechas.mes_id)
    INNER JOIN
        mdb_seguros_colombia.v_plan_individual AS plan
        ON (evpro.plan_individual_id = plan.plan_individual_id)
    INNER JOIN
        mdb_seguros_colombia.v_producto AS pro
        ON (plan.producto_id = pro.producto_id)
    INNER JOIN
        mdb_seguros_colombia.v_ramo AS ramo
        ON (pro.ramo_id = ramo.ramo_id)
    INNER JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON (evpro.poliza_id = poli.poliza_id)
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
        (
            SELECT
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
            FROM mdb_seguros_colombia.vsoat_poliza AS sp QUALIFY
                1
                = ROW_NUMBER()
                    OVER (
                        PARTITION BY sp.poliza_id ORDER BY sp.clase_soat_cd ASC
                    )
        ) AS spoli
        ON poli.poliza_id = spoli.poliza_id

    LEFT JOIN
        (SELECT DISTINCT
            codigo_ramo_op
            , nombre_canal_comercial
            , apertura_canal_desc
        FROM sucursales) AS s
        ON
            (
                ramo.codigo_ramo_op = s.codigo_ramo_op
                AND cacomer.nombre_canal_comercial = s.nombre_canal_comercial
            )
    LEFT JOIN
        (SELECT DISTINCT
            codigo_ramo_op
            , amparo_desc
            , apertura_amparo_desc
        FROM amparos) AS amparo
        ON
            (
                ramo.codigo_ramo_op = amparo.codigo_ramo_op
                AND apertura_amparo_desc = amparo.amparo_desc
            )
    LEFT JOIN
        (SELECT DISTINCT
            codigo_tarifa_cd
            , descuento
            , tipo_vehiculo
        FROM clases) AS vehi
        ON
            (
                spoli.codigo_tarifa_cd = vehi.codigo_tarifa_cd
                AND spoli.descuento = vehi.descuento
            )


    WHERE ramo.codigo_ramo_op IN ('041')

    GROUP BY 1, 2, 3, 4, 5, 6, 7

) WITH DATA PRIMARY INDEX (
    primer_dia_mes
    , mes_id
    , codigo_ramo_op
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
    , descuento
) ON COMMIT PRESERVE ROWS;

-- DROP TABLE PRIMAS_AUX;
CREATE MULTISET VOLATILE TABLE primas_aux AS
(
    SELECT
        primer_dia_mes
        , mes_id
        , codigo_ramo_op
        , apertura_canal_desc
        , apertura_amparo_desc
        , CASE
            WHEN apertura_canal_desc = 'CERRADO' THEN 'MOTO' ELSE tipo_vehiculo
        END AS tipo_vehiculo
        , descuento
        , SUM(prima_emitida) AS prima_emitida
    FROM primas
    GROUP BY 1, 2, 3, 4, 5, 6, 7
) WITH DATA PRIMARY INDEX (
    primer_dia_mes
    , mes_id
    , codigo_ramo_op
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
    , descuento
) ON COMMIT PRESERVE ROWS;

-- DROP TABLE PRIMAS2;
CREATE MULTISET VOLATILE TABLE primas2 AS
(
    SELECT
        'SOAT' AS ramo_desc
        , COALESCE(base.apertura_canal_desc, '-1') AS apertura_canal_desc
        , COALESCE(base.apertura_amparo_desc, '-1') AS apertura_amparo_desc
        , COALESCE(base.tipo_vehiculo, '-1') AS tipo_vehiculo

        , COALESCE(base.descuento, 'No') AS descuento

        , CONCAT(
            base.codigo_ramo_op, '_'
            , agrcanal.apertura_canal_cd, '_'
            , agrclas.tipo_vehiculo_cd, '_'
            , agrampa.apertura_amparo_cd
        ) AS agrupacion_reservas

        , base.primer_dia_mes AS mes_id
        , base.mes_id AS mes_aux_id


        , SUM(CAST(base.prima_emitida AS FLOAT)) AS prima_emitida

    FROM primas_aux AS base
    LEFT JOIN
        (SELECT DISTINCT
            apertura_canal_desc
            , apertura_canal_cd
        FROM sucursales) AS agrcanal
        ON (base.apertura_canal_desc = agrcanal.apertura_canal_desc)
    LEFT JOIN
        (SELECT DISTINCT
            tipo_vehiculo
            , tipo_vehiculo_cd
            , descuento
        FROM clases) AS agrclas
        ON
            (
                base.tipo_vehiculo = agrclas.tipo_vehiculo
                AND base.descuento = agrclas.descuento
            )
    LEFT JOIN
        (SELECT DISTINCT
            apertura_amparo_desc
            , apertura_amparo_cd
        FROM amparos) AS agrampa
        ON (base.apertura_amparo_desc = agrampa.apertura_amparo_desc)

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

) WITH DATA PRIMARY INDEX (
    mes_id
    , mes_aux_id
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
    , descuento
    , agrupacion_reservas
) ON COMMIT PRESERVE ROWS;

--Cada canal debe tener REGISTRO en todas las ocurrencias. Asi sea en 0.
CREATE VOLATILE TABLE base_prima3 AS (
    SELECT
        cat.mes_id
        , cat.mes_aux_id
        , cat.apertura_canal_desc
        , cat.apertura_amparo_desc
        , cat.tipo_vehiculo
        , cat.descuento
        , cat.agrupacion_reservas
        , ZEROIFNULL(v.prima_emitida) AS prima_emitida
    FROM
        (
            SELECT * FROM (SELECT DISTINCT
                mes_id
                , mes_aux_id
            FROM primas2) AS mes LEFT JOIN (SELECT
                apertura_canal_desc
                , apertura_amparo_desc
                , tipo_vehiculo
                , descuento
                , agrupacion_reservas
            FROM primas2 GROUP BY 1, 2, 3, 4, 5) AS cat ON 1 = 1
        ) AS cat
    LEFT JOIN primas2
        AS v ON cat.apertura_canal_desc = v.apertura_canal_desc
    AND cat.apertura_amparo_desc = v.apertura_amparo_desc
    AND cat.tipo_vehiculo = v.tipo_vehiculo
    AND cat.agrupacion_reservas = v.agrupacion_reservas
    AND cat.mes_id = v.mes_id
    AND cat.descuento = v.descuento

) WITH DATA PRIMARY INDEX (
    mes_id
    , mes_aux_id
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
    , descuento
    , agrupacion_reservas
) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE primas_final AS (
    SELECT
        '01' AS codigo_op
        , '041' AS codigo_ramo_op
        , COALESCE(m.apertura_canal_desc, s.apertura_canal_desc, '-1')
            AS apertura_canal_desc
        , COALESCE(m.apertura_amparo_desc, s.apertura_amparo_desc, '-1')
            AS apertura_amparo_desc
        , COALESCE(m.tipo_vehiculo, s.tipo_vehiculo, '-1') AS tipo_vehiculo
        , COALESCE(m.descuento, s.descuento, '-1') AS descuento
        , COALESCE(m.agrupacion_reservas, s.agrupacion_reservas, '-1')
            AS agrupacion_reservas
        , COALESCE(m.mes_id, s.mes_id) AS fecha_registro

        , ZEROIFNULL(m.prima_emitida) AS prima_bruta
        , SUM(CASE
            WHEN
                s.mes_aux_id BETWEEN m.mes_aux_id - 100 + 1 AND m.mes_aux_id - 1
                THEN (s.prima_emitida / 12)
            WHEN s.mes_aux_id = m.mes_aux_id - 100 THEN (s.prima_emitida / 24)
            WHEN s.mes_aux_id = m.mes_aux_id THEN (s.prima_emitida / 24)
            ELSE CAST(0 AS FLOAT)
        END) AS prima_bruta_devengada
        , prima_bruta AS prima_retenida
        --,Prima_Bruta_Devengada AS Prima_Retenida_Devengada
        , CASE
            WHEN
                COALESCE(m.descuento, s.descuento, '-1') = 'Si'
                AND COALESCE(m.mes_id, s.mes_id) = (DATE '2022-12-01')
                THEN prima_bruta_devengada * 2
            WHEN
                COALESCE(m.descuento, s.descuento, '-1') = 'Si'
                AND COALESCE(m.mes_id, s.mes_id) BETWEEN (
                    DATE '2023-01-01'
                ) AND (DATE '2023-12-01')
                THEN prima_bruta_devengada * 2.252
            --Nuevo calculo de los que si tienen descuento 2.255, anteror 2.252 
            WHEN
                COALESCE(m.descuento, s.descuento, '-1') = 'Si'
                AND COALESCE(m.mes_id, s.mes_id) > (DATE '2023-12-01')
                THEN prima_bruta_devengada * 2.255
            WHEN
                COALESCE(m.descuento, s.descuento, '-1') = 'No'
                AND COALESCE(m.mes_id, s.mes_id) BETWEEN (
                    DATE '2023-01-01'
                ) AND (DATE '2023-12-01')
                THEN prima_bruta_devengada * 1.0035
            WHEN COALESCE(m.descuento, s.descuento, '-1') = 'No' AND COALESCE(m.mes_id, s.mes_id) > (DATE '2023-12-01') THEN prima_bruta_devengada * 0.941 --Nuevo calculo de los que si tienen descuento 0.941 , anteror 1.0035
            ELSE prima_bruta_devengada
        END AS prima_devengada_mod

    FROM base_prima3 AS m
    LEFT JOIN base_prima3 AS s  --ON m.Mes_Id=s.Mes_Id
        ON
            m.apertura_canal_desc = s.apertura_canal_desc
            AND m.apertura_amparo_desc = s.apertura_amparo_desc
            AND m.tipo_vehiculo = s.tipo_vehiculo
            AND m.agrupacion_reservas = s.agrupacion_reservas
            AND m.descuento = s.descuento

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
) WITH DATA PRIMARY INDEX (
    apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
    , descuento
    , agrupacion_reservas
    , fecha_registro
) ON COMMIT PRESERVE ROWS;


SELECT
    codigo_op
    , codigo_ramo_op
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
    , fecha_registro
    , SUM(prima_bruta) AS prima_bruta
    , SUM(prima_bruta) AS prima_retenida
    , SUM(prima_bruta_devengada) AS prima_bruta_devengada
    , SUM(prima_bruta_devengada) AS prima_retenida_devengada
    , SUM(prima_devengada_mod) AS prima_devengada_mod
FROM primas_final
WHERE fecha_registro >= (DATE '{fecha_primera_ocurrencia}')
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6

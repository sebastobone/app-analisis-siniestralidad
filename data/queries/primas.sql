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
        , CAST((ultimo_dia_mes - primer_dia_mes + 1) * 1.00 AS DECIMAL(18, 0))
            AS num_dias_mes
    FROM mdb_seguros_colombia.v_dia
    WHERE mes_id BETWEEN '{mes_primera_ocurrencia}' AND '{mes_corte}'
) WITH DATA PRIMARY INDEX (mes_id) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE primas AS
(
    SELECT
        primer_dia_mes
        , codigo_op
        , codigo_ramo_aux AS codigo_ramo_op
        , ramo_desc
        , apertura_canal_aux AS apertura_canal_desc
        , apertura_amparo_desc
        , SUM(prima_bruta) AS prima_bruta
        , SUM(prima_bruta_devengada) AS prima_bruta_devengada
        , SUM(prima_retenida) AS prima_retenida
        , SUM(prima_retenida_devengada) AS prima_retenida_devengada

    FROM (
        SELECT
            fechas.primer_dia_mes
            , cia.codigo_op
            , CASE
                WHEN
                    rtdc.ramo_id = 78
                    AND rtdc.amparo_id NOT IN (18647, 641, 930, 64082, 61296)
                    THEN 'AAV'
                ELSE ramo.codigo_ramo_op
            END AS codigo_ramo_aux
            , CASE
                WHEN codigo_ramo_aux = 'AAV'
                    THEN 'AMPAROS ADICIONALES DE VIDA'
                ELSE ramo.ramo_desc
            END AS ramo_desc
            , COALESCE(
                p.apertura_canal_desc
                , COALESCE(
                    s.apertura_canal_desc
                    , CASE
                        WHEN
                            rtdc.ramo_id IN (78, 274)
                            AND pro.compania_id = 3
                            THEN 'NO BANCA'
                        ELSE 'RESTO'
                    END
                )
            ) AS apertura_canal_aux
            , COALESCE(amparo.apertura_amparo_desc, 'RESTO')
                AS apertura_amparo_desc
            , SUM(
                CASE
                    WHEN
                        n1.nivel_indicador_uno_id IN (1)
                        THEN rtdc.valor_indicador
                    ELSE 0
                END
            ) AS prima_bruta
            , SUM(
                CASE
                    WHEN
                        n1.nivel_indicador_uno_id IN (1, 5)
                        THEN rtdc.valor_indicador
                    ELSE 0
                END
            ) AS prima_bruta_devengada
            , SUM(
                CASE
                    WHEN
                        n1.nivel_indicador_uno_id IN (1, 2)
                        THEN rtdc.valor_indicador
                    ELSE 0
                END
            ) AS prima_retenida
            , SUM(
                CASE
                    WHEN
                        n1.nivel_indicador_uno_id IN (1, 2, 5)
                        THEN rtdc.valor_indicador
                    ELSE 0
                END
            ) AS prima_retenida_devengada

        FROM mdb_seguros_colombia.v_rt_detalle_cobertura AS rtdc
        INNER JOIN
            mdb_seguros_colombia.v_rt_nivel_indicador_cinco AS n5
            ON (rtdc.nivel_indicador_cinco_id = n5.nivel_indicador_cinco_id)
        INNER JOIN
            mdb_seguros_colombia.v_rt_nivel_indicador_uno AS n1
            ON
                (
                    n5.nivel_indicador_uno_id = n1.nivel_indicador_uno_id
                    AND n5.compania_origen_id = n1.compania_origen_id
                )
        INNER JOIN
            mdb_seguros_colombia.v_plan_individual AS pind
            ON (rtdc.plan_individual_id = pind.plan_individual_id)
        INNER JOIN
            mdb_seguros_colombia.v_producto AS pro
            ON (pind.producto_id = pro.producto_id)
        INNER JOIN
            mdb_seguros_colombia.v_ramo AS ramo
            ON (pro.ramo_id = ramo.ramo_id)
        INNER JOIN
            mdb_seguros_colombia.v_poliza AS poli
            ON (rtdc.poliza_id = poli.poliza_id)
        INNER JOIN
            mdb_seguros_colombia.v_compania AS cia
            ON (pro.compania_id = cia.compania_id)
        LEFT JOIN
            polizas AS p
            ON
                (
                    poli.numero_poliza = p.numero_poliza
                    AND rtdc.ramo_id = p.ramo_id
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
                AND rtdc.amparo_id = amparo.amparo_id
                AND pro.compania_id = amparo.compania_id
            )
        INNER JOIN fechas ON (rtdc.mes_id = fechas.mes_id)

        WHERE
            rtdc.ramo_id IN (54835, 57074, 78, 274, 140, 107, 271)
            AND pro.compania_id IN (3, 4)
            AND n1.nivel_indicador_uno_id IN (1, 2, 5)

        GROUP BY 1, 2, 3, 4, 5, 6

        UNION ALL

        SELECT
            fechas.primer_dia_mes
            , cia.codigo_op
            , CASE
                WHEN
                    ramo.ramo_desc = 'VIDA INDIVIDUAL'
                    AND rtdc.amparo_id NOT IN (18647, 641, 930, 64082, 61296)
                    THEN 'AAV'
                ELSE ramo.codigo_ramo_op
            END AS codigo_ramo_aux
            , CASE
                WHEN codigo_ramo_aux = 'AAV'
                    THEN 'AMPAROS ADICIONALES DE VIDA'
                ELSE ramo.ramo_desc
            END AS ramo_desc
            , COALESCE(
                p.apertura_canal_desc
                , COALESCE(
                    s.apertura_canal_desc
                    , CASE
                        WHEN
                            ramo.codigo_ramo_op IN ('081', '083')
                            THEN 'NO BANCA'
                        ELSE 'RESTO'
                    END
                )
            ) AS apertura_canal_aux
            , COALESCE(amparo.apertura_amparo_desc, 'RESTO')
                AS apertura_amparo_desc
            , SUM(
                CASE
                    WHEN
                        n1.nivel_indicador_uno_id IN (1)
                        THEN rtdc.valor_indicador
                    ELSE 0
                END
            ) AS prima_bruta
            , SUM(
                CASE
                    WHEN
                        n1.nivel_indicador_uno_id IN (1, 5)
                        THEN rtdc.valor_indicador
                    ELSE 0
                END
            ) AS prima_bruta_devengada
            , SUM(
                CASE
                    WHEN
                        n1.nivel_indicador_uno_id IN (1, 2)
                        THEN rtdc.valor_indicador
                    ELSE 0
                END
            ) AS prima_retenida
            , SUM(
                CASE
                    WHEN
                        n1.nivel_indicador_uno_id IN (1, 2, 5)
                        THEN rtdc.valor_indicador
                    ELSE 0
                END
            ) AS prima_retenida_devengada

        FROM mdb_seguros_colombia.v_rt_ramo_sucursal AS rtdc
        INNER JOIN
            mdb_seguros_colombia.v_rt_nivel_indicador_cinco AS n5
            ON (rtdc.nivel_indicador_cinco_id = n5.nivel_indicador_cinco_id)
        INNER JOIN
            mdb_seguros_colombia.v_rt_nivel_indicador_uno AS n1
            ON
                (
                    n5.nivel_indicador_uno_id = n1.nivel_indicador_uno_id
                    AND n5.compania_origen_id = n1.compania_origen_id
                )
        INNER JOIN
            mdb_seguros_colombia.v_ramo AS ramo
            ON (rtdc.ramo_id = ramo.ramo_id)
        INNER JOIN
            mdb_seguros_colombia.v_poliza AS poli
            ON (rtdc.poliza_id = poli.poliza_id)
        INNER JOIN
            mdb_seguros_colombia.v_compania AS cia
            ON (rtdc.compania_origen_id = cia.compania_id)
        LEFT JOIN
            polizas AS p
            ON
                (
                    poli.numero_poliza = p.numero_poliza
                    AND ramo.codigo_ramo_op = p.codigo_ramo_op
                )
        LEFT JOIN sucursales AS s
            ON (
                codigo_ramo_aux = s.codigo_ramo_op
                AND rtdc.sucursal_id = s.sucursal_id
                AND rtdc.compania_origen_id = s.compania_id
            )
        LEFT JOIN amparos AS amparo
            ON (
                codigo_ramo_aux = amparo.codigo_ramo_op
                AND apertura_canal_aux = amparo.apertura_canal_desc
                AND rtdc.amparo_id = amparo.amparo_id
                AND rtdc.compania_origen_id = amparo.compania_id
            )
        INNER JOIN fechas ON (rtdc.mes_id = fechas.mes_id)

        WHERE
            rtdc.ramo_id IN (54835, 57074, 78, 274, 140, 107, 271)
            AND rtdc.compania_origen_id IN (3, 4)
            AND n1.nivel_indicador_uno_id IN (1, 2, 5)

        GROUP BY 1, 2, 3, 4, 5, 6
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
    , ZEROIFNULL(SUM(base.prima_bruta)) AS prima_bruta
    , ZEROIFNULL(SUM(base.prima_bruta_devengada)) AS prima_bruta_devengada
    , ZEROIFNULL(SUM(base.prima_retenida)) AS prima_retenida
    , ZEROIFNULL(SUM(base.prima_retenida_devengada)) AS prima_retenida_devengada

FROM primas AS base

GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6

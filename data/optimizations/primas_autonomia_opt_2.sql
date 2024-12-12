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
INSERT INTO CANAL_POLIZA VALUES (?,?,?,?,?,?,?,?);  -- noqa:



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
INSERT INTO CANAL_CANAL VALUES (?,?,?,?,?,?,?,?);  -- noqa:



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
INSERT INTO CANAL_SUCURSAL VALUES (?,?,?,?,?,?,?,?);  -- noqa:



CREATE MULTISET VOLATILE TABLE fechas AS
(
    SELECT
        mes_id
        , MIN(dia_dt) AS primer_dia_mes
        , MAX(dia_dt) AS ultimo_dia_mes
        , CAST(ultimo_dia_mes - primer_dia_mes + 1 AS FLOAT) AS num_dias_mes
    FROM mdb_seguros_colombia.v_dia
    WHERE
        mes_id BETWEEN CAST('{mes_primera_ocurrencia}' AS INTEGER) AND CAST(
            '{mes_corte}' AS INTEGER
        )
    GROUP BY 1
) WITH DATA PRIMARY INDEX (mes_id) ON COMMIT PRESERVE ROWS;



EXPLAIN
WITH base AS (
    SELECT
        mes_id
        , codigo_op
        , codigo_ramo_aux AS codigo_ramo_op
        , apertura_canal_aux AS apertura_canal_desc
        , tipo_produccion
        , SUM(prima_bruta) AS prima_bruta
        , SUM(prima_bruta_devengada) AS prima_bruta_devengada
        , SUM(prima_retenida) AS prima_retenida
        , SUM(prima_retenida_devengada) AS prima_retenida_devengada

    FROM (
        SELECT
            fechas.mes_id
            , cia.codigo_op
            , CASE
                WHEN
                    rtdc.ramo_id = 78
                    AND rtdc.amparo_id NOT IN (18647, 641, 930, 64082, 61296, -1)
                    THEN 'AAV'
                ELSE ramo.codigo_ramo_op
            END AS codigo_ramo_aux
            , COALESCE(
                p.apertura_canal_desc, c.apertura_canal_desc, s.apertura_canal_desc
                , CASE
                    WHEN
                        rtdc.ramo_id IN (78, 274)
                        AND pro.compania_id = 3
                        THEN 'Otros Banca'
                    WHEN
                        rtdc.ramo_id = 274 AND pro.compania_id = 4
                        THEN 'Otros'
                    ELSE 'Resto'
                END
            ) AS apertura_canal_aux
            , CASE
                WHEN
                    n1.nivel_indicador_uno_id = 1 AND rtdc.valor_indicador < 0
                    THEN 'NEGATIVA'
                ELSE 'POSITIVA'
            END AS tipo_produccion
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
                n5.nivel_indicador_uno_id = n1.nivel_indicador_uno_id
                AND n5.compania_origen_id = n1.compania_origen_id
        INNER JOIN
            mdb_seguros_colombia.v_plan_individual AS plan
            ON (rtdc.plan_individual_id = plan.plan_individual_id)
        INNER JOIN
            mdb_seguros_colombia.v_producto AS pro
            ON (plan.producto_id = pro.producto_id)
        INNER JOIN
            mdb_seguros_colombia.v_ramo AS ramo
            ON (pro.ramo_id = ramo.ramo_id)
        INNER JOIN
            mdb_seguros_colombia.v_poliza AS poli
            ON (rtdc.poliza_id = poli.poliza_id)
        INNER JOIN
            mdb_seguros_colombia.v_compania AS cia
            ON (pro.compania_id = cia.compania_id)
        INNER JOIN
            mdb_seguros_colombia.v_sucursal AS sucu
            ON (poli.sucursal_id = sucu.sucursal_id)
        LEFT JOIN
            canal_poliza AS p
            ON
                (
                    rtdc.poliza_id = p.poliza_id
                    AND codigo_ramo_aux = p.codigo_ramo_op
                    AND cia.compania_id = p.compania_id
                )
        LEFT JOIN
            canal_canal AS c
            ON (
                codigo_ramo_aux = c.codigo_ramo_op
                AND sucu.canal_comercial_id = c.canal_comercial_id
                AND cia.compania_id = c.compania_id
            )
        LEFT JOIN
            canal_sucursal AS s
            ON (
                codigo_ramo_aux = s.codigo_ramo_op
                AND sucu.sucursal_id = s.sucursal_id
                AND cia.compania_id = s.compania_id
            )
        INNER JOIN fechas ON (rtdc.mes_id = fechas.mes_id)

        WHERE rtdc.ramo_id IN (54835, 274, 78, 57074, 140, 107, 271, 297, 204)
        AND pro.compania_id IN (3, 4)
        AND n1.nivel_indicador_uno_id IN (1, 2, 5)

        GROUP BY 1, 2, 3, 4, 5

        UNION ALL

        SELECT
            fechas.mes_id
            , cia.codigo_op
            , CASE
                WHEN
                    rtdc.ramo_id = 78
                    AND rtdc.amparo_id NOT IN (18647, 641, 930, 64082, 61296, -1)
                    THEN 'AAV'
                ELSE ramo.codigo_ramo_op
            END AS codigo_ramo_aux
            , COALESCE(
                p.apertura_canal_desc, c.apertura_canal_desc, s.apertura_canal_desc
                , CASE
                    WHEN
                        rtdc.ramo_id IN (78, 274)
                        AND rtdc.compania_origen_id = 3
                        THEN 'Otros Banca'
                    WHEN
                        rtdc.ramo_id = 274 AND rtdc.compania_origen_id = 4
                        THEN 'Otros'
                    ELSE 'Resto'
                END
            ) AS apertura_canal_aux
            , CASE
                WHEN
                    n1.nivel_indicador_uno_id = 1 AND rtdc.valor_indicador < 0
                    THEN 'NEGATIVA'
                ELSE 'POSITIVA'
            END AS tipo_produccion
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
                n5.nivel_indicador_uno_id = n1.nivel_indicador_uno_id
                AND n5.compania_origen_id = n1.compania_origen_id
        INNER JOIN
            mdb_seguros_colombia.v_ramo AS ramo
            ON (rtdc.ramo_id = ramo.ramo_id)
        INNER JOIN
            mdb_seguros_colombia.v_compania AS cia
            ON rtdc.compania_origen_id = cia.compania_id
        INNER JOIN
            mdb_seguros_colombia.v_poliza AS poli
            ON (rtdc.poliza_id = poli.poliza_id)
        INNER JOIN
            mdb_seguros_colombia.v_sucursal AS sucu
            ON (rtdc.sucursal_id = sucu.sucursal_id)
        LEFT JOIN
            canal_poliza AS p
            ON
                rtdc.poliza_id = p.poliza_id
                AND codigo_ramo_aux = p.codigo_ramo_op
                AND cia.compania_id = p.compania_id
        LEFT JOIN
            canal_canal AS c
            ON (
                codigo_ramo_aux = c.codigo_ramo_op
                AND sucu.canal_comercial_id = c.canal_comercial_id
                AND cia.compania_id = c.compania_id
            )
        LEFT JOIN
            canal_sucursal AS s
            ON (
                codigo_ramo_aux = s.codigo_ramo_op
                AND rtdc.sucursal_id = s.sucursal_id
                AND cia.compania_id = s.compania_id
            )
        INNER JOIN fechas ON (rtdc.mes_id = fechas.mes_id)

        WHERE rtdc.ramo_id IN (54835, 274, 78, 57074, 140, 107, 271, 297, 204)
        AND rtdc.compania_origen_id IN (3, 4)
        AND n1.nivel_indicador_uno_id IN (1, 2, 5)

        GROUP BY 1, 2, 3, 4, 5
    ) AS base2

    GROUP BY 1, 2, 3, 4, 5
)

SELECT TOP 100 * FROM base
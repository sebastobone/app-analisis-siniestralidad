CREATE MULTISET VOLATILE TABLE base_primas AS (
    WITH base_rt AS (
        SELECT
            rtdc.mes_id
            , SUM(
                CASE
                    WHEN
                        n5.nivel_indicador_uno_id IN (1)
                        THEN rtdc.valor_indicador
                    ELSE 0
                END
            ) AS prima_bruta
            , SUM(
                CASE
                    WHEN
                        n5.nivel_indicador_uno_id IN (1, 5)
                        THEN rtdc.valor_indicador
                    ELSE 0
                END
            ) AS prima_bruta_devengada
            , SUM(
                CASE
                    WHEN
                        n5.nivel_indicador_uno_id IN (1, 2)
                        THEN rtdc.valor_indicador
                    ELSE 0
                END
            ) AS prima_retenida
            , SUM(
                CASE
                    WHEN
                        n5.nivel_indicador_uno_id IN (1, 2, 5)
                        THEN rtdc.valor_indicador
                    ELSE 0
                END
            ) AS prima_retenida_devengada

        FROM mdb_seguros_colombia.v_rt_detalle_cobertura AS rtdc
        LEFT JOIN mdb_seguros_colombia.v_plan_individual_mstr AS pind
            ON rtdc.plan_individual_id = pind.plan_individual_id
        LEFT JOIN mdb_seguros_colombia.v_producto AS pro
            ON pind.producto_id = pro.producto_id
        LEFT JOIN mdb_seguros_colombia.v_rt_nivel_indicador_cinco AS n5
            ON rtdc.nivel_indicador_cinco_id = n5.nivel_indicador_cinco_id

        WHERE
            rtdc.ramo_id = 168
            AND pro.compania_id = 4
            AND n5.nivel_indicador_uno_id IN (1, 2, 5)
            AND rtdc.mes_id BETWEEN CAST('{mes_primera_ocurrencia}' AS INTEGER)
            AND CAST('{mes_corte}' AS INTEGER)

        GROUP BY 1

        UNION ALL

        SELECT
            rtrs.mes_id
            , SUM(
                CASE
                    WHEN
                        n5.nivel_indicador_uno_id IN (1)
                        THEN rtrs.valor_indicador
                    ELSE 0
                END
            ) AS prima_bruta
            , SUM(
                CASE
                    WHEN
                        n5.nivel_indicador_uno_id IN (1, 5)
                        THEN rtrs.valor_indicador
                    ELSE 0
                END
            ) AS prima_bruta_devengada
            , SUM(
                CASE
                    WHEN
                        n5.nivel_indicador_uno_id IN (1, 2)
                        THEN rtrs.valor_indicador
                    ELSE 0
                END
            ) AS prima_retenida
            , SUM(
                CASE
                    WHEN
                        n5.nivel_indicador_uno_id IN (1, 2, 5)
                        THEN rtrs.valor_indicador
                    ELSE 0
                END
            ) AS prima_retenida_devengada

        FROM mdb_seguros_colombia.v_rt_ramo_sucursal AS rtrs
        LEFT JOIN mdb_seguros_colombia.v_rt_nivel_indicador_cinco AS n5
            ON rtrs.nivel_indicador_cinco_id = n5.nivel_indicador_cinco_id

        WHERE
            rtrs.ramo_id = 168
            AND rtrs.compania_origen_id = 4
            AND n5.nivel_indicador_uno_id IN (1, 2, 5)
            AND rtrs.mes_id BETWEEN CAST('{mes_primera_ocurrencia}' AS INTEGER)
            AND CAST('{mes_corte}' AS INTEGER)

        GROUP BY 1
    )

    SELECT
        mes_id
        , SUM(prima_bruta) AS prima_bruta
        , SUM(prima_bruta_devengada) AS prima_bruta_devengada
        , SUM(prima_retenida) AS prima_retenida
        , SUM(prima_retenida_devengada) AS prima_retenida_devengada
    FROM base_rt
    GROUP BY 1

) WITH DATA PRIMARY INDEX (mes_id) ON COMMIT PRESERVE ROWS;


WITH fechas AS (
    SELECT
        mes_id
        , MIN(dia_dt) AS primer_dia_mes
    FROM mdb_seguros_colombia.v_dia
    GROUP BY 1
)

SELECT
    '01' AS codigo_op
    , '040' AS codigo_ramo_op
    , fechas.primer_dia_mes AS fecha_registro
    , base_primas.prima_bruta
    , base_primas.prima_bruta_devengada
    , base_primas.prima_retenida
    , base_primas.prima_retenida_devengada
FROM base_primas
LEFT JOIN fechas
    ON base_primas.mes_id = fechas.mes_id
ORDER BY 1, 2, 3

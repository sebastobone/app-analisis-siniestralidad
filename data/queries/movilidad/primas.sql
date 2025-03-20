CREATE MULTISET VOLATILE TABLE base_primas AS (
    WITH base_rt AS (
        SELECT
            rtdc.mes_id
            , CASE
                WHEN
                    fas.clase_tarifa_cd IN (3, 4, 5, 6)
                    AND pol.sucursal_id IN (21170919, 20056181, 52915901)
                    THEN 'MOTOS SUFI'
                WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) THEN 'MOTOS RESTO'
                WHEN rtdc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
                WHEN
                    rtdc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                    THEN 'RC'
                ELSE 'PARCIALES'
            END AS cobertura_general_desc
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
        LEFT JOIN mdb_seguros_colombia.v_poliza_certificado_etl AS pcetl
            ON rtdc.poliza_certificado_id = pcetl.poliza_certificado_id
        LEFT JOIN mdb_seguros_colombia.v_vehiculo AS vehi
            ON pcetl.bien_id = vehi.bien_id
        LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas
            ON vehi.fasecolda_cd = fas.fasecolda_cd
        LEFT JOIN mdb_seguros_colombia.v_poliza AS pol
            ON pcetl.poliza_id = pol.poliza_id

        WHERE
            rtdc.ramo_id = 168
            AND pro.compania_id = 4
            AND n5.nivel_indicador_uno_id IN (1, 2, 5)
            AND rtdc.mes_id BETWEEN CAST('{mes_primera_ocurrencia}' AS INTEGER)
            AND CAST('{mes_corte}' AS INTEGER)

        GROUP BY 1, 2

        UNION ALL

        SELECT
            rtrs.mes_id
            , CASE
                WHEN
                    fas.clase_tarifa_cd IN (3, 4, 5, 6)
                    AND rtrs.sucursal_id IN (21170919, 20056181, 52915901)
                    THEN 'MOTOS SUFI'
                WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) THEN 'MOTOS RESTO'
                WHEN rtrs.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
                WHEN
                    rtrs.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                    THEN 'RC'
                ELSE 'PARCIALES'
            END AS cobertura_general_desc
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
        LEFT JOIN mdb_seguros_colombia.v_poliza_certificado_etl AS pcetl
            ON rtrs.poliza_certificado_id = pcetl.poliza_certificado_id
        LEFT JOIN mdb_seguros_colombia.v_rt_nivel_indicador_cinco AS n5
            ON rtrs.nivel_indicador_cinco_id = n5.nivel_indicador_cinco_id
        LEFT JOIN mdb_seguros_colombia.v_vehiculo AS vehi
            ON pcetl.bien_id = vehi.bien_id
        LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas
            ON vehi.fasecolda_cd = fas.fasecolda_cd

        WHERE
            rtrs.ramo_id = 168
            AND rtrs.compania_origen_id = 4
            AND n5.nivel_indicador_uno_id IN (1, 2, 5)
            AND rtrs.mes_id BETWEEN CAST('{mes_primera_ocurrencia}' AS INTEGER)
            AND CAST('{mes_corte}' AS INTEGER)

        GROUP BY 1, 2
    )

    SELECT
        mes_id
        , cobertura_general_desc
        , SUM(prima_bruta) AS prima_bruta
        , SUM(prima_bruta_devengada) AS prima_bruta_devengada
        , SUM(prima_retenida) AS prima_retenida
        , SUM(prima_retenida_devengada) AS prima_retenida_devengada
    FROM base_rt
    GROUP BY 1, 2

) WITH DATA PRIMARY INDEX (
    mes_id, cobertura_general_desc
) ON COMMIT PRESERVE ROWS;


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
    , base_primas.cobertura_general_desc
    , fechas.primer_dia_mes AS fecha_registro
    , base_primas.prima_bruta
    , base_primas.prima_bruta_devengada
    , base_primas.prima_retenida
    , base_primas.prima_retenida_devengada
FROM base_primas
LEFT JOIN fechas
    ON base_primas.mes_id = fechas.mes_id
ORDER BY 1, 2, 3, 4

CREATE MULTISET VOLATILE TABLE fechas AS (
    SELECT
        mes_id
        , MIN(dia_dt) AS primer_dia_mes
        , MAX(dia_dt) AS ultimo_dia_mes
        , CAST(
            (ultimo_dia_mes - primer_dia_mes + 1) * 1.00 AS DECIMAL(18, 0)) AS
        num_dias_mes
    FROM mdb_seguros_colombia.v_dia
    WHERE
        mes_id BETWEEN CAST('{mes_primera_ocurrencia}' AS INTEGER)
        AND CAST('{mes_corte}' AS INTEGER)
    GROUP BY 1
) WITH DATA PRIMARY INDEX (mes_id) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON fechas INDEX (mes_id);  -- noqa:


CREATE MULTISET VOLATILE TABLE base_expuestos AS
(
    SELECT
        pc.poliza_certificado_id
        , CASE
            WHEN
                fas.clase_tarifa_cd IN (3, 4, 5, 6)
                AND pol.sucursal_id IN (21170919, 20056181, 52915901)
                THEN 'MOTOS SUFI'
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) THEN 'MOTOS RESTO'
            WHEN
                vpc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                THEN 'RC'
            WHEN vpc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
            ELSE 'PARCIALES'
        END AS cobertura_general_desc
        , pc.fecha_cancelacion
        , MIN(vpc.fecha_inclusion_cobertura) AS fecha_inclusion_cobertura
        , MAX(vpc.fecha_exclusion_cobertura) AS fecha_exclusion_cobertura

    FROM mdb_seguros_colombia.v_hist_polcert_cobertura AS vpc
    LEFT JOIN mdb_seguros_colombia.v_poliza_certificado AS pc
        ON
            vpc.poliza_certificado_id = pc.poliza_certificado_id
            AND vpc.plan_individual_id = pc.plan_individual_id
    LEFT JOIN mdb_seguros_colombia.v_plan_individual AS plan
        ON vpc.plan_individual_id = plan.plan_individual_id
    LEFT JOIN mdb_seguros_colombia.v_producto AS pro
        ON plan.producto_id = pro.producto_id
    LEFT JOIN mdb_seguros_colombia.v_poliza AS pol
        ON pc.poliza_id = pol.poliza_id
    LEFT JOIN mdb_seguros_colombia.v_vehiculo AS vehi
        ON pc.bien_id = vehi.bien_id
    LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas
        ON vehi.fasecolda_cd = fas.fasecolda_cd

    WHERE
        pro.ramo_id = 168
        AND pro.compania_id = 4

    GROUP BY 1, 2, 3
    HAVING
        MAX(vpc.fecha_exclusion_cobertura)
        >= (DATE '{fecha_primera_ocurrencia}')

) WITH DATA PRIMARY INDEX (
    poliza_certificado_id
    , cobertura_general_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE expuestos AS (
    SELECT
        primer_dia_mes
        , mes_id
        , cobertura_general_desc
        , SUM(expuestos) AS expuestos
        , SUM(vigentes) AS vigentes
    FROM (
        SELECT
            fechas.primer_dia_mes
            , fechas.mes_id
            , vpc.cobertura_general_desc
            , GREATEST(
                vpc.fecha_inclusion_cobertura
                , fechas.primer_dia_mes
            ) AS fecha_inicio
            , LEAST(
                vpc.fecha_exclusion_cobertura
                , fechas.ultimo_dia_mes
                , COALESCE(vpc.fecha_cancelacion, (DATE '3000-01-01'))
            ) AS fecha_fin
            , SUM(
                CAST(fecha_fin - fecha_inicio + 1 AS FLOAT)
                / fechas.num_dias_mes
            ) AS expuestos
            , SUM(1) AS vigentes
        FROM fechas
        INNER JOIN base_expuestos AS vpc
            ON
                fechas.ultimo_dia_mes >= vpc.fecha_inclusion_cobertura
                AND COALESCE(
                    vpc.fecha_cancelacion
                    , vpc.fecha_exclusion_cobertura
                    , (DATE '3000-01-01')
                ) >= fechas.primer_dia_mes
        GROUP BY 1, 2, 3, 4, 5
    ) AS base

    GROUP BY 1, 2, 3

) WITH DATA PRIMARY INDEX (
    primer_dia_mes
    , mes_id
    , cobertura_general_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE expuestos_1 AS (
    SELECT
        mes_id
        , cobertura_general_desc
        , primer_dia_mes AS fecha_registro
        , SUM(expuestos) AS expuestos
        , SUM(vigentes) AS vigentes
    FROM expuestos
    GROUP BY 1, 2, 3
) WITH DATA PRIMARY INDEX (
    mes_id
    , cobertura_general_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE expuestos_2 AS (
    WITH v100 AS (
        SELECT
            traz.*
            , CASE
                WHEN ((
                    traz.tipo_transaccion_txt IN ('Cambio en la póliza')
                )
                AND (
                    LAG(traz.valor_prima_anualizada, 1)
                        OVER (
                            PARTITION BY
                                traz.poliza_id
                                , traz.poliza_certificado_id
                                , CAST(traz.fecha_inicio AS DATE)
                            ORDER BY traz.fecha_transaccion
                        )
                ) IS NOT null
                AND (
                    LAG(traz.valor_prima_anualizada, 1)
                        OVER (
                            PARTITION BY
                                traz.poliza_id
                                , traz.poliza_certificado_id
                                , CAST(traz.fecha_inicio AS DATE)
                            ORDER BY traz.fecha_transaccion
                        )
                )
                <> traz.valor_prima_anualizada
                )
                    THEN 1
                ELSE 0
            END AS cambio_valorable
            , CASE
                WHEN
                    CAST(traz.fecha_transaccion AS DATE) BETWEEN CAST(
                        traz.fecha_inicio AS DATE
                    ) AND CAST(traz.fecha_fin AS DATE)
                    THEN 1
                ELSE 0
            END AS dentro_vigencia

        FROM mdb_seguros_colombia.ve_seg_tarif_traz_cert AS traz
    )


    , v200 AS (
        SELECT
            v100.*
            , CASE
                WHEN
                    (
                        LAG(v100.fecha_transaccion, 1)
                            OVER (
                                PARTITION BY
                                    v100.poliza_id
                                    , v100.poliza_certificado_id
                                    , CAST(v100.fecha_inicio AS DATE)
                                ORDER BY v100.fecha_transaccion
                            )
                    ) IS null
                    THEN CAST(v100.fecha_inicio AS DATE)
                ELSE v100.fecha_transaccion
            END AS fecha_inicio_v2
            , COALESCE(
                (
                    LEAD(CAST(v100.fecha_transaccion AS DATE), 1)
                        OVER (
                            PARTITION BY
                                v100.poliza_id
                                , v100.poliza_certificado_id
                                , CAST(v100.fecha_inicio AS DATE)
                            ORDER BY v100.fecha_transaccion
                        )
                )
                - (CASE
                    WHEN
                        LEAD(CAST(v100.fecha_transaccion AS DATE), 1)
                            OVER (
                                PARTITION BY
                                    v100.poliza_id
                                    , v100.poliza_certificado_id
                                    , CAST(v100.fecha_inicio AS DATE)
                                ORDER BY v100.fecha_transaccion
                            )
                        = CAST(v100.fecha_transaccion AS DATE) THEN 0
                    ELSE 1
                END), CAST(v100.fecha_fin AS DATE)
            ) AS fecha_finv2
        FROM v100
    )

    , v300 AS (
        SELECT
            v200.*
            , FIRST_VALUE(v200.fecha_inicio_v2)
                OVER (
                    PARTITION BY 
                        v200.poliza_id
                        , v200.poliza_certificado_id
                        , CAST(v200.fecha_inicio AS DATE) 
                    ORDER BY v200.fecha_transaccion 
                    RESET 
                        WHEN 
                            v200.cambio_valorable IN(1)
                ) AS fecha_inicio_v3 
            , MAX(v200.fecha_finv2) 
                OVER (
                    PARTITION BY 
                        v200.poliza_id
                        , v200.poliza_certificado_id
                        , CAST(v200.fecha_inicio AS DATE) 
                    ORDER BY v200.fecha_transaccion 
                    RESET 
                        WHEN 
                            v200.cambio_valorable IN(1)
                ) AS fecha_fin_v3
        FROM v200
    )

    , v400 AS (
        SELECT 
            v300.*
            , ROW_NUMBER()
                OVER(
                    PARTITION BY   
                        v300.poliza_id
                        , v300.poliza_certificado_id
                        , CAST(v300.fecha_inicio AS DATE)
                    ORDER BY fecha_transaccion  
                    RESET 
                        WHEN 
                            (v300.cambio_valorable*v300.dentro_vigencia) IN (1)
                    ) AS recuento
            ,CASE 
                WHEN (
                        v300.tipo_transaccion_txt = 'Cancelación' 
                        AND LAG(v300.tipo_transaccion_txt)
                            OVER(
                                    PARTITION BY   
                                        v300.poliza_id
                                        , v300.poliza_certificado_id
                                    ORDER BY v300.fecha_transaccion
                                ) = 'Cancelación'
                        ) THEN LAG(v300.valor_prima_anualizada,2) 
                            OVER (
                                    PARTITION BY   
                                        v300.poliza_id
                                        , v300.poliza_certificado_id 
                                    ORDER BY v300.fecha_transaccion
                                ) 
                ELSE LAG(v300.valor_prima_anualizada)
                    OVER(
                        PARTITION BY
                            v300.poliza_id
                            , v300.poliza_certificado_id 
                        ORDER BY v300.fecha_transaccion
                        ) 
            END AS prima_transaccion_anterior
            ,CASE 
                WHEN (
                        v300.tipo_transaccion_txt = 'Cancelación' 
                        AND LAG(v300.tipo_transaccion_txt)
                            OVER(
                                PARTITION BY
                                    v300.poliza_id
                                    , v300.poliza_certificado_id 
                                ORDER BY v300.fecha_transaccion
                            ) = 'Cancelación'
                        ) THEN LAG(v300.tipo_transaccion_txt,2) 
                            OVER (
                                PARTITION BY   
                                    v300.poliza_id
                                    , v300.poliza_certificado_id 
                                ORDER BY v300.fecha_transaccion
                            ) 
                ELSE LAG(v300.tipo_transaccion_txt) 
                    OVER (
                        PARTITION BY
                            v300.poliza_id
                            , v300.poliza_certificado_id 
                        ORDER BY v300.fecha_transaccion
                    )
            END AS tipo_transaccion_anterior
            ,CASE
                WHEN (
                    v300.tipo_transaccion_txt = 'Cancelación' 
                    AND LAG(v300.tipo_transaccion_txt)
                        OVER(
                            PARTITION BY   
                            v300.poliza_id
                            , v300.poliza_certificado_id 
                        ORDER BY v300.fecha_transaccion
                    ) = 'Cancelación'
                ) THEN LAG(v300.numero_transaccion,2) 
                    OVER (
                        PARTITION BY   
                        v300.poliza_id
                        , v300.poliza_certificado_id
                        ORDER BY v300.fecha_transaccion
                    ) 
                ELSE LAG(v300.numero_transaccion)
                    OVER(
                        PARTITION BY   
                        v300.poliza_id
                        , v300.poliza_certificado_id
                    ORDER BY v300.fecha_transaccion
                )
                END AS numero_transaccion_anterior
        FROM v300
    )

    , v500 AS (
        SELECT
            v400.*
            , CASE
                WHEN
                    v400.tipo_transaccion_txt IN ('Cancelación')
                    THEN COALESCE(
                            v400.fecha_cancelacion_poliza
                            , v400.fecha_transaccion)
            END AS fecha_cancelacion_vigencia --- Como hay cancelaciones retriactivas a veces esas quedan sin "fecha de cancelacion" por tanto si la transacción se llama "cancelacion" le asgnamos que su fecha de cancelación es la fecha de trasacccion

        FROM v400
        QUALIFY recuento = MAX(recuento) 
            OVER(
                PARTITION BY   
                v400.poliza_id
                , v400.poliza_certificado_id
                , CAST(v400.fecha_inicio AS DATE)
            ORDER BY fecha_transaccion  
            RESET 
                WHEN (v400.cambio_valorable*v400.dentro_vigencia) IN(1)
        )
    )

    , v600 AS (
        SELECT 
            v500.*
            ,CASE 
                WHEN ((
                    v500.fecha_fin_v3>LEAD(v500.fecha_inicio_v3) 
                        OVER(
                            PARTITION BY   
                                v500.poliza_id
                                , v500.poliza_certificado_id
                                , v500.placa_vehiculo_txt 
                            ORDER BY v500.fecha_transaccion
                        )
                    ) OR (
                        v500.fecha_inicio_v3<LAG(v500.fecha_fin_v3) 
                            OVER(
                                PARTITION BY   
                                    v500.poliza_id
                                    , v500.poliza_certificado_id
                                    , v500.placa_vehiculo_txt
                                ORDER BY v500.fecha_transaccion
                            )
                        )
                    ) THEN 1 
                ELSE 0 
            END AS superposicion
        FROM v500
        QUALIFY 1= ROW_NUMBER() 
            OVER(
                PARTITION BY   
                    v500.fecha_inicio_v3
                    , v500.fecha_fin_v3
                    , v500.valor_prima_anualizada
                    , v500.poliza_certificado_id
                    , v500.placa_vehiculo_txt 
                ORDER BY v500.fecha_transaccion) 

    )

    , v700 AS (
        SELECT
            v600.*
            , CASE
                WHEN
                    v600.superposicion = 1
                    AND (
                        v600.fecha_inicio_v3
                        < LAG(v600.fecha_fin_v3)
                            OVER (
                                PARTITION BY v600.poliza_id, v600.poliza_certificado_id
                                ORDER BY v600.fecha_transaccion
                            )
                    )
                    THEN
                        GREATEST(
                            v600.fecha_inicio_v3
                            , LAG(v600.fecha_fin_v3)
                                OVER (
                                    PARTITION BY
                                        v600.poliza_id
                                        , v600.poliza_certificado_id
                                        , v600.superposicion
                                    ORDER BY v600.fecha_transaccion
                                )
                        )
                ELSE v600.fecha_inicio_v3
            END AS fecha_inicio_v4
            , CASE
                WHEN
                    v600.superposicion = 1
                    AND (
                        v600.fecha_fin_v3
                        > LEAD(v600.fecha_inicio_v3)
                            OVER (
                                PARTITION BY v600.poliza_id, v600.poliza_certificado_id
                                ORDER BY v600.fecha_transaccion
                            )
                    )
                    THEN
                        GREATEST(
                            v600.fecha_fin_v3
                            , LEAD(v600.fecha_inicio_v3)
                                OVER (
                                    PARTITION BY
                                        v600.poliza_id
                                        , v600.poliza_certificado_id
                                        , v600.superposicion
                                    ORDER BY v600.fecha_transaccion
                                )
                        )
                ELSE v600.fecha_fin_v3
            END AS fecha_fin_v4

        FROM v600
    )

    , v800 AS (
        SELECT v700.*
        FROM v700
        WHERE
            v700.tipo_transaccion_txt <> 'Cancelación'
            AND v700.valor_prima_anualizada <> 0
        QUALIFY
            1
            = ROW_NUMBER()
                OVER (
                    PARTITION BY
                        v700.fecha_inicio_v4
                        , v700.fecha_fin_v4
                        , v700.poliza_certificado_id
                        , v700.placa_vehiculo_txt
                    ORDER BY v700.fecha_transaccion DESC
                )
    )

    , traza AS (

        SELECT 
        v800.*
        ,CASE 
            WHEN (
                first_VALUE(v800.tipo_transaccion_txt) 
                    OVER (
                        PARTITION BY 
                            v800.poliza_certificado_id
                            ,v800.placa_vehiculo_txt
                        ORDER BY v800.fecha_transaccion DESC
                    ) 
            ) IN ('Cancelación') THEN 'INACTIVO'  
            ELSE 'ACTIVO' 
        END AS ESTADO_CERTIFICADO
        , CASE
            WHEN (first_value(v800.Modelo_vehiculo)
                OVER(
                    PARTITION BY   
                        v800.poliza_id
                        ,v800.poliza_certificado_id
                        ,v800.Fecha_inicio
                        ,v800.fecha_fin
                    ORDER BY v800.fecha_transaccion
                ) <> first_value(v800.Modelo_vehiculo)
                    OVER(
                        PARTITION BY   
                            v800.poliza_id
                            ,v800.poliza_certificado_id
                            ,v800.Fecha_inicio
                            ,v800.fecha_fin
                        ORDER BY v800.fecha_transaccion DESC
                    )
            ) THEN (
                    CASE 
                        WHEN v800.modelo_vehiculo = first_value(v800.Modelo_vehiculo)
                            OVER(
                                PARTITION BY   
                                    v800.poliza_id
                                    ,v800.poliza_certificado_id
                                    ,v800.Fecha_inicio
                                    ,v800.fecha_fin
                                ORDER BY v800.fecha_transaccion
                            ) THEN 'Primer modelo' 
                        ELSE 'Cambio modelo' 
                    END 
                )   
            ELSE 'Primer modelo' 
        END AS Cambio_modelo
        , CASE 
            WHEN (Fecha_Fin_v4=LEAD(v800.Fecha_Inicio_v4) 
                OVER(
                    PARTITION BY   
                    v800.poliza_id
                    ,v800.poliza_certificado_id
                    ORDER BY v800.fecha_transaccion
                )
            ) THEN v800.Fecha_Fin_v4-1 
            ELSE v800.Fecha_Fin_v4 
        END AS Fecha_Fin_v5

        ,1 contar
        FROM v800
        where v800.poliza_certificado_id <> '-1' 
    )

    , coberturas AS (
        SELECT DISTINCT
            traza_id
            , CASE
                WHEN tcob.termino_cobert_id IN (5962, 6089, 5945) THEN 'TOTALES'
                WHEN cob.cobertura_id IN (3230, 8233, 2812) THEN 'RC'
                ELSE 'PARCIALES'
            END AS cobertura

        FROM mdb_seguros_colombia.ve_seg_tarifa_traza_cobter AS tc
        LEFT JOIN
            mdb_seguros_colombia.vc_seg_termino_cobertura AS tcob
            ON tc.termino_cobert_id = tcob.termino_cobert_id
        LEFT JOIN
            mdb_seguros_colombia.vc_seg_cobertura AS cob
            ON tc.cobertura_id = cob.cobertura_id

        WHERE tc.valor_prima <> 0
    )

    

    , trazas AS (
        SELECT
            traza.*
            , fechas.mes_id
            , CAST(CASE
                WHEN traza.fecha_fin_v5 < fechas.primer_dia_mes THEN 0
                WHEN traza.fecha_inicio_v4 > fechas.ultimo_dia_mes THEN 0
                WHEN
                    traza.fecha_inicio_v4 <= fechas.primer_dia_mes
                    AND fechas.ultimo_dia_mes <= traza.fecha_fin_v5
                    THEN (fechas.ultimo_dia_mes - fechas.primer_dia_mes) + 1
                WHEN
                    traza.fecha_inicio_v4 >= fechas.primer_dia_mes
                    AND fechas.ultimo_dia_mes >= traza.fecha_fin_v5
                    THEN (traza.fecha_fin_v5 - traza.fecha_inicio_v4) + 1
                WHEN
                    traza.fecha_inicio_v4 >= fechas.primer_dia_mes
                    AND fechas.ultimo_dia_mes <= traza.fecha_fin_v5
                    THEN (fechas.ultimo_dia_mes - traza.fecha_inicio_v4) + 1
                WHEN fechas.primer_dia_mes = traza.fecha_fin_v5 THEN 1
                WHEN
                    traza.fecha_inicio_v4 <= fechas.primer_dia_mes
                    AND fechas.ultimo_dia_mes >= traza.fecha_fin_v5
                    THEN (traza.fecha_fin_v5 - fechas.primer_dia_mes) + 1
                ELSE 0
            END AS FLOAT) AS dias_expuesto_mes
            , CAST((fechas.ultimo_dia_mes - fechas.primer_dia_mes) + 1 AS FLOAT) AS dias_periodo
            , dias_expuesto_mes / dias_periodo AS exposicion
        FROM traza
        INNER JOIN fechas 
                    ON traza.fecha_inicio_v4 <= fechas.ultimo_dia_mes 
                    AND COALESCE(traza.fecha_fin_v5,CAST('3000-01-01' AS DATE)) >= fechas.primer_dia_mes
        WHERE 1 = 1
        --and fechas.ultimo_dia_mes between cast(fecha_inicio_v4 as date) and cast(fecha_fin_v5 as date)
        --and exposicion > 0
        AND dias_expuesto_mes > 0
    )

    , expuestos_ AS (
        SELECT
            mes_id
            , poliza_certificado_id
            , CASE
                WHEN
                    trazas.clase_vehiculo_txt IN (
                        'Motos 125 - 250 CC', 'Motos 0 - 125 CC', 'Motos > 250 CC'
                    )
                    AND poli.sucursal_id IN (21170919, 20056181, 52915901)
                    THEN 'MOTOS SUFI'
                WHEN
                    trazas.clase_vehiculo_txt IN (
                        'Motos 125 - 250 CC', 'Motos 0 - 125 CC', 'Motos > 250 CC'
                    )
                    AND poli.sucursal_id NOT IN (21170919, 20056181, 52915901)
                    THEN 'MOTOS RESTO'
                ELSE cobertura
            END AS cobertura_general_desc
            , MAX(exposicion) AS expuestos
            , COUNT(DISTINCT trazas.poliza_certificado_id) AS vigentes

        FROM trazas
        INNER JOIN coberturas ON trazas.traza_id = coberturas.traza_id
        LEFT JOIN
            mdb_seguros_colombia.v_poliza AS poli
            ON (trazas.poliza_id = poli.poliza_id)
        GROUP BY 1, 2, 3
    )

    SELECT
        mes_id
        , cobertura_general_desc
        , SUM(expuestos) AS expuestos
        , SUM(vigentes) AS vigentes
    FROM expuestos_
    GROUP BY 1,2

) WITH DATA PRIMARY INDEX (
    mes_id
    , cobertura_general_desc
) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE porcentaje AS (
    SELECT
        expuestos_1.cobertura_general_desc
        , SUM(expuestos_1.expuestos) AS expuestos1
        , SUM(expuestos_2.expuestos) AS expuestos2
        , expuestos2 / expuestos1 AS porcentaje
        , SUM(expuestos_1.vigentes) AS vigentes1
        , SUM(expuestos_2.vigentes) AS vigentes2
        , vigentes2 / vigentes1 AS porcentaje_vi

    FROM expuestos_1
    LEFT JOIN expuestos_2
        ON
            expuestos_1.mes_id = expuestos_2.mes_id
            AND expuestos_1.cobertura_general_desc
            = expuestos_2.cobertura_general_desc
    WHERE (
        expuestos_1.mes_id BETWEEN 201901 AND 202207
        AND expuestos_1.cobertura_general_desc NOT IN ('MOTOS SUFI')
    )
    OR (
        expuestos_1.mes_id BETWEEN 202207 AND 202307
        AND expuestos_1.cobertura_general_desc
        IN ('MOTOS SUFI')
    )
    GROUP BY 1
) WITH DATA PRIMARY INDEX (
    cobertura_general_desc
) ON COMMIT PRESERVE ROWS;



SELECT
    '01' AS codigo_op
    , '040' AS codigo_ramo_op
    , expuestos_1.cobertura_general_desc
    , expuestos_1.fecha_registro
    , CASE
        WHEN
            expuestos_1.mes_id < 201901
            AND expuestos_1.cobertura_general_desc NOT IN ('MOTOS SUFI')
            THEN expuestos_1.expuestos * porcentaje.porcentaje
        WHEN
            expuestos_1.mes_id < 202207
            AND expuestos_1.cobertura_general_desc IN ('MOTOS SUFI')
            THEN expuestos_1.expuestos * porcentaje.porcentaje
        ELSE expuestos_2.expuestos
    END AS expuestos
    , CASE
        WHEN
            expuestos_1.mes_id < 201901
            AND expuestos_1.cobertura_general_desc NOT IN ('MOTOS SUFI')
            THEN expuestos_1.vigentes * porcentaje.porcentaje_vi
        WHEN
            expuestos_1.mes_id < 202207
            AND expuestos_1.cobertura_general_desc = 'MOTOS SUFI'
            THEN expuestos_1.vigentes * porcentaje.porcentaje_vi
        ELSE expuestos_2.vigentes
    END AS vigentes

FROM expuestos_1
LEFT JOIN expuestos_2
    ON
        expuestos_1.mes_id = expuestos_2.mes_id
        AND expuestos_1.cobertura_general_desc
        = expuestos_2.cobertura_general_desc
LEFT JOIN porcentaje
    ON expuestos_1.cobertura_general_desc = porcentaje.cobertura_general_desc

ORDER BY 1, 2, 3, 4

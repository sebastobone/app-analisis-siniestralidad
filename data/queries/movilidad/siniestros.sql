CREATE MULTISET VOLATILE TABLE base_pago_bruto AS (
    SELECT
        sini.fecha_siniestro
        , esc.fecha_registro
        , esc.siniestro_id
        , CASE
            WHEN esc.reserva_id = 18635 THEN 'PARCIALES'
            WHEN esc.tipo_oper_siniestro_cd IN (131, 132) THEN 'TOTALES'
            WHEN
                esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                AND sini.acontecimiento_id IN (
                    19866, 58026, 15268, 20052, 7634, 19867, 20112
                )
                THEN 'RC'
            WHEN
                fas.clase_tarifa_cd IN (3, 4, 5, 6)
                AND poli.sucursal_id IN (21170919, 20056181, 52915901)
                THEN 'MOTOS SUFI'
            WHEN
                fas.clase_tarifa_cd IN (3, 4, 5, 6)
                THEN 'MOTOS RESTO'
            WHEN esc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
            WHEN
                esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                THEN 'RC'
            ELSE 'PARCIALES'
        END AS cobertura_general_desc

        , CASE
            WHEN esc.reserva_id = 18635 THEN 'SUBR'
            WHEN esc.tipo_oper_siniestro_cd IN (131, 132) THEN 'SALV'
            WHEN
                esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                AND sini.acontecimiento_id IN (
                    19866, 58026, 15268, 20052, 7634, 19867, 20112
                )
                THEN 'RC PJ'
            WHEN
                fas.clase_tarifa_cd IN (3, 4, 5, 6)
                AND poli.sucursal_id IN (21170919, 20056181, 52915901)
                THEN 'MOTOS SUFI'
            WHEN
                fas.clase_tarifa_cd IN (3, 4, 5, 6)
                THEN 'MOTOS RESTO'
            WHEN esc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
            WHEN
                esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                THEN 'RC NO PJ'
            ELSE 'PARCIALES_RESTO'
        END AS cobertura_desc
        , SUM(esc.valor_siniestro * esc.valor_tasa) AS pago_bruto
        , SUM(CAST(0 AS FLOAT)) AS aviso_bruto

    FROM mdb_seguros_colombia.v_evento_siniestro_cobertura AS esc
    LEFT JOIN mdb_seguros_colombia.v_plan_individual AS pind
        ON esc.plan_individual_id = pind.plan_individual_id
    LEFT JOIN mdb_seguros_colombia.v_producto AS pro
        ON pind.producto_id = pro.producto_id
    LEFT JOIN mdb_seguros_colombia.v_siniestro AS sini
        ON esc.siniestro_id = sini.siniestro_id
    LEFT JOIN mdb_seguros_colombia.v_poliza AS poli
        ON esc.poliza_id = poli.poliza_id
    LEFT JOIN mdb_seguros_colombia.v_poliza_certificado AS polc
        ON
            esc.poliza_certificado_id = polc.poliza_certificado_id
            AND esc.plan_individual_id = polc.plan_individual_id
    LEFT JOIN mdb_seguros_colombia.v_vehiculo AS vehi
        ON polc.bien_id = vehi.bien_id
    LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas
        ON vehi.fasecolda_cd = fas.fasecolda_cd

    WHERE
        esc.ramo_id = 168
        AND pro.compania_id = 4
        AND esc.mes_id <= CAST('{mes_corte}' AS INTEGER)
        AND esc.tipo_oper_siniestro_cd <> '130'

    GROUP BY 1, 2, 3, 4, 5
    HAVING pago_bruto <> 0

) WITH DATA PRIMARY INDEX (
    fecha_siniestro
    , fecha_registro
    , siniestro_id
    , cobertura_general_desc
    , cobertura_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE base_aviso_bruto AS (
    WITH saldos AS (
        SELECT
            sini.fecha_siniestro
            , esc.mes_id
            , esc.siniestro_id
            , CASE
                WHEN esc.reserva_id = 18635 THEN 'PARCIALES'
                WHEN
                    esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                    AND sini.acontecimiento_id IN (
                        19866, 58026, 15268, 20052, 7634, 19867, 20112
                    )
                    THEN 'RC'
                WHEN
                    fas.clase_tarifa_cd IN (3, 4, 5, 6)
                    AND poli.sucursal_id IN (21170919, 20056181, 52915901)
                    THEN 'MOTOS SUFI'
                WHEN
                    fas.clase_tarifa_cd IN (3, 4, 5, 6)
                    THEN 'MOTOS RESTO'
                WHEN esc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
                WHEN
                    esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                    THEN 'RC'
                ELSE 'PARCIALES'
            END AS cobertura_general_desc

            , CASE
                WHEN esc.reserva_id = 18635 THEN 'SUBR'
                WHEN
                    esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                    AND sini.acontecimiento_id IN (
                        19866, 58026, 15268, 20052, 7634, 19867, 20112
                    )
                    THEN 'RC PJ'
                WHEN
                    fas.clase_tarifa_cd IN (3, 4, 5, 6)
                    AND poli.sucursal_id IN (21170919, 20056181, 52915901)
                    THEN 'MOTOS SUFI'
                WHEN
                    fas.clase_tarifa_cd IN (3, 4, 5, 6)
                    THEN 'MOTOS RESTO'
                WHEN esc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
                WHEN
                    esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                    THEN 'RC NO PJ'
                ELSE 'PARCIALES_RESTO'
            END AS cobertura_desc
            , SUM(esc.valor_reserva) AS saldo_aviso_bruto

        FROM mdb_seguros_colombia.v_evento_reserva_comparativo AS esc
        LEFT JOIN mdb_seguros_colombia.v_plan_individual AS pind
            ON esc.plan_individual_id = pind.plan_individual_id
        LEFT JOIN mdb_seguros_colombia.v_producto AS pro
            ON pind.producto_id = pro.producto_id
        LEFT JOIN mdb_seguros_colombia.v_siniestro AS sini
            ON esc.siniestro_id = sini.siniestro_id
        LEFT JOIN mdb_seguros_colombia.v_poliza AS poli
            ON esc.poliza_id = poli.poliza_id
        LEFT JOIN mdb_seguros_colombia.v_poliza_certificado AS polc
            ON
                esc.poliza_certificado_id = polc.poliza_certificado_id
                AND esc.plan_individual_id = polc.plan_individual_id
        LEFT JOIN mdb_seguros_colombia.v_vehiculo AS vehi
            ON polc.bien_id = vehi.bien_id
        LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas
            ON vehi.fasecolda_cd = fas.fasecolda_cd

        WHERE
            esc.ramo_id = 168
            AND pro.compania_id = 4
            AND esc.mes_id <= CAST('{mes_corte}' AS INTEGER)

        GROUP BY 1, 2, 3, 4, 5
        HAVING saldo_aviso_bruto <> 0
    )

    , campos_descriptores AS (
        SELECT
            fecha_siniestro
            , siniestro_id
            , cobertura_general_desc
            , cobertura_desc
            , MIN(mes_id) AS mes_constitucion
        FROM saldos
        GROUP BY 1, 2, 3, 4
    )

    , meses AS (
        SELECT
            mes_id
            , MAX(dia_dt) AS ultimo_dia_mes
        FROM mdb_seguros_colombia.v_dia
        WHERE mes_id <= CAST('{mes_corte}' AS INTEGER)
        GROUP BY 1
    )

    , base_ficticia AS (
        SELECT
            descr.fecha_siniestro
            , meses.mes_id
            , descr.siniestro_id
            , descr.cobertura_general_desc
            , descr.cobertura_desc
            , CAST(0 AS FLOAT) AS saldo_aviso_bruto
        FROM meses
        INNER JOIN campos_descriptores AS descr
            ON meses.mes_id >= descr.mes_constitucion
    )

    , base_completa AS (
        SELECT * FROM saldos
        UNION ALL
        SELECT * FROM base_ficticia
    )

    , base_completa_agrupada AS (
        SELECT
            fecha_siniestro
            , mes_id
            , siniestro_id
            , cobertura_general_desc
            , cobertura_desc
            , SUM(saldo_aviso_bruto) AS saldo_aviso_bruto
        FROM base_completa
        GROUP BY 1, 2, 3, 4, 5
    )

    , movimientos AS (
        SELECT
            fecha_siniestro
            , siniestro_id
            , cobertura_general_desc
            , cobertura_desc
            , mes_id
            , MIN(mes_id)
                OVER (
                    PARTITION BY
                        siniestro_id, cobertura_general_desc, cobertura_desc
                )
                AS mes_constitucion
            , CASE
                WHEN mes_id = mes_constitucion THEN saldo_aviso_bruto
                ELSE saldo_aviso_bruto - LAG(saldo_aviso_bruto) OVER (
                    PARTITION BY
                        siniestro_id, cobertura_general_desc, cobertura_desc
                    ORDER BY mes_id
                )
            END AS movimiento_aviso_bruto
        FROM base_completa_agrupada
    )

    SELECT
        mov.fecha_siniestro
        , meses.ultimo_dia_mes AS fecha_registro
        , mov.siniestro_id
        , mov.cobertura_general_desc
        , mov.cobertura_desc
        , CAST(0 AS FLOAT) AS pago_bruto
        , mov.movimiento_aviso_bruto AS aviso_bruto
    FROM movimientos AS mov
    LEFT JOIN meses
        ON mov.mes_id = meses.mes_id

) WITH DATA PRIMARY INDEX (
    fecha_siniestro
    , fecha_registro
    , siniestro_id
    , cobertura_general_desc
    , cobertura_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE base_bruto AS (
    WITH consolidado AS (
        SELECT * FROM base_pago_bruto
        UNION ALL
        SELECT * FROM base_aviso_bruto
    )

    SELECT
        fecha_siniestro
        , fecha_registro
        , siniestro_id
        , cobertura_general_desc
        , cobertura_desc
        , SUM(pago_bruto) AS pago_bruto
        , SUM(aviso_bruto) AS aviso_bruto
    FROM consolidado
    GROUP BY 1, 2, 3, 4, 5

) WITH DATA PRIMARY INDEX (
    fecha_siniestro
    , fecha_registro
    , siniestro_id
    , cobertura_general_desc
    , cobertura_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE base_cedido AS (
    SELECT
        sini.fecha_siniestro
        , ersc.fecha_registro
        , ersc.siniestro_id
        , CASE
            WHEN ersc.reserva_id = 18635 THEN 'PARCIALES'
            WHEN ersc.tipo_operacion_cd IN (131, 127) THEN 'TOTALES'
            WHEN
                ersc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                AND sini.acontecimiento_id IN (
                    19866, 58026, 15268, 20052, 7634, 19867, 20112
                )
                THEN 'RC'
            WHEN
                fas.clase_tarifa_cd IN (3, 4, 5, 6)
                AND poli.sucursal_id IN (21170919, 20056181, 52915901)
                THEN 'MOTOS SUFI'
            WHEN
                fas.clase_tarifa_cd IN (3, 4, 5, 6)
                THEN 'MOTOS RESTO'
            WHEN ersc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
            WHEN
                ersc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                THEN 'RC'
            ELSE 'PARCIALES'
        END AS cobertura_general_desc
        , CASE
            WHEN ersc.reserva_id = 18635 THEN 'SUBR'
            WHEN ersc.tipo_operacion_cd IN (131, 127) THEN 'SALV'
            WHEN
                ersc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                AND sini.acontecimiento_id IN (
                    19866, 58026, 15268, 20052, 7634, 19867, 20112
                )
                THEN 'RC PJ'
            WHEN
                fas.clase_tarifa_cd IN (3, 4, 5, 6)
                AND poli.sucursal_id IN (21170919, 20056181, 52915901)
                THEN 'MOTOS SUFI'
            WHEN
                fas.clase_tarifa_cd IN (3, 4, 5, 6)
                THEN 'MOTOS RESTO'
            WHEN ersc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
            WHEN
                ersc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                THEN 'RC NO PJ'
            ELSE 'PARCIALES_RESTO'
        END AS cobertura_desc
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
    LEFT JOIN mdb_seguros_colombia.v_evento_reaseguro_sini AS ers
        ON ersc.evento_id = ers.evento_id
    LEFT JOIN mdb_seguros_colombia.v_plan_individual_mstr AS pind
        ON ersc.plan_individual_id = pind.plan_individual_id
    LEFT JOIN mdb_seguros_colombia.v_producto AS pro
        ON pind.producto_id = pro.producto_id
    LEFT JOIN mdb_seguros_colombia.v_siniestro AS sini
        ON ersc.siniestro_id = sini.siniestro_id
    LEFT JOIN mdb_seguros_colombia.v_poliza AS poli
        ON ersc.poliza_id = poli.poliza_id
    LEFT JOIN mdb_seguros_colombia.v_poliza_certificado AS polc
        ON
            ersc.poliza_certificado_id = polc.poliza_certificado_id
            AND ersc.plan_individual_id = polc.plan_individual_id
    LEFT JOIN mdb_seguros_colombia.v_vehiculo AS vehi
        ON polc.bien_id = vehi.bien_id
    LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas
        ON vehi.fasecolda_cd = fas.fasecolda_cd

    WHERE
        pro.ramo_id = 168
        AND pro.compania_id = 4
        AND ersc.mes_id <= CAST('{mes_corte}' AS INTEGER)

    GROUP BY 1, 2, 3, 4, 5
    HAVING pago_cedido <> 0 OR aviso_cedido <> 0

) WITH DATA PRIMARY INDEX (
    fecha_siniestro
    , fecha_registro
    , siniestro_id
    , cobertura_desc
    , cobertura_general_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE base_ced_y_bruto AS
(
    SELECT
        COALESCE(brt.fecha_siniestro, ced.fecha_siniestro) AS fecha_siniestro
        , COALESCE(brt.fecha_registro, ced.fecha_registro) AS fecha_registro
        , COALESCE(brt.siniestro_id, ced.siniestro_id) AS siniestro_id
        , COALESCE(brt.cobertura_general_desc, ced.cobertura_general_desc)
            AS cobertura_general_desc
        , COALESCE(brt.cobertura_desc, ced.cobertura_desc) AS cobertura_desc
        , ZEROIFNULL(brt.pago_bruto) AS pago_bruto
        , ZEROIFNULL(ced.pago_cedido) AS pago_cedido
        , ZEROIFNULL(brt.pago_bruto)
        - ZEROIFNULL(ced.pago_cedido) AS pago_retenido
        , ZEROIFNULL(brt.aviso_bruto) AS aviso_bruto
        , ZEROIFNULL(ced.aviso_cedido) AS aviso_cedido
        , ZEROIFNULL(brt.aviso_bruto)
        - ZEROIFNULL(ced.aviso_cedido) AS aviso_retenido

    FROM base_bruto AS brt
    FULL OUTER JOIN base_cedido AS ced
        ON
            brt.fecha_siniestro = ced.fecha_siniestro
            AND brt.fecha_registro = ced.fecha_registro
            AND brt.siniestro_id = ced.siniestro_id
            AND brt.cobertura_general_desc = ced.cobertura_general_desc
            AND brt.cobertura_desc = ced.cobertura_desc

) WITH DATA PRIMARY INDEX (
    fecha_siniestro
    , fecha_registro
    , cobertura_desc
    , cobertura_general_desc
    , siniestro_id
) ON COMMIT PRESERVE ROWS;




CREATE MULTISET VOLATILE TABLE atipicos AS (
    SELECT
        atip.cobertura_desc
        , atip.cobertura_general_desc
        , atip.siniestro_id
        , 1 AS atipico

    FROM (
        SELECT
            esc.siniestro_id
            , sini.fecha_siniestro
            , CASE
                WHEN esc.reserva_id = 18635 THEN 'PARCIALES'
                WHEN esc.tipo_oper_siniestro_cd IN (131, 132) THEN 'TOTALES'
                WHEN
                    esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                    AND sini.acontecimiento_id IN (
                        19866, 58026, 15268, 20052, 7634, 19867, 20112
                    )
                    THEN 'RC'
                WHEN
                    fas.clase_tarifa_cd IN (3, 4, 5, 6)
                    AND poli.sucursal_id IN (21170919, 20056181, 52915901)
                    THEN 'MOTOS SUFI'
                WHEN
                    fas.clase_tarifa_cd IN (3, 4, 5, 6)
                    THEN 'MOTOS RESTO'
                WHEN esc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
                WHEN
                    esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                    THEN 'RC'
                ELSE 'PARCIALES'
            END AS cobertura_general_desc
            , CASE
                WHEN esc.reserva_id = 18635 THEN 'SUBR'
                WHEN esc.tipo_oper_siniestro_cd IN (131, 132) THEN 'SALV'
                WHEN
                    esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                    AND sini.acontecimiento_id IN (
                        19866, 58026, 15268, 20052, 7634, 19867, 20112
                    )
                    THEN 'RC PJ'
                WHEN
                    fas.clase_tarifa_cd IN (3, 4, 5, 6)
                    AND poli.sucursal_id IN (21170919, 20056181, 52915901)
                    THEN 'MOTOS SUFI'
                WHEN
                    fas.clase_tarifa_cd IN (3, 4, 5, 6)
                    THEN 'MOTOS RESTO'
                WHEN esc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
                WHEN
                    esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                    THEN 'RC NO PJ'
                ELSE 'PARCIALES_RESTO'
            END AS cobertura_desc
            , SUM(esc.valor_siniestro * esc.valor_tasa) AS total

        FROM mdb_seguros_colombia.v_evento_siniestro_cobertura AS esc
        LEFT JOIN mdb_seguros_colombia.v_plan_individual AS pind
            ON esc.plan_individual_id = pind.plan_individual_id
        LEFT JOIN mdb_seguros_colombia.v_producto AS pro
            ON pind.producto_id = pro.producto_id
        LEFT JOIN mdb_seguros_colombia.v_siniestro AS sini
            ON esc.siniestro_id = sini.siniestro_id
        LEFT JOIN mdb_seguros_colombia.v_poliza AS poli
            ON esc.poliza_id = poli.poliza_id
        LEFT JOIN mdb_seguros_colombia.v_poliza_certificado AS polc
            ON
                esc.poliza_certificado_id = polc.poliza_certificado_id
                AND esc.plan_individual_id = polc.plan_individual_id
        LEFT JOIN mdb_seguros_colombia.v_vehiculo AS vehi
            ON polc.bien_id = vehi.bien_id
        LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas
            ON vehi.fasecolda_cd = fas.fasecolda_cd

        WHERE
            esc.ramo_id = 168
            AND pro.compania_id = 4
            AND esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)

        GROUP BY 1, 2, 3, 4
        HAVING total >= 800000000
    ) AS atip
) WITH DATA PRIMARY INDEX (siniestro_id) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE base_incurrido AS (
    SELECT
        base.fecha_siniestro
        , base.fecha_registro
        , base.siniestro_id
        , base.cobertura_desc
        , base.cobertura_general_desc
        , COALESCE(atip.atipico, 0) AS atipico
        , base.pago_bruto
        , base.pago_retenido
        , base.aviso_bruto
        , base.aviso_retenido

    FROM base_ced_y_bruto AS base
    LEFT JOIN atipicos AS atip
        ON
            base.siniestro_id = atip.siniestro_id
            AND base.cobertura_desc = atip.cobertura_desc
            AND base.cobertura_general_desc = atip.cobertura_general_desc
) WITH DATA PRIMARY INDEX (
    fecha_siniestro
    , fecha_registro
    , siniestro_id
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE conteo_pago_bruto AS (
    SELECT
        fecha_siniestro
        , cobertura_desc
        , cobertura_general_desc
        , fecha_registro
        , atipico
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_pago

    FROM
        (
            SELECT
                base.fecha_siniestro
                , base.cobertura_general_desc
                , base.cobertura_desc
                , base.siniestro_id
                , base.atipico
                , MIN(base.fecha_registro) AS fecha_registro

            FROM base_incurrido AS base
            WHERE ABS(base.pago_bruto) > 1000
            GROUP BY 1, 2, 3, 4, 5
        ) AS fmin
    GROUP BY 1, 2, 3, 4, 5
) WITH DATA PRIMARY INDEX (
    fecha_siniestro
    , fecha_registro
    , atipico
    , cobertura_general_desc
    , cobertura_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE conteo_incurrido_bruto AS (
    SELECT
        fecha_siniestro
        , fecha_registro
        , atipico
        , cobertura_general_desc
        , cobertura_desc
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_incurrido

    FROM (
        SELECT
            base.fecha_siniestro
            , base.cobertura_general_desc
            , base.cobertura_desc
            , base.atipico
            , base.siniestro_id
            , MIN(base.fecha_registro) AS fecha_registro
        FROM base_incurrido AS base
        WHERE NOT (ABS(base.pago_bruto) < 1000 AND ABS(base.aviso_bruto) < 1000)
        GROUP BY 1, 2, 3, 4, 5
    ) AS fmin
    GROUP BY 1, 2, 3, 4, 5
) WITH DATA PRIMARY INDEX (
    fecha_registro
    , fecha_siniestro
    , atipico
    , cobertura_general_desc
    , cobertura_desc
) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE sin_pagos_bruto AS (
    WITH rva AS (
        SELECT
            fecha_siniestro
            , siniestro_id
            , cobertura_general_desc
            , cobertura_desc
            , atipico
            , 1 AS en_reserva
            , SUM(aviso_bruto) AS aviso_bruto

        FROM base_incurrido
        GROUP BY 1, 2, 3, 4, 5, 6
        HAVING SUM(aviso_bruto) > 1000
    )

    SELECT
        base.fecha_siniestro
        , base.siniestro_id
        , base.cobertura_general_desc
        , base.cobertura_desc
        , base.atipico
        , ZEROIFNULL(rva.en_reserva) AS en_rva
        , ZEROIFNULL(SUM(base.pago_bruto)) AS pago

    FROM base_incurrido AS base
    LEFT JOIN rva
        ON
            (base.fecha_siniestro = rva.fecha_siniestro)
            AND (base.siniestro_id = rva.siniestro_id)
            AND (base.cobertura_general_desc = rva.cobertura_general_desc)
            AND (base.cobertura_desc = rva.cobertura_desc)
            AND (base.atipico = rva.atipico)

    WHERE en_rva = 0
    GROUP BY 1, 2, 3, 4, 5, 6
    HAVING pago = 0
) WITH DATA PRIMARY INDEX (
    fecha_siniestro
    , cobertura_general_desc
    , cobertura_desc
    , atipico
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE conteo_desistido_bruto AS (
    SELECT
        fecha_siniestro
        , fecha_registro
        , cobertura_general_desc
        , cobertura_desc
        , atipico
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_desistido

    FROM (
        SELECT
            base.fecha_siniestro
            , base.siniestro_id
            , base.cobertura_general_desc
            , base.cobertura_desc
            , base.atipico
            , MAX(base.fecha_registro) AS fecha_registro

        FROM base_incurrido AS base
        INNER JOIN sin_pagos_bruto AS pag
            ON
                (base.fecha_siniestro = pag.fecha_siniestro)
                AND (base.siniestro_id = pag.siniestro_id)
                AND (base.cobertura_general_desc = pag.cobertura_general_desc)
                AND (base.cobertura_desc = pag.cobertura_desc)
                AND (base.atipico = pag.atipico)
        WHERE base.aviso_bruto <> 0
        GROUP BY 1, 2, 3, 4, 5
    ) AS fecha

    GROUP BY 1, 2, 3, 4, 5
) WITH DATA PRIMARY INDEX (
    fecha_siniestro
    , fecha_registro
    , cobertura_general_desc
    , cobertura_desc
) ON COMMIT PRESERVE ROWS;




CREATE MULTISET VOLATILE TABLE base_pagos_aviso AS (
    SELECT
        fecha_siniestro
        , fecha_registro
        , cobertura_general_desc
        , cobertura_desc
        , atipico
        , ZEROIFNULL(SUM(pago_bruto)) AS pago_bruto
        , ZEROIFNULL(SUM(pago_retenido)) AS pago_retenido
        , ZEROIFNULL(SUM(aviso_bruto)) AS aviso_bruto
        , ZEROIFNULL(SUM(aviso_retenido)) AS aviso_retenido
    FROM base_incurrido
    GROUP BY 1, 2, 3, 4, 5
) WITH DATA PRIMARY INDEX (
    fecha_siniestro
    , fecha_registro
    , atipico
    , cobertura_general_desc
    , cobertura_desc
) ON COMMIT PRESERVE ROWS;



WITH fechas AS (
    SELECT DISTINCT
        dia_dt
        , MIN(dia_dt) OVER (PARTITION BY mes_id) AS mes_id
    FROM mdb_seguros_colombia.v_dia
)

SELECT
    '01' AS codigo_op
    , '040' AS codigo_ramo_op
    , base.atipico
    , base.cobertura_general_desc
    , base.cobertura_desc
    , GREATEST(CAST('{fecha_primera_ocurrencia}' AS DATE), focurr.mes_id)
        AS fecha_siniestro
    , GREATEST(CAST('{fecha_primera_ocurrencia}' AS DATE), fmov.mes_id)
        AS fecha_registro
    , SUM(ZEROIFNULL(contp.conteo_pago)) AS conteo_pago
    , SUM(ZEROIFNULL(conti.conteo_incurrido)) AS conteo_incurrido
    , SUM(ZEROIFNULL(contd.conteo_desistido)) AS conteo_desistido
    , SUM(ZEROIFNULL(base.pago_bruto)) AS pago_bruto
    , SUM(ZEROIFNULL(base.pago_retenido)) AS pago_retenido
    , SUM(ZEROIFNULL(base.aviso_bruto)) AS aviso_bruto
    , SUM(ZEROIFNULL(base.aviso_retenido)) AS aviso_retenido

FROM base_pagos_aviso AS base
LEFT JOIN conteo_pago_bruto AS contp
    ON
        base.fecha_siniestro = contp.fecha_siniestro
        AND base.fecha_registro = contp.fecha_registro
        AND base.cobertura_desc = contp.cobertura_desc
        AND base.atipico = contp.atipico
        AND base.cobertura_general_desc = contp.cobertura_general_desc
LEFT JOIN conteo_incurrido_bruto AS conti
    ON
        base.fecha_siniestro = conti.fecha_siniestro
        AND base.fecha_registro = conti.fecha_registro
        AND base.cobertura_desc = conti.cobertura_desc
        AND base.atipico = conti.atipico
        AND base.cobertura_general_desc = conti.cobertura_general_desc
LEFT JOIN conteo_desistido_bruto AS contd
    ON
        base.fecha_siniestro = contd.fecha_siniestro
        AND base.fecha_registro = contd.fecha_registro
        AND base.cobertura_desc = contd.cobertura_desc
        AND base.atipico = contd.atipico
        AND base.cobertura_general_desc = contd.cobertura_general_desc
INNER JOIN fechas AS focurr ON base.fecha_siniestro = focurr.dia_dt
INNER JOIN fechas AS fmov ON base.fecha_registro = fmov.dia_dt

GROUP BY 1, 2, 3, 4, 5, 6, 7

HAVING
    NOT (
        SUM(ZEROIFNULL(contp.conteo_pago)) = 0
        AND SUM(ZEROIFNULL(conti.conteo_incurrido)) = 0
        AND SUM(ZEROIFNULL(contd.conteo_desistido)) = 0
        AND SUM(ZEROIFNULL(base.pago_bruto)) = 0
        AND SUM(ZEROIFNULL(base.aviso_bruto)) = 0
        AND SUM(ZEROIFNULL(base.pago_retenido)) = 0
        AND SUM(ZEROIFNULL(base.aviso_retenido)) = 0
    )

ORDER BY 1, 2, 3, 4, 5, 6, 7

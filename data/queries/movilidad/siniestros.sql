
CREATE MULTISET VOLATILE TABLE base_bruto AS (
    SELECT
        LAST_DAY(sini.fecha_siniestro) AS fecha_siniestro
        , LAST_DAY(esc.fecha_registro) AS fecha_registro
        , '040' AS codigo_ramo_op 
        , esc.siniestro_id
        , CASE 
            
            WHEN esc.reserva_id = 18635 THEN 'PARCIALES'
            WHEN esc.tipo_oper_siniestro_cd IN (131, 132) THEN 'TOTALES'
            WHEN esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                AND sini.acontecimiento_id IN (19866, 58026, 15268, 20052, 7634, 19867, 20112) THEN 'RC'
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND pol.sucursal_id in (21170919, 20056181, 52915901) THEN 'MOTOS SUFI'
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND pol.sucursal_id NOT IN (21170919, 20056181, 52915901) THEN 'MOTOS RESTO'
            WHEN esc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
            WHEN esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694) THEN 'RC'
            ELSE 'PARCIALES'
        END AS cobertura_general_desc

        , CASE 
            WHEN esc.reserva_id = 18635 THEN 'SUBR'
            WHEN esc.tipo_oper_siniestro_cd IN (131, 132) THEN 'SALV'
            WHEN esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                AND sini.acontecimiento_id IN (19866, 58026, 15268, 20052, 7634, 19867, 20112) THEN 'RC PJ'
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND pol.sucursal_id in (21170919, 20056181, 52915901) THEN 'MOTOS SUFI'
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND pol.sucursal_id not in (21170919, 20056181, 52915901) THEN 'MOTOS RESTO'
            WHEN esc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
            WHEN esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694) THEN 'RC NO PJ'
            ELSE 'PARCIALES_RESTO'
        END AS cobertura_desc
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
        LEFT JOIN mdb_seguros_colombia.v_siniestro AS sini
        ON esc.siniestro_id = sini.siniestro_id

        LEFT JOIN mdb_seguros_colombia.v_poliza AS poli
        ON esc.poliza_id = poli.poliza_id
        
        LEFT JOIN mdb_seguros_colombia.v_poliza_certificado as polc
        ON esc.poliza_certificado_id = polc.poliza_certificado_id
        AND esc.plan_individual_id = polc.plan_individual_id
        
        LEFT JOIN mdb_seguros_colombia.v_vehiculo as vehi
        ON polc.bien_id = vehi.bien_id

        LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas
        ON fas.fasecolda_cd = vehi.fasecolda_cd

        LEFT JOIN mdb_seguros_colombia.v_poliza as pol
        ON pol.poliza_id = esc.poliza_id

        LEFT JOIN mdb_seguros_colombia.v_ramo as ramo
        ON ramo.ramo_id = esc.ramo_id

        WHERE
        esc.ramo_id = 168
        AND esc.mes_id BETWEEN CAST(
            '{mes_primera_ocurrencia}' AS INTEGER
        ) AND CAST('{mes_corte}' AS INTEGER)

    GROUP BY 1, 2, 3, 4, 5, 6
    HAVING NOT (pago_bruto = 0 AND aviso_bruto = 0)    
)
WITH DATA PRIMARY INDEX
(
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , siniestro_id
    , cobertura_general_desc
    , cobertura_desc
) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE base_cedido AS (
    SELECT
    sini.fecha_siniestro,
    ersc.fecha_registro,
    ramo.Codigo_Ramo_Op,
    ersc.siniestro_id
    , CASE 
            
            WHEN ersc.reserva_id = 18635 THEN 'PARCIALES'
            WHEN ersc.Tipo_Operacion_Cd IN (131, 127) THEN 'TOTALES'
            WHEN ersc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                AND sini.acontecimiento_id IN (19866, 58026, 15268, 20052, 7634, 19867, 20112) THEN 'RC'
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND pol.sucursal_id in (21170919, 20056181, 52915901) THEN 'MOTOS SUFI'
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND pol.sucursal_id NOT IN (21170919, 20056181, 52915901) THEN 'MOTOS RESTO'
            WHEN ersc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
            WHEN ersc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694) THEN 'RC'
            ELSE 'PARCIALES'
        END AS cobertura_general_desc 
    , CASE
        WHEN ersc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
            AND sini.acontecimiento_id IN (19866, 58026, 15268, 20052, 7634, 19867, 20112) THEN 'RC PJ'
        WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND pol.sucursal_id IN (21170919,  20056181, 52915901) THEN 'MOTOS SUFI'
        WHEN fas.clase_tarifa_Cd IN (3,4,5,6)  AND pol.sucursal_id NOT IN (21170919,  20056181, 52915901) THEN 'MOTOS RESTO'
        WHEN ersc.reserva_id = 18635 THEN 'SUBR'
        WHEN ersc.Tipo_Operacion_Cd IN (131,127) THEN 'SALV'
        WHEN ersc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
        WHEN ersc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694) THEN 'RC NO PJ'
        ELSE 'PARCIALES_RESTO'
    END AS cobertura_desc
    , SUM(
        CASE
            WHEN ers.Numero_Documento <> '-1' THEN ersc.Valor_Siniestro_Cedido * ersc.Valor_Tasa ELSE 0
        END) pago_cedido
    , SUM(
        CASE
            WHEN ers.Numero_Documento = '-1' THEN ersc.Valor_Siniestro_Cedido * ersc.Valor_Tasa ELSE 0
        END) aviso_cedido
        
    FROM mdb_seguros_colombia.v_EVENTO_REASEGURO_SINI_COB ersc
    LEFT JOIN mdb_seguros_colombia.v_EVENTO_REASEGURO_SINI ers 
    ON (ers.Evento_Id = ersc.Evento_Id)
    LEFT JOIN mdb_seguros_colombia.v_PLAN_INDIVIDUAL_MSTR plan 
    ON (ersc.Plan_Individual_Id = plan.Plan_Individual_Id)
    LEFT JOIN mdb_seguros_colombia.v_PRODUCTO pro 
    ON (plan.Producto_Id = pro.Producto_Id)
    LEFT JOIN mdb_seguros_colombia.v_RAMO ramo 
    ON (pro.Ramo_Id = ramo.Ramo_Id)
    LEFT JOIN mdb_seguros_colombia.v_SINIESTRO sini
    ON (ersc.Siniestro_Id = sini.Siniestro_Id)
    LEFT JOIN mdb_seguros_colombia.v_AMPARO ampa 
    ON (ersc.Amparo_Id = ampa.Amparo_Id)
    LEFT JOIN mdb_seguros_colombia.v_COMPANIA cia 
    ON (pro.Compania_Id = cia.Compania_Id) 
    LEFT JOIN mdb_seguros_colombia.v_poliza_certificado AS polc
    ON ersc.poliza_certificado_id = polc.poliza_certificado_id
    AND ersc.plan_individual_id = polc.plan_individual_id
    LEFT JOIN mdb_seguros_colombia.v_vehiculo AS vehi
    ON polc.bien_id = vehi.bien_id
    LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas
    ON fas.fasecolda_cd = vehi.fasecolda_cd
    LEFT JOIN mdb_seguros_colombia.v_poliza AS pol
    ON pol.poliza_id = ersc.poliza_id
    WHERE ramo.Codigo_Ramo_Op IN ('040')
        AND  ersc.Mes_Id BETWEEN CAST(
    '{mes_primera_ocurrencia}' AS INTEGER
    ) AND CAST('{mes_corte}' AS INTEGER)
        AND  cia.Codigo_Op = '01' 
    
    GROUP BY 1, 2, 3, 4, 5, 6
    HAVING NOT ( Pago_Cedido = 0 AND Aviso_Cedido = 0)
)
WITH DATA PRIMARY INDEX
(
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , siniestro_id
    , cobertura_desc
    , cobertura_general_desc
) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE base_ced_y_bruto AS 
(
    SELECT 
        COALESCE(brt.fecha_siniestro, ced.fecha_siniestro) AS fecha_siniestro
        , COALESCE(brt.fecha_registro, ced.fecha_registro) AS fecha_registro
        , COALESCE(brt.codigo_ramo_op, ced.codigo_ramo_op) AS codigo_ramo_op
        , COALESCE(brt.siniestro_id, ced.siniestro_id) AS siniestro_id
        , COALESCE(brt.cobertura_general_desc, ced.cobertura_general_desc) AS cobertura_general_desc
        , COALESCE(brt.cobertura_desc, ced.cobertura_desc) AS cobertura_desc
        , ZEROIFNULL(brt.pago_bruto) AS pago_bruto
        , ZEROIFNULL(ced.pago_cedido) AS pago_cedido
        , ZEROIFNULL(brt.pago_bruto) - ZEROIFNULL(ced.pago_cedido) AS pago_retenido
        , ZEROIFNULL(brt.aviso_bruto) AS aviso_bruto
        , ZEROIFNULL(ced.aviso_cedido) AS aviso_cedido
        , ZEROIFNULL(brt.aviso_bruto) - ZEROIFNULL(ced.aviso_cedido) AS aviso_retenido

    FROM base_bruto AS brt
        FULL OUTER JOIN base_cedido AS ced
        ON brt.fecha_siniestro = ced.fecha_siniestro
        AND brt.fecha_registro = ced.fecha_registro
        AND brt.codigo_ramo_op = ced.codigo_ramo_op
        AND brt.siniestro_id = ced.siniestro_id
        AND brt.cobertura_general_desc = ced.cobertura_general_desc
        AND brt.cobertura_desc = ced.cobertura_desc     
)
WITH DATA PRIMARY INDEX
(
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , cobertura_desc
    , cobertura_general_desc
    , siniestro_id
) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE atipicos AS (
    SELECT atip.cobertura_desc
    , atip.cobertura_general_desc
    , atip.codigo_ramo_op
    , atip.siniestro_id
    , 1 AS atipico
    
    FROM (
        SELECT
        esc.siniestro_id
        , sini.fecha_siniestro
        , ramo.codigo_ramo_op
       , CASE 
            
            WHEN esc.reserva_id = 18635 THEN 'PARCIALES'
            WHEN esc.tipo_oper_siniestro_cd IN (131, 132) THEN 'TOTALES'
            WHEN esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
                AND sini.acontecimiento_id IN (19866, 58026, 15268, 20052, 7634, 19867, 20112) THEN 'RC'
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND pol.sucursal_id in (21170919, 20056181, 52915901) THEN 'MOTOS SUFI'
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND pol.sucursal_id NOT IN (21170919, 20056181, 52915901) THEN 'MOTOS RESTO'
            WHEN esc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
            WHEN esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694) THEN 'RC'
            ELSE 'PARCIALES'
        END AS cobertura_general_desc
        , CASE
        WHEN esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694)
             AND sini.acontecimiento_id IN (19866, 58026, 15268, 20052, 7634, 19867, 20112) THEN 'RC PJ'
        WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND pol.sucursal_id in (21170919, 20056181, 52915901) THEN 'MOTOS SUFI'
        WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND pol.sucursal_id not in (21170919, 20056181, 52915901) THEN 'MOTOS RESTO'
        WHEN esc.reserva_id = 18635 THEN 'SUBR'
        WHEN esc.tipo_oper_siniestro_cd IN (131, 132) THEN 'SALV'
        WHEN esc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
        WHEN esc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694) THEN 'RC NO PJ'
        ELSE 'PARCIALES_RESTO'
        END AS cobertura_desc
        , sum(esc.valor_siniestro * esc.valor_tasa) AS total

        FROM mdb_seguros_colombia.v_evento_siniestro_cobertura AS esc
        LEFT JOIN mdb_seguros_colombia.v_siniestro AS sini
        ON esc.siniestro_id = sini.siniestro_id

        LEFT JOIN mdb_seguros_colombia.v_poliza AS poli
        ON esc.poliza_id = poli.poliza_id
        
        LEFT JOIN mdb_seguros_colombia.v_poliza_certificado as polc
        ON esc.poliza_certificado_id = polc.poliza_certificado_id
        AND esc.plan_individual_id = polc.plan_individual_id
        
        LEFT JOIN mdb_seguros_colombia.v_vehiculo as vehi
        ON polc.bien_id = vehi.bien_id

        LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas
        ON fas.fasecolda_cd = vehi.fasecolda_cd

        LEFT JOIN mdb_seguros_colombia.v_poliza as pol
        ON pol.poliza_id = esc.poliza_id

        LEFT JOIN mdb_seguros_colombia.v_ramo as ramo
        ON ramo.ramo_id = esc.ramo_id

        WHERE 1=1
         AND esc.ramo_id = 168
         
         AND esc.Amparo_Id IN (14489, 14277, 14122 , 14771 , 56397 , 694)

	group by 1, 2, 3, 4, 5
	having (total >= 800000000) 
    ) AS atip
)WITH DATA PRIMARY INDEX(Siniestro_Id) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE base_incurrido AS (
    SELECT
    base.fecha_siniestro
    , base.fecha_registro
    , base.Codigo_Ramo_Op
	, base.Siniestro_Id
    , base.cobertura_desc
    , base.cobertura_general_desc
    , COALESCE(atip.atipico, 0) AS atipico
    , base.pago_bruto
    , base.Pago_Retenido
	, base.Aviso_Bruto
    , base.Aviso_Retenido

    FROM base_ced_y_bruto AS base
    LEFT JOIN atipicos AS atip 
    ON atip.siniestro_id = base.siniestro_id
    AND atip.cobertura_desc = base.cobertura_desc
    AND atip.cobertura_general_desc = base.cobertura_general_desc
)
WITH DATA PRIMARY INDEX 
(
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , siniestro_id
) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE conteo_pago_bruto AS
(
    SELECT
        fecha_siniestro
        , codigo_ramo_op
        , cobertura_desc
        , cobertura_general_desc
        , fecha_registro
        , atipico
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_pago

    FROM
        (
            SELECT
                fecha_siniestro
                , codigo_ramo_op
                , cobertura_general_desc
                , cobertura_desc
                , siniestro_id
                , atipico
                , MIN(fecha_registro) AS fecha_registro

            FROM base_incurrido
            WHERE ABS(pago_bruto) > 1000
            GROUP BY 1, 2, 3, 4, 5, 6
        ) AS fmin
    GROUP BY 1, 2, 3, 4, 5, 6
)
WITH DATA PRIMARY INDEX
(
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , atipico
    , cobertura_general_desc
    , cobertura_desc
) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE conteo_incurrido_bruto AS (
    SELECT 
        fecha_siniestro
        , codigo_ramo_op
        , fecha_registro
        , atipico
        , cobertura_general_desc
        , cobertura_desc
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_incurrido

        FROM (
            SELECT
                fecha_siniestro
                , codigo_ramo_op
                , cobertura_general_desc
                , cobertura_desc
                , atipico
                , siniestro_id
                , MIN(fecha_registro) AS fecha_registro
            FROM base_incurrido
            WHERE NOT (ABS(pago_bruto) < 1000 AND ABS(aviso_bruto) < 1000)
            GROUP BY 1, 2, 3, 4, 5, 6
        ) fmin
        GROUP BY 1, 2, 3, 4, 5, 6
)
WITH DATA PRIMARY INDEX
(
    fecha_registro
    , fecha_siniestro
    , codigo_ramo_op
    , atipico
    , cobertura_general_desc
    , cobertura_desc
)ON COMMIT PRESERVE ROWS;




CREATE MULTISET VOLATILE TABLE conteo_desistido_bruto AS (
    SELECT    
        fecha_siniestro
        , fecha_registro
        , codigo_ramo_op
        , cobertura_general_desc
        , cobertura_desc
        , atipico
        , ZEROIFNULL(COUNT(DISTINCT siniestro_id)) AS conteo_desistido
        FROM (
            SELECT fecha_siniestro
                   , codigo_ramo_op
                   , cobertura_general_desc
                   , cobertura_desc
                   , atipico
                   , MAX(fecha_registro) AS fecha_registro
                   , siniestro_id
                   , SUM(aviso_bruto) AS sum_aviso
                   , MAX(ABS(pago_bruto)) AS pago_maximo 
                   , MAX(ABS(aviso_bruto)) AS aviso_maximo
            FROM base_incurrido 
            HAVING sum_aviso < 1000 
                AND pago_maximo < 1000 
                AND aviso_maximo > 1000
            GROUP BY 1, 2, 3, 4, 5, 7
        ) AS fmax GROUP BY 1, 2, 3, 4, 5, 6
        
)
WITH DATA PRIMARY INDEX (
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , cobertura_general_desc
    , cobertura_desc
    , atipico
) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE base_pagos_aviso AS
(
    SELECT
        fecha_siniestro
        , codigo_ramo_op
        , fecha_registro 
        , cobertura_general_desc
        , cobertura_desc
        , atipico
        , ZEROIFNULL(SUM(pago_bruto)) AS pago_bruto
        , ZEROIFNULL(SUM(pago_retenido)) AS pago_retenido
        , ZEROIFNULL(SUM(aviso_bruto)) AS aviso_bruto
        , ZEROIFNULL(SUM(aviso_retenido)) AS aviso_retenido
    FROM base_incurrido
    GROUP BY 1, 2, 3, 4, 5, 6  
)
WITH DATA PRIMARY INDEX
(
    fecha_siniestro
    , fecha_registro
    , codigo_ramo_op
    , atipico
    , cobertura_general_desc
    , cobertura_desc
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
    , '040' AS codigo_ramo_op
    , 'AUTOMOVILES' AS ramo_desc
    , base.atipico
    , COALESCE(base.cobertura_general_desc, '-1') AS cobertura_general_desc
    , COALESCE(base.cobertura_desc, '-1') AS cobertura_desc
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
ON base.fecha_siniestro = contp.fecha_siniestro
AND base.fecha_registro = contp.fecha_registro
AND base.codigo_ramo_op = contp.codigo_ramo_op
AND base.cobertura_desc = contp.cobertura_desc
AND base.atipico = contp.atipico
AND base.cobertura_general_desc = contp.cobertura_general_desc


LEFT JOIN conteo_incurrido_bruto AS conti
ON base.fecha_siniestro = conti.fecha_siniestro
AND base.fecha_registro = conti.fecha_registro
AND base.codigo_ramo_op = conti.codigo_ramo_op
AND base.cobertura_desc = conti.cobertura_desc
AND base.atipico = conti.atipico
AND base.cobertura_general_desc = conti.cobertura_general_desc


LEFT JOIN conteo_desistido_bruto AS contd
ON base.fecha_siniestro = contd.fecha_siniestro
AND base.fecha_registro = contd.fecha_registro
AND base.codigo_ramo_op = contd.codigo_ramo_op
AND base.cobertura_desc = contd.cobertura_desc
AND base.atipico = contd.atipico
AND base.cobertura_general_desc = contd.cobertura_general_desc


LEFT JOIN 
mdb_seguros_colombia.v_ramo AS ramo
ON base.codigo_ramo_op = ramo.codigo_ramo_op
INNER JOIN fechas AS focurr ON base.fecha_siniestro = focurr.dia_dt
INNER JOIN fechas AS fmov ON base.fecha_registro = fmov.dia_dt

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

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
    codigo_op
    , codigo_ramo_op
    , cobertura_desc
    , cobertura_general_desc
    , ramo_desc
    , atipico
    , fecha_siniestro
    , fecha_registro
) ON COMMIT PRESERVE ROWS;

SELECT * FROM siniestros_final ORDER BY 1, 2, 3, 4, 5, 6, 7, 8

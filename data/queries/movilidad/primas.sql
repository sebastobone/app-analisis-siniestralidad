CREATE MULTISET VOLATILE TABLE fechas AS (
    SELECT DISTINCT
		Mes_Id
		,MIN(Dia_Dt) OVER (PARTITION BY Mes_Id) AS Primer_dia_mes
		,MAX(Dia_Dt) OVER (PARTITION BY Mes_Id) AS Ultimo_dia_mes
		,Cast((Ultimo_dia_mes - Primer_dia_mes + 1)*1.00 AS DECIMAL(18,0)) Num_dias_mes
	FROM MDB_SEGUROS_COLOMBIA.V_DIA
	WHERE Mes_Id BETWEEN CAST('{mes_primera_ocurrencia}' AS INTEGER ) 
                    AND CAST('{mes_corte}' AS INTEGER)
) WITH DATA PRIMARY INDEX (Mes_Id) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE primas AS (
    SELECT
        codigo_op
        , codigo_ramo_op 
        , primer_dia_mes AS fecha_registro 
        , cobertura_general_desc
        ,SUM(prima_bruta) AS prima_bruta
		,SUM(prima_bruta_devengada) AS prima_bruta_devengada
		,SUM(prima_retenida) AS prima_retenida
		,SUM(prima_retenida_devengada) AS prima_retenida_devengada

    FROM (
        SELECT
        fechas.primer_dia_mes 
        ,'01' AS codigo_op
        , '040' AS codigo_ramo_op
        , CASE 
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND pol.sucursal_id IN (21170919, 20056181, 52915901) THEN 'MOTOS SUFI'
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) THEN 'MOTOS RESTO'
            WHEN rtdc.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
            WHEN rtdc.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694) THEN 'RC'
            ELSE 'PARCIALES'
        END AS cobertura_general_desc
        , SUM(CASE WHEN n1.nivel_indicador_uno_id IN (1) THEN rtdc.valor_indicador ELSE 0 END) AS prima_bruta
        , SUM(CASE WHEN n1.nivel_indicador_uno_id IN (1,5) THEN rtdc.Valor_Indicador ELSE 0 END) Prima_Bruta_Devengada
		, SUM(CASE WHEN n1.Nivel_Indicador_Uno_Id IN (1,2) THEN rtdc.Valor_Indicador ELSE 0 END) Prima_Retenida
		, SUM(CASE WHEN n1.Nivel_Indicador_Uno_Id IN (1,2,5) THEN rtdc.Valor_Indicador ELSE 0 END) Prima_Retenida_Devengada

        FROM mdb_seguros_colombia.v_rt_detalle_cobertura AS rtdc
        LEFT JOIN mdb_seguros_colombia.v_rt_nivel_indicador_cinco AS n5 
            ON rtdc.nivel_indicador_cinco_id = n5.nivel_indicador_cinco_id 
        LEFT JOIN mdb_seguros_colombia.v_rt_nivel_indicador_uno AS n1 
            ON n5.nivel_indicador_uno_id = n1.nivel_indicador_uno_id
            AND n5.compania_origen_id = n1.compania_origen_id

        LEFT JOIN mdb_seguros_colombia.v_poliza_certificado_etl AS pcetl
            ON rtdc.poliza_certificado_id = pcetl.poliza_certificado_id
        LEFT JOIN mdb_seguros_colombia.v_vehiculo AS vehi
            ON vehi.bien_id = pcetl.bien_id
        LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas 
            ON fas.fasecolda_cd = vehi.fasecolda_cd
        LEFT JOIN mdb_seguros_colombia.v_plan AS plan   
            ON plan.plan_id = pcetl.plan_id
        LEFT JOIN mdb_seguros_colombia.v_poliza AS pol
            ON pol.poliza_id = rtdc.poliza_id
        INNER JOIN fechas AS fechas     
            ON fechas.mes_id = rtdc.mes_id
    WHERE rtdc.ramo_id = 168
    AND plan.compania_id = 4
    AND n1.nivel_indicador_uno_id IN (1,2,5)
    
    GROUP BY 1, 2, 3, 4

    UNION ALL    

    SELECT
        fechas.primer_dia_mes 
        , '01' AS codigo_op 
        , '040' AS codigo_ramo_op
        , CASE 
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) AND rtrs.sucursal_id IN (21170919, 20056181, 52915901) THEN 'MOTOS SUFI'
            WHEN fas.clase_tarifa_cd IN (3, 4, 5, 6) THEN 'MOTOS RESTO'
            WHEN rtrs.articulo_id IN (79, 190, 163, 249) THEN 'TOTALES'
            WHEN rtrs.amparo_id IN (14489, 14277, 14122, 14771, 56397, 694) THEN 'RC'
            ELSE 'PARCIALES'
        END AS cobertura_general_desc
        , SUM(CASE WHEN n1.nivel_indicador_uno_id IN (1) THEN rtrs.valor_indicador ELSE 0 END) AS prima_bruta
        , SUM(CASE WHEN n1.nivel_indicador_uno_id IN (1,5) THEN rtrs.Valor_Indicador ELSE 0 END) Prima_Bruta_Devengada
		, SUM(CASE WHEN n1.Nivel_Indicador_Uno_Id IN (1,2) THEN rtrs.Valor_Indicador ELSE 0 END) Prima_Retenida
		, SUM(CASE WHEN n1.Nivel_Indicador_Uno_Id IN (1,2,5) THEN rtrs.Valor_Indicador ELSE 0 END) Prima_Retenida_Devengada

        FROM mdb_seguros_colombia.v_rt_ramo_sucursal AS rtrs
        LEFT JOIN mdb_seguros_colombia.v_rt_nivel_indicador_cinco AS n5 
            ON rtrs.nivel_indicador_cinco_id = n5.nivel_indicador_cinco_id 
        LEFT JOIN mdb_seguros_colombia.v_rt_nivel_indicador_uno AS n1 
            ON n5.nivel_indicador_uno_id = n1.nivel_indicador_uno_id
            AND n5.compania_origen_id = n1.compania_origen_id

         LEFT JOIN mdb_seguros_colombia.v_poliza_certificado_etl AS pcetl
            ON rtrs.poliza_certificado_id = pcetl.poliza_certificado_id
        LEFT JOIN mdb_seguros_colombia.v_vehiculo AS vehi
            ON vehi.bien_id = pcetl.bien_id
        LEFT JOIN mdb_seguros_colombia.v_fasecolda AS fas 
            ON fas.fasecolda_cd = vehi.fasecolda_cd
        LEFT JOIN mdb_seguros_colombia.v_plan AS plan   
            ON plan.plan_id = pcetl.plan_id
        
        INNER JOIN fechas AS fechas     
            ON fechas.mes_id = rtrs.mes_id
    WHERE rtrs.ramo_id = 168
    AND plan.compania_id = 4
    AND n1.nivel_indicador_uno_id IN (1,2,5)

    GROUP BY  1, 2, 3, 4
    ) AS base
    GROUP BY 1, 2, 3, 4
 ) WITH DATA PRIMARY INDEX (
    fecha_registro
    , codigo_op
    , codigo_ramo_op
    , cobertura_general_desc
) ON COMMIT PRESERVE ROWS;

SELECT * FROM primas
ORDER BY 1, 2, 3, 4

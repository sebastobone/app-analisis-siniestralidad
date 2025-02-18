CREATE MULTISET VOLATILE TABLE canal_poliza
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , ramo_id INTEGER NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , poliza_id BIGINT NOT NULL
    , numero_poliza VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
) PRIMARY INDEX (
    poliza_id, codigo_ramo_op, compania_id
) ON COMMIT PRESERVE ROWS;
INSERT INTO canal_poliza VALUES (?, ?, ?, ?, ?, ?, ?);  -- noqa:
COLLECT STATISTICS ON canal_poliza INDEX (poliza_id, codigo_ramo_op, compania_id);  -- noqa:


CREATE MULTISET VOLATILE TABLE canal_canal
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , canal_comercial_id BIGINT NOT NULL
    , codigo_canal_comercial_op VARCHAR(20) NOT NULL
    , nombre_canal_comercial VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
) PRIMARY INDEX (canal_comercial_id, codigo_ramo_op, compania_id) ON COMMIT PRESERVE ROWS;
INSERT INTO canal_canal VALUES (?, ?, ?, ?, ?, ?, ?);  -- noqa:
COLLECT STATISTICS ON canal_canal INDEX (canal_comercial_id, codigo_ramo_op, compania_id);  -- noqa:


CREATE MULTISET VOLATILE TABLE canal_sucursal
(
    compania_id SMALLINT NOT NULL
    , codigo_op VARCHAR(100) NOT NULL
    , codigo_ramo_op VARCHAR(100) NOT NULL
    , sucursal_id BIGINT NOT NULL
    , codigo_sucural_op VARCHAR(10) NOT NULL
    , nombre_sucursal VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
) PRIMARY INDEX (sucursal_id, codigo_ramo_op, compania_id) ON COMMIT PRESERVE ROWS;
INSERT INTO canal_sucursal VALUES (?, ?, ?, ?, ?, ?, ?);  -- noqa:
COLLECT STATISTICS ON canal_sucursal INDEX (sucursal_id, codigo_ramo_op, compania_id);  -- noqa:


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
COLLECT STATISTICS ON fechas INDEX (mes_id);


CREATE MULTISET VOLATILE TABLE meses_devengue AS
(
    SELECT
        mes_id
        , MIN(dia_dt) AS primer_dia_mes
        , MAX(dia_dt) AS ultimo_dia_mes
        , ultimo_dia_mes - primer_dia_mes + 1 AS num_dias_mes
    FROM mdb_seguros_colombia.v_dia
    WHERE
        mes_id BETWEEN CAST('{mes_corte}' AS INTEGER)
        - 300 AND CAST('{mes_corte}' AS INTEGER)
        + 200
    GROUP BY 1
) WITH DATA PRIMARY INDEX (mes_id) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE primas_cedidas_rpnd_sap
(
    codigo_op VARCHAR(2) NOT NULL
    , codigo_ramo_op VARCHAR(3) NOT NULL
    , mes_id INTEGER NOT NULL
    , prima_bruta FLOAT NOT NULL
    , prima_retenida FLOAT NOT NULL
    , prima_bruta_devengada FLOAT NOT NULL
    , prima_retenida_devengada FLOAT NOT NULL
    , mov_rpnd_bruto FLOAT NOT NULL
    , mov_rpnd_cedido FLOAT NOT NULL
    , mov_rpnd_retenido FLOAT NOT NULL
    , prima_cedida FLOAT NOT NULL
) PRIMARY INDEX (codigo_op, codigo_ramo_op, mes_id) ON COMMIT PRESERVE ROWS;
INSERT INTO primas_cedidas_rpnd_sap VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);  -- noqa:


CREATE MULTISET VOLATILE TABLE gastos_expedicion
(
    codigo_op VARCHAR(2) NOT NULL
    , codigo_ramo_op VARCHAR(3) NOT NULL
    , ano_id INTEGER NOT NULL
    , porcentaje_gastos FLOAT NOT NULL
) PRIMARY INDEX (codigo_op, codigo_ramo_op, ano_id) ON COMMIT PRESERVE ROWS;
INSERT INTO gastos_expedicion VALUES (?, ?, ?, ?);  -- noqa:



CREATE MULTISET VOLATILE TABLE primas_rtdc
(
    mes_id INTEGER NOT NULL
    , codigo_op VARCHAR(2) NOT NULL
    , codigo_ramo_op VARCHAR(3) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
    , tipo_produccion VARCHAR(20) NOT NULL
    , prima_bruta FLOAT NOT NULL
    , prima_bruta_devengada FLOAT NOT NULL
    , prima_retenida FLOAT NOT NULL
    , prima_retenida_devengada FLOAT NOT NULL
) PRIMARY INDEX (
    mes_id, codigo_op, codigo_ramo_op, apertura_canal_desc, tipo_produccion
) ON COMMIT PRESERVE ROWS;

INSERT INTO PRIMAS_RTDC
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
) AS base

GROUP BY 1, 2, 3, 4, 5;



CREATE MULTISET VOLATILE TABLE primas_evpro AS
(
    SELECT
        evpro_cob.mes_id
        , cia.codigo_op
        , CASE
            WHEN
                pro.ramo_id = 78
                AND evpro_cob.amparo_id NOT IN (
                    18647, 641, 930, 64082, 61296, -1
                )
                THEN 'AAV'
            ELSE ramo.codigo_ramo_op
        END AS codigo_ramo_aux
        , COALESCE(
            p.apertura_canal_desc, c.apertura_canal_desc, s.apertura_canal_desc
            , CASE
                WHEN
                    pro.ramo_id IN (78, 274)
                    AND pro.compania_id = 3
                    THEN 'Otros Banca'
                WHEN
                    pro.ramo_id = 274 AND pro.compania_id = 4
                    THEN 'Otros'
                ELSE 'Resto'
            END
        ) AS apertura_canal_desc
        , CASE
            WHEN evpro_cob.valor_prima < 0 THEN 'NEGATIVA' ELSE 'POSITIVA'
        END AS tipo_produccion
        , SUM(
            CAST(
                evpro_cob.valor_prima * evpro_cob.valor_tasa AS FLOAT
            )
        ) AS prima_bruta

    FROM mdb_seguros_colombia.v_evento_prod_cobertura AS evpro_cob
    INNER JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON (evpro_cob.poliza_id = poli.poliza_id)
    INNER JOIN
        mdb_seguros_colombia.v_poliza_certificado_etl AS pcetl
        ON (evpro_cob.poliza_certificado_id = pcetl.poliza_certificado_id)
    INNER JOIN
        mdb_seguros_colombia.v_plan_individual AS pind
        ON (evpro_cob.plan_individual_id = pind.plan_individual_id)
    INNER JOIN
        mdb_seguros_colombia.v_producto AS pro
        ON (pind.producto_id = pro.producto_id)
    INNER JOIN
        mdb_seguros_colombia.v_ramo AS ramo
        ON (pro.ramo_id = ramo.ramo_id)
    INNER JOIN
        mdb_seguros_colombia.v_compania AS cia
        ON (pro.compania_id = cia.compania_id)
    INNER JOIN
        mdb_seguros_colombia.v_sucursal AS sucu
        ON (poli.sucursal_id = sucu.sucursal_id)
    LEFT JOIN
        canal_poliza AS p
        ON
			evpro_cob.poliza_id = p.poliza_id
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
            AND sucu.sucursal_id = s.sucursal_id
            AND cia.compania_id = s.compania_id
        )

    WHERE
        pro.ramo_id IN (54835, 274, 78, 57074, 140, 107, 271, 297, 204)
        AND pro.compania_id IN (3, 4)
        AND evpro_cob.mes_id = CAST('{mes_corte}' AS INTEGER)

    GROUP BY 1, 2, 3, 4, 5

) WITH DATA PRIMARY INDEX (
    mes_id, codigo_op, codigo_ramo_aux, apertura_canal_desc, tipo_produccion
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE base_primas AS
(
    WITH sap AS (
        SELECT
            mes_id
            , codigo_op
            , codigo_ramo_op
            , 'POSITIVA' AS tipo_produccion
            , prima_cedida
            , CASE
                WHEN
                    codigo_op = '02' AND codigo_ramo_op IN ('081', 'AAV', '083')
                    THEN 'No Banca'
                ELSE 'Resto'
            END AS apertura_canal_desc
        FROM primas_cedidas_rpnd_sap
    )

    SELECT
        COALESCE(rtdc.mes_id, evpro.mes_id) AS mes_id
        , COALESCE(rtdc.codigo_op, evpro.codigo_op) AS codigo_op
        , COALESCE(rtdc.codigo_ramo_op, evpro.codigo_ramo_aux) AS codigo_ramo_op
        , COALESCE(rtdc.apertura_canal_desc, evpro.apertura_canal_desc)
            AS apertura_canal_desc
        , COALESCE(rtdc.tipo_produccion, evpro.tipo_produccion)
            AS tipo_produccion
        , SUM(
            CASE
                WHEN
                    COALESCE(rtdc.mes_id, evpro.mes_id)
                    = CAST('{mes_corte}' AS INTEGER)
                    AND CAST('{aproximar_reaseguro}' AS INTEGER) = 1
                    THEN evpro.prima_bruta
                ELSE rtdc.prima_bruta
            END
        ) AS prima_bruta
        , SUM(
            CASE
                WHEN
                    COALESCE(rtdc.mes_id, evpro.mes_id)
                    = CAST('{mes_corte}' AS INTEGER)
                    AND CAST('{aproximar_reaseguro}' AS INTEGER) = 1
                    THEN evpro.prima_bruta - ZEROIFNULL(sap.prima_cedida)
                ELSE rtdc.prima_retenida
            END
        ) AS prima_retenida
        , SUM(rtdc.prima_bruta_devengada) AS prima_bruta_devengada
        , SUM(rtdc.prima_retenida_devengada) AS prima_retenida_devengada

    FROM primas_rtdc AS rtdc
    FULL OUTER JOIN primas_evpro AS evpro
        ON (
            rtdc.mes_id = evpro.mes_id
            AND rtdc.codigo_op = evpro.codigo_op
            AND rtdc.codigo_ramo_op = evpro.codigo_ramo_aux
            AND rtdc.apertura_canal_desc = evpro.apertura_canal_desc
            AND rtdc.tipo_produccion = evpro.tipo_produccion
        )
    LEFT JOIN sap
        ON (
            sap.mes_id = COALESCE(rtdc.mes_id, evpro.mes_id)
            AND sap.codigo_op = COALESCE(rtdc.codigo_op, evpro.codigo_op)
            AND sap.codigo_ramo_op
            = COALESCE(rtdc.codigo_ramo_op, evpro.codigo_ramo_aux)
            AND sap.apertura_canal_desc
            = COALESCE(rtdc.apertura_canal_desc, evpro.apertura_canal_desc)
            AND sap.tipo_produccion
            = COALESCE(rtdc.tipo_produccion, evpro.tipo_produccion)
        )

    GROUP BY 1, 2, 3, 4, 5

) WITH DATA PRIMARY INDEX (
    mes_id, codigo_op, codigo_ramo_op, apertura_canal_desc, tipo_produccion
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE base_perfiles_devengue AS
(
    SELECT
        evpro.mes_id
        , cia.codigo_op
        , CASE
            WHEN ramo.ramo_desc = 'VIDA INDIVIDUAL' THEN 'AAV' ELSE
                ramo.codigo_ramo_op
        END AS codigo_ramo_aux
        , COALESCE(
            p.apertura_canal_desc, c.apertura_canal_desc, s.apertura_canal_desc
            , CASE
                WHEN
                    pro.ramo_id IN (78, 274)
                    AND pro.compania_id = 3
                    THEN 'Otros Banca'
                WHEN
                    pro.ramo_id = 78 AND pro.compania_id = 4
                    THEN 'Otros'
                ELSE 'Resto'
            END
        ) AS apertura_canal_desc

        , CASE
            WHEN evpro.fecha_fin_vigencia_dcto < evpro.fecha_registro THEN 'MV'
            WHEN
                evpro.fecha_fin_vigencia_dcto
                - evpro.fecha_inicio_vigencia_dcto
                + 1
                <= 32
                THEN 'PC'
            ELSE 'Anualizada'
        END AS tipo_vigencia
        , evpro.fecha_fin_vigencia_dcto
        - evpro.fecha_inicio_vigencia_dcto
        + 1 AS dias_vigencia
        , evpro.fecha_fin_vigencia_dcto
        - evpro.fecha_registro
        + 1 AS dias_constitucion
        , EXTRACT(DAY FROM evpro.fecha_registro) AS dia_contabilizacion

        , CASE WHEN evpro.valor_prima < 0 THEN 'NEGATIVA' ELSE 'POSITIVA' END
            AS tipo_produccion
        , SUM(
            CAST((evpro.valor_prima * evpro.valor_tasa) AS DECIMAL(18, 6))
            - COALESCE(evpro_cob.valor_pdn, 0)
        ) AS prima_bruta_devengable

    FROM mdb_seguros_colombia.v_evento_produccion AS evpro
    INNER JOIN
        mdb_seguros_colombia.v_poliza AS poli
        ON (evpro.poliza_id = poli.poliza_id)
    INNER JOIN
        mdb_seguros_colombia.v_poliza_certificado_etl AS pcetl
        ON (evpro.poliza_certificado_id = pcetl.poliza_certificado_id)
    INNER JOIN
        mdb_seguros_colombia.v_plan_individual AS pind
        ON (evpro.plan_individual_id = pind.plan_individual_id)
    INNER JOIN
        mdb_seguros_colombia.v_producto AS pro
        ON (pind.producto_id = pro.producto_id)
    INNER JOIN
        mdb_seguros_colombia.v_ramo AS ramo
        ON (pro.ramo_id = ramo.ramo_id)
    INNER JOIN
        mdb_seguros_colombia.v_compania AS cia
        ON (pro.compania_id = cia.compania_id)
    INNER JOIN
        mdb_seguros_colombia.v_sucursal AS sucu
        ON (poli.sucursal_id = sucu.sucursal_id)

    -- Subquery que filtra los movimientos de amparos excluidos de reserva
    LEFT JOIN (
        SELECT
            evento_id
            , CAST(
                SUM(evpro_cob.valor_prima * evpro_cob.valor_tasa)
                AS FLOAT
            ) AS valor_pdn

        FROM mdb_seguros_colombia.v_evento_prod_cobertura AS evpro_cob
        INNER JOIN
            mdb_seguros_colombia.v_plan_individual AS pind
            ON (evpro_cob.plan_individual_id = pind.plan_individual_id)
        INNER JOIN
            mdb_seguros_colombia.v_producto AS pro
            ON (pind.producto_id = pro.producto_id)

        WHERE
            evpro_cob.amparo_id IN (63717, 18647, 64082, 641, 930, -1)
            AND pro.ramo_id = 78

        GROUP BY 1
    ) AS evpro_cob ON (evpro.evento_id = evpro_cob.evento_id)

    LEFT JOIN
        canal_poliza AS p
        ON
            evpro.poliza_id = p.poliza_id
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
            AND sucu.sucursal_id = s.sucursal_id
            AND cia.compania_id = s.compania_id
        )

    WHERE
        pro.ramo_id IN (54835, 78, 274, 57074, 140, 107, 271, 297, 204)
        AND pro.compania_id IN (3, 4)
        AND evpro.mes_id BETWEEN CAST('{mes_corte}' AS INTEGER)
        - 200 AND CAST('{mes_corte}' AS INTEGER)

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    HAVING prima_bruta_devengable <> 0

) WITH DATA PRIMARY INDEX (
    mes_id, codigo_op, codigo_ramo_aux, apertura_canal_desc, tipo_produccion
) ON COMMIT PRESERVE ROWS;




CREATE MULTISET VOLATILE TABLE PERFILES_DEVENGUE AS
(
	WITH perfil1 AS (
		SELECT
			base.*
			,SUM(Prima_Bruta_Devengable) OVER (PARTITION BY Mes_Id, Codigo_Op, Codigo_Ramo_Aux, Apertura_Canal_Desc, Tipo_Vigencia, Tipo_Produccion) AS Total_Pdn
			,CAST(CAST(Prima_Bruta_Devengable AS FLOAT) / CASE WHEN Total_Pdn = 0 THEN 1E9 ELSE Total_Pdn END AS DECIMAL(18,6)) AS Peso
		FROM BASE_PERFILES_DEVENGUE AS base
	),

	perfil2 AS (
		SELECT
			apertura_canal_desc
			, codigo_ramo_aux AS codigo_ramo_op
			, codigo_op
			, mes_id
			, tipo_vigencia
			, tipo_produccion
			, SUM(dia_contabilizacion * peso) AS dia_contabilizacion
			, SUM(dias_vigencia * peso) AS dias_vigencia
			, SUM(dias_constitucion * peso) AS dias_constitucion
			, SUM(prima_bruta_devengable) AS prima_bruta_devengable
		FROM perfil1
		GROUP BY 1, 2, 3, 4, 5, 6
	)

	SELECT
		perfil2.*
		,SUM(Prima_Bruta_Devengable) OVER (PARTITION BY Mes_Id, Codigo_Op, Codigo_Ramo_Op, Apertura_Canal_Desc, Tipo_Produccion) AS Total_Pdn
		,CAST(CAST(perfil2.Prima_Bruta_Devengable AS FLOAT) / CASE WHEN Total_Pdn = 0 THEN 1E9 ELSE Total_Pdn END AS DECIMAL(18,6)) AS Porcentaje_Produccion
		
	FROM perfil2
	
) WITH DATA PRIMARY INDEX (Mes_Id, Codigo_Op, Codigo_Ramo_Op, Apertura_Canal_Desc, Tipo_Produccion) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE input_devengue AS
(
    SELECT
        perf.apertura_canal_desc
        , perf.codigo_ramo_op
        , perf.codigo_op
        , perf.mes_id
        , perf.tipo_vigencia
        , perf.tipo_produccion
        , ROUND(perf.dias_vigencia) AS dias_vigencia
        , ROUND(perf.dias_constitucion) AS dias_constitucion
        , ROUND(perf.dia_contabilizacion) AS dia_contabilizacion
        , pdn_real.prima_bruta
        * perf.porcentaje_produccion AS prima_bruta_devengable
        , pdn_real.prima_retenida
        * perf.porcentaje_produccion AS prima_retenida_devengable

    FROM base_primas AS pdn_real
    INNER JOIN perfiles_devengue
        AS perf ON (pdn_real.apertura_canal_desc = perf.apertura_canal_desc)
    AND (pdn_real.codigo_ramo_op = perf.codigo_ramo_op)
    AND (pdn_real.codigo_op = perf.codigo_op)
    AND (pdn_real.mes_id = perf.mes_id)
    AND (pdn_real.tipo_produccion = perf.tipo_produccion)

) WITH DATA PRIMARY INDEX (
    mes_id, codigo_op, codigo_ramo_op, apertura_canal_desc, tipo_produccion
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE devengue AS
(
    WITH dev AS (
        SELECT
            base.codigo_op
            , base.codigo_ramo_op
            , base.apertura_canal_desc
            , base.mes_id AS mes_pdn

            , base.tipo_vigencia
            , base.tipo_produccion
            , base.dias_vigencia
            , base.dias_constitucion
            , base.dia_contabilizacion
            , base.prima_bruta_devengable

            , base.prima_retenida_devengable
            , riego.mes_id

            , riego.primer_dia_mes
            , riego.ultimo_dia_mes

            , CAST((base.mes_id - 190000) * 100 + 1 AS DATE)
            + GREATEST(base.dia_contabilizacion, 1)
            - 1 AS fecha_contabilizacion
            , CASE
                WHEN tipo_vigencia = 'PC' AND MOD(base.mes_id, 100) = 1 AND dia_contabilizacion > 25 THEN LAST_DAY(fecha_contabilizacion + 15) -- En esta fecha se estaban creando 3 filas para el riego, no debe ocurrir
                WHEN tipo_vigencia = 'PC' AND tipo_produccion = 'POSITIVA' AND EXTRACT(MONTH FROM fecha_contabilizacion + base.dias_constitucion) = EXTRACT(MONTH FROM fecha_contabilizacion) THEN LAST_DAY(fecha_contabilizacion + 15) -- La pdn positiva siempre debe hacer el 50/50, entonces tiene que tener dos lineas
                ELSE
                    fecha_contabilizacion
                    + GREATEST(ROUND(base.dias_constitucion), 1)
            END AS fecha_fin_vigencia
            , fecha_fin_vigencia
            - GREATEST(ROUND(base.dias_vigencia), 1) AS fecha_inicio_vigencia
            , EXTRACT(YEAR FROM fecha_inicio_vigencia) * 100
            + EXTRACT(MONTH FROM fecha_inicio_vigencia) AS mes_inicio_vigencia

            , EXTRACT(YEAR FROM fecha_fin_vigencia) * 100
            + EXTRACT(MONTH FROM fecha_fin_vigencia) AS mes_fin_vigencia
            , base.prima_bruta_devengable
            / GREATEST(base.dias_vigencia, 1) AS prima_bruta_devengable_diaria
            , base.prima_retenida_devengable
            / GREATEST(base.dias_vigencia, 1)
                AS prima_retenida_devengable_diaria
            , CASE
                WHEN
                    riego.mes_id = GREATEST(base.mes_id, mes_inicio_vigencia)
                    THEN
                        EXTRACT(
                            DAY FROM LEAST(
                                riego.ultimo_dia_mes, fecha_fin_vigencia
                            )
                        )
                        - EXTRACT(
                            DAY FROM GREATEST(
                                fecha_contabilizacion, fecha_inicio_vigencia
                            )
                        )
                WHEN
                    riego.mes_id = mes_fin_vigencia
                    THEN EXTRACT(DAY FROM fecha_fin_vigencia)
                ELSE riego.num_dias_mes
            END AS dias_mes

            , CASE
                WHEN
                    base.mes_id = riego.mes_id AND base.tipo_vigencia = 'PC'
                    THEN -base.prima_bruta_devengable * 0.5
                WHEN
                    base.mes_id = riego.mes_id
                    THEN -prima_bruta_devengable_diaria * base.dias_constitucion
                ELSE 0
            END AS constitucion_bruta

            , CASE
                WHEN riego.mes_id < mes_inicio_vigencia THEN 0
                WHEN
                    riego.mes_id = mes_fin_vigencia
                    AND base.tipo_vigencia = 'PC'
                    THEN base.prima_bruta_devengable * 0.5
                WHEN
                    base.tipo_vigencia <> 'PC'
                    THEN prima_bruta_devengable_diaria * dias_mes
                ELSE 0
            END AS liberacion_bruta

            , CASE
                WHEN
                    base.mes_id = riego.mes_id AND base.tipo_vigencia = 'PC'
                    THEN -base.prima_retenida_devengable * 0.5
                WHEN
                    base.mes_id = riego.mes_id
                    THEN
                        -prima_retenida_devengable_diaria
                        * base.dias_constitucion
                ELSE 0
            END AS constitucion_retenida

            , CASE
                WHEN riego.mes_id < mes_inicio_vigencia THEN 0
                WHEN
                    riego.mes_id = mes_fin_vigencia
                    AND base.tipo_vigencia = 'PC'
                    THEN base.prima_retenida_devengable * 0.5
                WHEN
                    base.tipo_vigencia <> 'PC'
                    THEN prima_retenida_devengable_diaria * dias_mes
                ELSE 0
            END AS liberacion_retenida

        FROM input_devengue AS base
        INNER JOIN meses_devengue AS riego
            ON (
                riego.ultimo_dia_mes >= fecha_contabilizacion
                AND riego.primer_dia_mes <= fecha_fin_vigencia
            )

        WHERE
            base.tipo_vigencia <> 'MV'
            AND base.dias_constitucion > 0
    )

    SELECT
        dev.codigo_op
        , dev.codigo_ramo_op
        , apertura_canal_desc
        , mes_pdn
        , mes_id
        , tipo_vigencia
        , SUM(prima_bruta_devengable) AS prima_bruta_devengable
        , SUM(prima_retenida_devengable) AS prima_retenida_devengable
        , SUM(constitucion_bruta * (1 - gexp.porcentaje_gastos))
            AS constitucion_rpnd_bruta
        , SUM(constitucion_retenida * (1 - gexp.porcentaje_gastos))
            AS constitucion_rpnd_retenida
        , SUM(liberacion_bruta * (1 - gexp.porcentaje_gastos))
            AS liberacion_rpnd_bruta
        , SUM(liberacion_retenida * (1 - gexp.porcentaje_gastos))
            AS liberacion_rpnd_retenida
        , constitucion_rpnd_bruta + liberacion_rpnd_bruta AS mov_rpnd_bruto
        , constitucion_rpnd_retenida
        + liberacion_rpnd_retenida AS mov_rpnd_retenido

    FROM dev

    INNER JOIN gastos_expedicion AS gexp
        ON (
            dev.codigo_ramo_op = gexp.codigo_ramo_op
            AND dev.codigo_op = gexp.codigo_op
            AND CAST(dev.mes_pdn / 100 AS INTEGER) = gexp.ano_id
        )

    GROUP BY 1, 2, 3, 4, 5, 6

) WITH DATA PRIMARY INDEX (
    mes_id, codigo_op, codigo_ramo_op, apertura_canal_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE reparticion_difs_rpnd_sap AS
(
    WITH pond AS (
        SELECT DISTINCT
            codigo_op
            , codigo_ramo_aux AS codigo_ramo_op
            , apertura_canal_desc
            , SUM(prima_bruta_devengable)
                OVER (
                    PARTITION BY codigo_op, codigo_ramo_aux, apertura_canal_desc
                )
                AS prima_bruta_devengable_ramo_canal
            , SUM(prima_bruta_devengable)
                OVER (PARTITION BY codigo_op, codigo_ramo_aux)
                AS prima_bruta_devengable_ramo
            , prima_bruta_devengable_ramo_canal
            / prima_bruta_devengable_ramo AS peso
        FROM base_perfiles_devengue
        WHERE
            mes_id = CAST('{mes_corte}' AS INTEGER)
            AND tipo_vigencia <> 'MV'
    )

    , dev AS (
        SELECT
            codigo_op
            , codigo_ramo_op
            , SUM(mov_rpnd_bruto) AS mov_rpnd_bruto
            , SUM(mov_rpnd_retenido) AS mov_rpnd_retenido
        FROM devengue
        WHERE mes_id = CAST('{mes_corte}' AS INTEGER)
        GROUP BY 1, 2
    )

    SELECT
        pond.codigo_op
        , pond.codigo_ramo_op
        , pond.apertura_canal_desc
        , (dev.mov_rpnd_bruto - sap.mov_rpnd_bruto)
        * pond.peso AS diferencia_mov_rpnd_bruto
        , (dev.mov_rpnd_retenido - sap.mov_rpnd_retenido)
        * pond.peso AS diferencia_mov_rpnd_retenido

    FROM pond

    INNER JOIN dev
        ON (
            pond.codigo_op = dev.codigo_op
            AND pond.codigo_ramo_op = dev.codigo_ramo_op
        )

    INNER JOIN
        primas_cedidas_rpnd_sap AS sap
        ON
            pond.codigo_op = sap.codigo_op
            AND pond.codigo_ramo_op = sap.codigo_ramo_op

) WITH DATA PRIMARY INDEX (
    codigo_op, codigo_ramo_op, apertura_canal_desc
) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE primas_final AS
(
    WITH base AS (
        SELECT
            codigo_op
            , codigo_ramo_op
            , apertura_canal_desc
            , mes_id
            , SUM(prima_bruta) AS prima_bruta
            , SUM(prima_retenida) AS prima_retenida
            , SUM(prima_bruta_devengada) AS prima_bruta_devengada
            , SUM(prima_retenida_devengada) AS prima_retenida_devengada
        FROM base_primas GROUP BY 1, 2, 3, 4
    )

    , dev AS (
        SELECT
            codigo_op
            , codigo_ramo_op
            , apertura_canal_desc
            , mes_id
            , SUM(mov_rpnd_bruto) AS mov_rpnd_bruto
            , SUM(mov_rpnd_retenido) AS mov_rpnd_retenido
        FROM devengue GROUP BY 1, 2, 3, 4
    )

    SELECT
        base.codigo_op
        , base.codigo_ramo_op
        , base.apertura_canal_desc
        , base.mes_id
        , base.prima_bruta
        , base.prima_retenida
        , SUM(dev.mov_rpnd_bruto)
            OVER (
                PARTITION BY
                    base.codigo_op
                    , base.codigo_ramo_op
                    , base.apertura_canal_desc
            )
            AS mov_rpnd_bruto_ramo
        , SUM(dev.mov_rpnd_retenido)
            OVER (
                PARTITION BY
                    base.codigo_op
                    , base.codigo_ramo_op
                    , base.apertura_canal_desc
            )
            AS mov_rpnd_retenido_ramo
        , CASE
            WHEN
                base.mes_id = CAST('{mes_corte}' AS INTEGER)
                AND CAST('{aproximar_reaseguro}' AS INTEGER) = 1
                THEN
                    base.prima_bruta
                    + ZEROIFNULL(dev.mov_rpnd_bruto)
                    - ZEROIFNULL(difs.diferencia_mov_rpnd_bruto)
            ELSE base.prima_bruta_devengada
        END AS prima_bruta_devengada
        , CASE
            WHEN
                base.mes_id = CAST('{mes_corte}' AS INTEGER)
                AND CAST('{aproximar_reaseguro}' AS INTEGER) = 1
                THEN
                    base.prima_retenida
                    + ZEROIFNULL(dev.mov_rpnd_retenido)
                    - ZEROIFNULL(difs.diferencia_mov_rpnd_retenido)
            ELSE base.prima_retenida_devengada
        END AS prima_retenida_devengada

    FROM base
    LEFT JOIN dev
        ON
            (base.apertura_canal_desc = dev.apertura_canal_desc)
            AND (base.codigo_ramo_op = dev.codigo_ramo_op)
            AND (base.codigo_op = dev.codigo_op)
            AND (base.mes_id = dev.mes_id)
    LEFT JOIN reparticion_difs_rpnd_sap AS difs
        ON
            (base.apertura_canal_desc = difs.apertura_canal_desc)
            AND (base.codigo_ramo_op = difs.codigo_ramo_op)
            AND (base.codigo_op = difs.codigo_op)

) WITH DATA PRIMARY INDEX (
    mes_id, codigo_op, codigo_ramo_op, apertura_canal_desc
) ON COMMIT PRESERVE ROWS;



SELECT
    base.codigo_op
    , base.codigo_ramo_op
    , CASE
        WHEN base.codigo_ramo_op = 'AAV' THEN 'ANEXOS VI'
        ELSE ramo.ramo_desc
    END AS ramo_desc
    , COALESCE(base.apertura_canal_desc, '-1') AS apertura_canal_desc
    , 'NO APLICA' AS apertura_amparo_desc
    , fechas.primer_dia_mes AS fecha_registro
    , ZEROIFNULL(SUM(base.prima_bruta)) AS prima_bruta
    , ZEROIFNULL(SUM(base.prima_bruta_devengada)) AS prima_bruta_devengada
    , ZEROIFNULL(SUM(base.prima_retenida)) AS prima_retenida
    , ZEROIFNULL(SUM(base.prima_retenida_devengada)) AS prima_retenida_devengada

FROM primas_final AS base
INNER JOIN fechas ON base.mes_id = fechas.mes_id
LEFT JOIN
    mdb_seguros_colombia.v_ramo AS ramo
    ON base.codigo_ramo_op = ramo.codigo_ramo_op

GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6

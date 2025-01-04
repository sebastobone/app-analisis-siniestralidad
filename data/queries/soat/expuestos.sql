--Se cambio la tabla V_CONTRATO_MSTR por V_CONTRATO
--ya que no estaba trayendo el nombre a unas sucursales)

-- drop table SUCURSALES;
CREATE MULTISET VOLATILE TABLE sucursales
(
    codigo_ramo_op VARCHAR(100) NOT NULL
    , nombre_canal_comercial VARCHAR(100) NOT NULL
    , apertura_canal_desc VARCHAR(100) NOT NULL
    , apertura_canal_cd VARCHAR(100) NOT NULL
) PRIMARY INDEX (nombre_canal_comercial) ON COMMIT PRESERVE ROWS;
INSERT INTO sucursales VALUES (?, ?, ?, ?);  -- noqa:
--sel*from SUCURSALES

-- drop table AMPAROS;
CREATE MULTISET VOLATILE TABLE amparos
(
    codigo_ramo_op VARCHAR(100) NOT NULL
    , amparo_desc VARCHAR(100) NOT NULL
    , apertura_amparo_desc VARCHAR(100) NOT NULL
    , apertura_amparo_cd VARCHAR(100) NOT NULL
) PRIMARY INDEX (amparo_desc) ON COMMIT PRESERVE ROWS;
INSERT INTO amparos VALUES (?, ?, ?, ?);  -- noqa:
--sel*from AMPAROS

-- drop table CLASES;
CREATE MULTISET VOLATILE TABLE clases
(
    codigo_tarifa_cd VARCHAR(3) NOT NULL
    , descuento VARCHAR(2) NOT NULL
    , tipo_vehiculo VARCHAR(100) NOT NULL
    , tipo_vehiculo_cd VARCHAR(100) NOT NULL
) PRIMARY INDEX (codigo_tarifa_cd, descuento) ON COMMIT PRESERVE ROWS;
INSERT INTO clases VALUES (?, ?, ?, ?);  -- noqa:
--sel*from CLASES

CREATE MULTISET VOLATILE TABLE fechas AS
(
    SELECT
        mes_id
        , MIN(dia_dt) AS primer_dia_mes
        , MAX(dia_dt) AS ultimo_dia_mes
        , CAST(ultimo_dia_mes - primer_dia_mes + 1 AS FLOAT)
            AS num_dias_mes
    FROM mdb_seguros_colombia.v_dia
    WHERE
        mes_id BETWEEN CAST('{mes_primera_ocurrencia}' AS INTEGER) AND CAST(
            '{mes_corte}' AS INTEGER
        )
    GROUP BY 1
) WITH DATA PRIMARY INDEX (mes_id) ON COMMIT PRESERVE ROWS;

-- drop table BASE_EXPUESTOS;
CREATE MULTISET VOLATILE TABLE base_expuestos AS
(
    SELECT
        spoli.poliza_id
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
        , spoli.fecha_anulacion AS fecha_cancelacion
        , spoli.fecha_inicio AS fecha_inclusion_cobertura
        , spoli.fecha_fin AS fecha_exclusion_cobertura
    FROM
        (SELECT
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
                    AND sp.fecha_expedicion >= '2022/12/19'
                    THEN 'Si'
                ELSE 'No'
            END AS descuento
        FROM mdb_seguros_colombia.vsoat_poliza AS sp) AS spoli
    --LEFT JOIN mdb_seguros_colombia.V_POLIZA poli ON (poli.poliza_id = spoli.poliza_id)
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
        ON (spoli.poliza_id = cont.contrato_id)
    LEFT JOIN
        mdb_seguros_colombia.v_sucursal AS sucu
        ON (cont.sucursal_id = sucu.sucursal_id)
    LEFT JOIN
        mdb_seguros_colombia.v_canal_comercial AS cacomer
        ON sucu.canal_comercial_id = cacomer.canal_comercial_id



    LEFT JOIN
        (SELECT DISTINCT
            codigo_ramo_op
            , nombre_canal_comercial
            , apertura_canal_desc
        FROM sucursales) AS s
        ON
            (
                s.codigo_ramo_op = '041'
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
                amparo.codigo_ramo_op = '041'
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

    --WHERE ramo.codigo_ramo_op IN ('041')
    --and pc.poliza_certificado_id = '122652357'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    QUALIFY
        ROW_NUMBER()
            OVER (
                PARTITION BY spoli.poliza_id
                ORDER BY fecha_exclusion_cobertura DESC
            )
        = 1
--HAVING MAX(pc.Fecha_Fin_Ultima_Vigencia) >= CAST('2012/10/01' AS DATE FORMAT 'YYYY/MM/DD')

) WITH DATA PRIMARY INDEX (
    poliza_id
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
    , descuento
) ON COMMIT PRESERVE ROWS;

-- drop table EXPUESTOS;
CREATE MULTISET VOLATILE TABLE expuestos AS
(
    SELECT
        primer_dia_mes
        , codigo_ramo_op
        , apertura_canal_desc
        , apertura_amparo_desc
        , CASE
            WHEN apertura_canal_desc = 'CERRADO' THEN 'MOTO' ELSE tipo_vehiculo
        END AS tipo_vehiculo
        , descuento
        , SUM(expuestos) AS expuestos
        , SUM(vigentes) AS vigentes
    FROM (
        SELECT
            fechas.primer_dia_mes
            , vpc.codigo_ramo_op
            , vpc.apertura_canal_desc
            , vpc.apertura_amparo_desc
            , vpc.tipo_vehiculo
            , vpc.descuento
            , GREATEST(vpc.fecha_inclusion_cobertura, fechas.primer_dia_mes)
                AS fecha_inicio
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

        FROM
            (SELECT DISTINCT
                primer_dia_mes
                , ultimo_dia_mes
                , num_dias_mes
            FROM fechas) AS fechas
        INNER JOIN base_expuestos AS vpc
            ON (
                fechas.ultimo_dia_mes >= vpc.fecha_inclusion_cobertura
                AND COALESCE(
                    vpc.fecha_cancelacion
                    , vpc.fecha_exclusion_cobertura
                    , (DATE '3000-01-01')
                )
                >= fechas.primer_dia_mes
            --AND COALESCE(vpc.Fecha_Exclusion_Cobertura, (DATE '3000-01-01')) >= fechas.Primer_dia_mes
            )

        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    ) AS base

    GROUP BY 1, 2, 3, 4, 5, 6
) WITH DATA PRIMARY INDEX (
    primer_dia_mes
    , codigo_ramo_op
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
    , descuento
) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE expuestos_final AS (
    SELECT
        '01' AS codigo_op
        , '041' AS codigo_ramo_op
        , 'AUTOS OBLIGATORIO' AS ramo_desc
        , COALESCE(base.apertura_canal_desc, '-1') AS apertura_canal_desc
        , COALESCE(base.apertura_amparo_desc, '-1') AS apertura_amparo_desc
        , COALESCE(base.tipo_vehiculo, '-1') AS tipo_vehiculo
        , COALESCE(base.descuento, '-1') AS descuento

        , CONCAT(
            base.codigo_ramo_op, '_'
            , agrcanal.apertura_canal_cd, '_'
            , agrclas.tipo_vehiculo_cd, '_'
            , agrampa.apertura_amparo_cd
        ) AS agrupacion_reservas

        , base.primer_dia_mes AS fecha_registro

        , SUM(ZEROIFNULL(base.expuestos)) AS expuestos
        , SUM(ZEROIFNULL(base.vigentes)) AS vigentes

    FROM expuestos AS base
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
        FROM clases) AS agrclas
        ON (base.tipo_vehiculo = agrclas.tipo_vehiculo)
    LEFT JOIN
        (SELECT DISTINCT
            apertura_amparo_desc
            , apertura_amparo_cd
        FROM amparos) AS agrampa
        ON (base.apertura_amparo_desc = agrampa.apertura_amparo_desc)
    --LEFT JOIN MDB_SEGUROS_COLOMBIA.V_RAMO ramo ON (base.Codigo_Ramo_Op = ramo.Codigo_Ramo_Op)

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
) WITH DATA PRIMARY INDEX (
    ramo_desc
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
    , descuento
    , agrupacion_reservas
    , fecha_registro
) ON COMMIT PRESERVE ROWS;


SELECT
    codigo_op
    , codigo_ramo_op
    , ramo_desc
    , apertura_canal_desc
    , apertura_amparo_desc
    , tipo_vehiculo
    , fecha_registro
    , SUM(expuestos) AS expuestos
    , SUM(vigentes) AS vigentes
FROM expuestos_final
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2, 3, 4, 5, 6, 7

/*

--========================
PRUEBAS SOX
--========================
--  SELECT Sum(Expuestos) as Suma_Columna_Expuestos, Count(Ramo_Desc) as Conteo_Registros from EXPUESTOS_FINAL

*/


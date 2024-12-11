CREATE MULTISET VOLATILE TABLE base_bruto
(
    fecha_siniestro DATE
    , fecha_registro DATE
    , sucursal_id BIGINT
    , poliza_id BIGINT
    , asegurado_id BIGINT
    , compania_id SMALLINT
    , plan_individual_id INTEGER
    , ramo_id INTEGER
    , siniestro_id VARCHAR(15)
    , tipo_estado_siniestro_cd VARCHAR(5) NOT NULL
    , amparo_id INTEGER NOT NULL
    , pago_bruto FLOAT
    , aviso_bruto FLOAT
) PRIMARY INDEX (
    fecha_registro
    , poliza_id
    , asegurado_id
    , plan_individual_id
    , siniestro_id
    , amparo_id
) ON COMMIT PRESERVE ROWS;

INSERT INTO base_bruto
SELECT
    sini.fecha_siniestro
    , esc.fecha_registro
    , sucu.sucursal_id
    , poli.poliza_id
    , pc.asegurado_id
    , pro.compania_id
    , esc.plan_individual_id
    , esc.ramo_id
    , esc.siniestro_id
    , sini.tipo_estado_siniestro_cd
    , esc.amparo_id
    , SUM(
        CASE
            WHEN
                esc.tipo_oper_siniestro_cd <> '130'
                THEN esc.valor_siniestro * esc.valor_tasa
            ELSE 0
        END
    ) AS pago_bruto
    -- Aviso corregido
    , SUM(
        CASE
            WHEN
                esc.tipo_oper_siniestro_cd = '130'
                AND sini.fecha_siniestro >= (DATE '2010-01-01')
                THEN esc.valor_siniestro * esc.valor_tasa
            ELSE 0
        END
    ) AS aviso_bruto

FROM mdb_seguros_colombia.v_evento_siniestro_cobertura AS esc
INNER JOIN
    mdb_seguros_colombia.v_plan_individual_mstr AS pind
    ON esc.plan_individual_id = pind.plan_individual_id
INNER JOIN
    mdb_seguros_colombia.v_producto AS pro
    ON pind.producto_id = pro.producto_id
INNER JOIN
    mdb_seguros_colombia.v_siniestro AS sini
    ON esc.siniestro_id = sini.siniestro_id
INNER JOIN
    mdb_seguros_colombia.v_poliza AS poli
    ON esc.poliza_id = poli.poliza_id
INNER JOIN
    mdb_seguros_colombia.v_sucursal AS sucu
    ON poli.sucursal_id = sucu.sucursal_id
INNER JOIN
    mdb_seguros_colombia.v_poliza_certificado AS pc
    ON
        esc.poliza_certificado_id = pc.poliza_certificado_id
        AND esc.plan_individual_id = pc.plan_individual_id

WHERE
    esc.ramo_id IN (
        54835, 78, 274, 57074, 140, 107, 271, 297, 204
    )
    AND pro.compania_id IN (3, 4)
    AND esc.valor_siniestro <> 0
    AND esc.mes_id BETWEEN CAST('{chunk_ini}' AS INTEGER) AND CAST(
        '{chunk_fin}' AS INTEGER
    )

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11

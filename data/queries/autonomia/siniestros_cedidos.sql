CREATE MULTISET VOLATILE TABLE base_cedido
(
    fecha_siniestro DATE
    , fecha_registro DATE NOT NULL
    , sucursal_id INTEGER
    , numero_poliza VARCHAR(20)
    , asegurado_id BIGINT
    , plan_individual_id INTEGER NOT NULL
    , siniestro_id VARCHAR(15) NOT NULL
    , tipo_estado_siniestro_cd VARCHAR(5)
    , amparo_id INTEGER NOT NULL
    , pago_cedido FLOAT NOT NULL
    , aviso_cedido FLOAT NOT NULL
) PRIMARY INDEX (
    fecha_registro
    , numero_poliza
    , asegurado_id
    , plan_individual_id
    , siniestro_id
    , amparo_id
) ON COMMIT PRESERVE ROWS;

INSERT INTO base_cedido
SELECT
    sini.fecha_siniestro
    , esc.fecha_registro
    , sucu.sucursal_id
    , poli.numero_poliza
    , pc.asegurado_id
    , esc.plan_individual_id
    , esc.siniestro_id
    , sini.tipo_estado_siniestro_cd
    , esc.amparo_id
    , SUM(
        CASE
            WHEN
                ers.numero_documento <> '-1'
                THEN esc.valor_siniestro_cedido * esc.valor_tasa
            ELSE 0
        END
    ) AS pago_cedido
    --Aviso corregido
    , SUM(
        CASE
            WHEN
                ers.numero_documento = '-1'
                AND sini.fecha_siniestro >= (DATE '2010-01-01')
                THEN esc.valor_siniestro_cedido * esc.valor_tasa
            ELSE 0
        END
    ) AS aviso_cedido

FROM mdb_seguros_colombia.v_evento_reaseguro_sini_cob AS esc
LEFT JOIN
    mdb_seguros_colombia.v_evento_reaseguro_sini AS ers
    ON esc.evento_id = ers.evento_id
LEFT JOIN
    mdb_seguros_colombia.v_plan_individual_mstr AS pind
    ON esc.plan_individual_id = pind.plan_individual_id
LEFT JOIN
    mdb_seguros_colombia.v_producto AS pro
    ON pind.producto_id = pro.producto_id
LEFT JOIN
    mdb_seguros_colombia.v_siniestro AS sini
    ON esc.siniestro_id = sini.siniestro_id
LEFT JOIN
    mdb_seguros_colombia.v_poliza AS poli
    ON esc.poliza_id = poli.poliza_id
LEFT JOIN
    mdb_seguros_colombia.v_sucursal AS sucu
    ON poli.sucursal_id = sucu.sucursal_id
LEFT JOIN
    mdb_seguros_colombia.v_poliza_certificado AS pc
    ON
        esc.poliza_certificado_id = pc.poliza_certificado_id
        AND esc.plan_individual_id = pc.plan_individual_id

WHERE
    pro.ramo_id IN (
        54835, 78, 274, 57074, 140, 107, 271, 297, 204
    )
    AND pro.compania_id IN (3, 4)
    AND esc.valor_siniestro_cedido <> 0
    AND esc.mes_id BETWEEN CAST('{mes_primera_ocurrencia}' AS INTEGER) AND CAST(
        '{mes_corte}' AS INTEGER
    )

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;

SELECT * FROM base_cedido  -- noqa:

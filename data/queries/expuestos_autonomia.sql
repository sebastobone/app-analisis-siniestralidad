CREATE MULTISET VOLATILE TABLE CANAL_POLIZA
(
	Compania_Id SMALLINT NOT NULL
	,Codigo_Op VARCHAR(100) NOT NULL
	,Ramo_Id INTEGER NOT NULL
	,Codigo_Ramo_Op VARCHAR(100) NOT NULL
	,Poliza_Id BIGINT NOT NULL
	,Numero_Poliza VARCHAR(100) NOT NULL
	,Apertura_Canal_Desc VARCHAR(100) NOT NULL
	,Apertura_Canal_Cd VARCHAR(100) NOT NULL
) PRIMARY INDEX (Numero_Poliza) ON COMMIT PRESERVE ROWS;
INSERT INTO CANAL_POLIZA VALUES (?,?,?,?,?,?,?,?);


CREATE MULTISET VOLATILE TABLE CANAL_CANAL
(
	Compania_Id SMALLINT NOT NULL
	,Codigo_Op VARCHAR(100) NOT NULL
	,Codigo_Ramo_Op VARCHAR(100) NOT NULL
	,Canal_Comercial_Id BIGINT NOT NULL
	,Codigo_Canal_Comercial_Op VARCHAR(20) NOT NULL
	,Nombre_Canal_Comercial VARCHAR(100) NOT NULL
	,Apertura_Canal_Desc VARCHAR(100) NOT NULL
	,Apertura_Canal_Cd VARCHAR(100) NOT NULL
) PRIMARY INDEX (Canal_Comercial_Id) ON COMMIT PRESERVE ROWS;
INSERT INTO CANAL_CANAL VALUES (?,?,?,?,?,?,?,?);


CREATE MULTISET VOLATILE TABLE CANAL_SUCURSAL
(
	Compania_Id SMALLINT NOT NULL
	,Codigo_Op VARCHAR(100) NOT NULL
	,Codigo_Ramo_Op VARCHAR(100) NOT NULL
	,Sucursal_Id BIGINT NOT NULL
	,Codigo_Sucural_Op VARCHAR(10) NOT NULL
	,Nombre_Sucursal VARCHAR(100) NOT NULL
	,Apertura_Canal_Desc VARCHAR(100) NOT NULL
	,Apertura_Canal_Cd VARCHAR(100) NOT NULL
) PRIMARY INDEX (Sucursal_Id) ON COMMIT PRESERVE ROWS;
INSERT INTO CANAL_SUCURSAL VALUES (?,?,?,?,?,?,?,?);


CREATE MULTISET VOLATILE TABLE AMPAROS
(
	Compania_Id SMALLINT NOT NULL
	,Codigo_Op VARCHAR(100) NOT NULL
	,Codigo_Ramo_Op VARCHAR(100) NOT NULL
	,Apertura_Canal_Desc VARCHAR(100) NOT NULL
	,Amparo_Id BIGINT NOT NULL
	,Amparo_Desc VARCHAR(100) NOT NULL
	,Apertura_Amparo_Desc VARCHAR(100) NOT NULL
) PRIMARY INDEX (Amparo_Desc) ON COMMIT PRESERVE ROWS;
INSERT INTO AMPAROS VALUES (?,?,?,?,?,?,?);


CREATE MULTISET VOLATILE TABLE FECHAS AS
(
	SELECT DISTINCT
		Mes_Id
		,MIN(Dia_Dt) OVER (PARTITION BY Mes_Id) AS Primer_dia_mes
		,MAX(Dia_Dt) OVER (PARTITION BY Mes_Id) AS Ultimo_dia_mes
		,Cast((Ultimo_dia_mes - Primer_dia_mes + 1)*1.00 AS DECIMAL(18,0)) Num_dias_mes
		,MIN(Dia_Dt) OVER (PARTITION BY Trimestre_Id) AS Primer_dia_trimestre
		,MAX(Dia_Dt) OVER (PARTITION BY Trimestre_Id) AS Ultimo_dia_trimestre
		,Cast((Ultimo_dia_trimestre - Primer_dia_trimestre + 1)*1.00 AS DECIMAL(18,0)) Num_dias_trimestre
		,MIN(Dia_Dt) OVER (PARTITION BY Ano_Id) AS Primer_dia_anno
		,MAX(Dia_Dt) OVER (PARTITION BY Ano_Id) AS Ultimo_dia_anno
		,Cast((Ultimo_dia_anno - Primer_dia_anno + 1)*1.00 AS DECIMAL(18,0)) Num_dias_anno
	FROM MDB_SEGUROS_COLOMBIA.V_DIA
	WHERE Mes_Id BETWEEN {mes_primera_ocurrencia} AND {mes_corte}
) WITH DATA PRIMARY INDEX (Mes_Id) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON FECHAS COLUMN (Mes_Id);



CREATE MULTISET VOLATILE TABLE BASE_EXPUESTOS
(
	Poliza_Certificado_Id INTEGER NOT NULL
	,Codigo_Ramo_Aux VARCHAR(3) NOT NULL
	,Codigo_Op VARCHAR(2) NOT NULL
	,Apertura_Canal_Aux VARCHAR(100) NOT NULL
	,Apertura_Amparo_Desc VARCHAR(100) NOT NULL
	,Fecha_Cancelacion DATE
	,Fecha_Inclusion_Cobertura DATE
	,Fecha_Exclusion_Cobertura DATE
) PRIMARY INDEX (Poliza_Certificado_Id, Apertura_Amparo_Desc, Fecha_Inclusion_Cobertura, Fecha_Exclusion_Cobertura, Fecha_Cancelacion) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON BASE_EXPUESTOS COLUMN (Poliza_Certificado_Id, Apertura_Amparo_Desc, Fecha_Inclusion_Cobertura, Fecha_Exclusion_Cobertura, Fecha_Cancelacion);

INSERT INTO BASE_EXPUESTOS
SELECT
	pc.Poliza_Certificado_Id
	,CASE
		WHEN ramo.Codigo_Ramo_Op = '081' AND vpc.Amparo_Id NOT IN (18647, 641, 930, 64082, 61296, -1)
		THEN 'AAV'
		ELSE ramo.Codigo_Ramo_Op
	END AS Codigo_Ramo_Aux
	,cia.Codigo_Op
	,COALESCE(p.Apertura_Canal_Desc, c.Apertura_Canal_Desc, s.Apertura_Canal_Desc, 
		CASE 
			WHEN ramo.Codigo_Ramo_Op IN ('081','083') AND cia.Codigo_Op = '02' THEN 'Otros Banca'
			WHEN ramo.Codigo_Ramo_Op IN ('083') AND cia.Codigo_Op = '01' THEN 'Otros'
			ELSE 'Resto'
		END
	) AS Apertura_Canal_Aux
	,COALESCE(amparo.Apertura_Amparo_Desc, 'RESTO') AS Apertura_Amparo_Desc
	,pc.Fecha_Cancelacion
	,MIN(vpc.Fecha_Inclusion_Cobertura) AS Fecha_Inclusion_Cobertura
	,MAX(vpc.Fecha_Exclusion_Cobertura) aS Fecha_Exclusion_Cobertura

FROM MDB_SEGUROS_COLOMBIA.V_HIST_POLCERT_COBERTURA vpc
	LEFT JOIN MDB_SEGUROS_COLOMBIA.V_POLIZA_CERTIFICADO pc ON (vpc.poliza_certificado_id = pc.poliza_certificado_id AND vpc.Plan_Individual_Id = pc.Plan_Individual_Id)
	LEFT JOIN MDB_SEGUROS_COLOMBIA.V_PLAN_INDIVIDUAL plan ON (vpc.Plan_individual_Id = plan.Plan_individual_Id)
	LEFT JOIN MDB_SEGUROS_COLOMBIA.V_PRODUCTO pro ON (plan.Producto_id = pro.Producto_id)
	LEFT JOIN MDB_SEGUROS_COLOMBIA.V_COMPANIA cia ON (pro.Compania_Id = cia.Compania_Id)
	LEFT JOIN MDB_SEGUROS_COLOMBIA.V_RAMO ramo ON (pro.Ramo_Id = ramo.Ramo_Id)
	LEFT JOIN MDB_SEGUROS_COLOMBIA.V_POLIZA poli ON (poli.poliza_id = pc.poliza_id)
	LEFT JOIN MDB_SEGUROS_COLOMBIA.V_AMPARO ampa ON (vpc.Amparo_Id = ampa.Amparo_Id)
	LEFT JOIN MDB_SEGUROS_COLOMBIA.V_SUCURSAL sucu ON (poli.Sucursal_Id = sucu.Sucursal_Id)
	INNER JOIN MDB_SEGUROS_COLOMBIA.V_CANAL_COMERCIAL canal ON (sucu.Canal_Comercial_Id = canal.Canal_Comercial_Id)
	LEFT JOIN CANAL_POLIZA p ON (vpc.Poliza_Id = p.Poliza_Id AND Codigo_Ramo_Aux = p.Codigo_Ramo_Op AND p.Compania_Id = cia.Compania_Id)
	LEFT JOIN (SELECT DISTINCT Compania_Id, Codigo_Ramo_Op, Canal_Comercial_Id, Apertura_Canal_Desc FROM CANAL_CANAL) c
		ON (Codigo_Ramo_Aux = c.Codigo_Ramo_Op
		AND sucu.Canal_Comercial_Id = c.Canal_Comercial_Id
		AND cia.Compania_Id = c.Compania_Id)
	LEFT JOIN (SELECT DISTINCT Compania_Id, Codigo_Ramo_Op, Sucursal_Id, Apertura_Canal_Desc FROM CANAL_SUCURSAL) s
		ON (Codigo_Ramo_Aux = s.Codigo_Ramo_Op
		AND sucu.Sucursal_Id = s.Sucursal_Id
		AND cia.Compania_Id = s.Compania_Id)
	LEFT JOIN (SELECT DISTINCT Compania_Id, Codigo_Ramo_Op, Apertura_Canal_Desc, Amparo_Id, Apertura_Amparo_Desc FROM AMPAROS) amparo
		ON (Codigo_Ramo_Aux = amparo.Codigo_Ramo_Op
		AND vpc.Amparo_Id = amparo.Amparo_Id
		AND Apertura_Canal_Aux = amparo.Apertura_Canal_Desc
		AND cia.Compania_Id = amparo.Compania_Id)
		
WHERE ((pro.Ramo_Id IN (78, 274, 57074, 140, 107, 271, 297, 204) AND pro.Compania_Id = 3)
	OR (pro.Ramo_Id IN (54835, 274, 140, 107) AND pro.Compania_Id = 4))

GROUP BY 1,2,3,4,5,6
HAVING MAX(vpc.Fecha_Exclusion_Cobertura) >= (DATE {fecha_primera_ocurrencia});



CREATE MULTISET VOLATILE TABLE EXPUESTOS AS
(
	SELECT
		Primer_dia_mes
		,Codigo_Op
		,Codigo_Ramo_Op
		,Apertura_Canal_Desc
		,Apertura_Amparo_Desc
		,SUM(Expuestos) AS Expuestos
		,SUM(Vigentes) AS Vigentes
	FROM (
		SELECT
			fechas.Primer_dia_mes
			,vpc.Codigo_Op
			,vpc.Codigo_Ramo_Aux AS Codigo_Ramo_Op
			,vpc.Apertura_Canal_Aux AS Apertura_Canal_Desc
			,vpc.Apertura_Amparo_Desc
			,GREATEST(vpc.Fecha_Inclusion_Cobertura, fechas.Primer_dia_mes) AS Fecha_Inicio
			,LEAST(vpc.Fecha_Exclusion_Cobertura, fechas.Ultimo_dia_mes, COALESCE(vpc.Fecha_Cancelacion, (DATE '3000-01-01'))) AS Fecha_Fin
			,SUM(CAST((Fecha_Fin - Fecha_Inicio + 1) AS DECIMAL(18,6)) / fechas.Num_dias_mes) AS Expuestos
			,SUM(1) AS Vigentes

		FROM (SELECT DISTINCT Primer_dia_mes, Ultimo_dia_mes, Num_dias_mes FROM FECHAS) fechas
			INNER JOIN BASE_EXPUESTOS vpc
				ON (
					vpc.Fecha_Inclusion_Cobertura <= fechas.Ultimo_dia_mes
					AND COALESCE(vpc.Fecha_Cancelacion, (DATE '3000-01-01')) >= fechas.Primer_dia_mes
					AND COALESCE(vpc.Fecha_Exclusion_Cobertura, (DATE '3000-01-01')) >= fechas.Primer_dia_mes
				)

		GROUP BY 1,2,3,4,5,6,7
		) BASE

	GROUP BY 1,2,3,4,5
) WITH DATA PRIMARY INDEX (Primer_dia_mes, Codigo_Ramo_Op, Apertura_Amparo_Desc) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON EXPUESTOS COLUMN (Primer_dia_mes, Codigo_Ramo_Op, Apertura_Amparo_Desc);


SELECT
	CASE
		WHEN base.Codigo_Op = '01'
		THEN CONCAT(TRIM(base.Codigo_Ramo_Op), 'G - ', ramo.Ramo_Desc, ' GENERALES')
		ELSE CONCAT(TRIM(base.Codigo_Ramo_Op), ' - ', CASE WHEN base.Codigo_Ramo_Op = 'AAV' THEN 'ANEXOS VI' ELSE ramo.Ramo_Desc END)
	END AS Ramo_Desc
	,COALESCE(base.Apertura_Canal_Desc, '-1') AS Apertura_Canal_Desc
	,COALESCE(base.Apertura_Amparo_Desc, '-1') AS Apertura_Amparo_Desc
	
	,CASE
		WHEN base.Codigo_Ramo_Op IN ('081','AAV','083') AND base.Codigo_Op = '02'
			THEN CONCAT(
				base.Codigo_Ramo_Op, '_',
				agrcanal.Apertura_Canal_Cd, '_',
				base.Apertura_Amparo_Desc
			)
		WHEN base.Codigo_Ramo_Op IN ('083') AND base.Codigo_Op = '01'
			THEN CONCAT(
				CONCAT(TRIM(base.Codigo_Ramo_Op), 'G'), '_',
				agrcanal.Apertura_Canal_Cd, '_',
				base.Apertura_Amparo_Desc
			)
		WHEN base.Codigo_Op = '01' AND base.Codigo_Ramo_Op NOT IN ('083') THEN CONCAT(TRIM(base.Codigo_Ramo_Op), 'G')
		ELSE base.Codigo_Ramo_Op
	END AS Agrupacion_Reservas

	,base.Primer_dia_mes AS Fecha_Registro

	,ZEROIFNULL(SUM(base.Expuestos)) AS Expuestos
	,ZEROIFNULL(SUM(base.Vigentes)) AS Vigentes

FROM EXPUESTOS base
	LEFT JOIN (SELECT DISTINCT Apertura_Canal_Desc, Apertura_Canal_Cd FROM CANAL_CANAL UNION SELECT DISTINCT Apertura_Canal_Desc, Apertura_Canal_Cd FROM CANAL_SUCURSAL UNION SELECT DISTINCT Apertura_Canal_Desc, Apertura_Canal_Cd FROM CANAL_POLIZA) agrcanal ON (base.Apertura_Canal_Desc = agrcanal.Apertura_Canal_Desc)
	LEFT JOIN MDB_SEGUROS_COLOMBIA.V_RAMO ramo ON (base.Codigo_Ramo_Op = ramo.Codigo_Ramo_Op)

GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5

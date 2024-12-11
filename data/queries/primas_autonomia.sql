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


CREATE MULTISET VOLATILE TABLE FECHAS AS
(
	SELECT DISTINCT
		Mes_Id
		,MIN(Dia_Dt) OVER (PARTITION BY Mes_Id) AS Primer_dia_mes
		,MAX(Dia_Dt) OVER (PARTITION BY Mes_Id) AS Ultimo_dia_mes
		,CAST((Ultimo_dia_mes - Primer_dia_mes + 1)*1.00 AS DECIMAL(18,0)) Num_dias_mes
	FROM MDB_SEGUROS_COLOMBIA.V_DIA
	WHERE Mes_Id BETWEEN {mes_primera_ocurrencia} AND {mes_corte}
) WITH DATA PRIMARY INDEX (Mes_Id) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE MESES_DEVENGUE AS
(
	SELECT DISTINCT
		Mes_Id
		,MIN(Dia_Dt) OVER (PARTITION BY Mes_Id) AS Primer_Dia_Mes
		,MAX(Dia_Dt) OVER (PARTITION BY Mes_Id) AS Ultimo_Dia_Mes
		,Ultimo_Dia_Mes - Primer_Dia_Mes + 1 AS Num_Dias_Mes
	FROM MDB_SEGUROS_COLOMBIA.V_DIA
	WHERE Mes_Id BETWEEN {mes_corte} - 300 AND {mes_corte} + 200
) WITH DATA PRIMARY INDEX (Mes_Id) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE PRIMAS_CEDIDAS_RPND_SAP
(
	Codigo_Op VARCHAR(2) NOT NULL
	,Codigo_Ramo_Op VARCHAR(3) NOT NULL
	,Mes_Id INTEGER NOT NULL
	,Prima_Bruta FLOAT NOT NULL
	,Prima_Retenida FLOAT NOT NULL
	,Prima_Bruta_Devengada FLOAT NOT NULL
	,Prima_Retenida_Devengada FLOAT NOT NULL
	,Mov_RPND_Bruto FLOAT NOT NULL
	,Mov_RPND_Cedido FLOAT NOT NULL
	,Mov_RPND_Retenido FLOAT NOT NULL
	,Prima_Cedida FLOAT NOT NULL
	,Prima_Devengada_Cedida FLOAT NOT NULL
) PRIMARY INDEX (Codigo_Op, Codigo_Ramo_Op, Mes_Id) ON COMMIT PRESERVE ROWS;
INSERT INTO PRIMAS_CEDIDAS_RPND_SAP VALUES (?,?,?,?,?,?,?,?,?,?,?,?);


CREATE MULTISET VOLATILE TABLE GASTOS_EXPEDICION
(
	Codigo_Op VARCHAR(2) NOT NULL
	,Codigo_Ramo_Op VARCHAR(3) NOT NULL
	,Ano_Id INTEGER NOT NULL
	,Porcentaje_Gastos FLOAT NOT NULL
) PRIMARY INDEX (Codigo_Op, Codigo_Ramo_Op, Ano_Id) ON COMMIT PRESERVE ROWS;
INSERT INTO GASTOS_EXPEDICION VALUES (?,?,?,?);



CREATE MULTISET VOLATILE TABLE PRIMAS_RTDC
(
	Mes_Id INTEGER NOT NULL
	,Codigo_Op VARCHAR(2) NOT NULL
	,Codigo_Ramo_Op VARCHAR(3) NOT NULL
	,Apertura_Canal_Desc VARCHAR(100) NOT NULL
	,Tipo_Produccion VARCHAR(20) NOT NULL
	,Prima_Bruta FLOAT NOT NULL
	,Prima_Bruta_Devengada FLOAT NOT NULL
	,Prima_Retenida FLOAT NOT NULL
	,Prima_Retenida_Devengada FLOAT NOT NULL
) PRIMARY INDEX (Mes_Id, Codigo_Op, Codigo_Ramo_Op, Apertura_Canal_Desc, Tipo_Produccion) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON PRIMAS_RTDC COLUMN (Mes_Id, Codigo_Op, Codigo_Ramo_Op, Apertura_Canal_Desc, Tipo_Produccion);

INSERT INTO PRIMAS_RTDC
SELECT
	Mes_Id
	,Codigo_Op
	,Codigo_Ramo_Aux AS Codigo_Ramo_Op
	,Apertura_Canal_Aux AS Apertura_Canal_Desc
	,Tipo_Produccion
	,SUM(Prima_Bruta) AS Prima_Bruta
	,SUM(Prima_Bruta_Devengada) AS Prima_Bruta_Devengada
	,SUM(Prima_Retenida) AS Prima_Retenida
	,SUM(Prima_Retenida_Devengada) AS Prima_Retenida_Devengada

FROM (
	SELECT 
		fechas.Mes_Id
		,cia.Codigo_Op
		,CASE
			WHEN ramo.Ramo_Desc = 'VIDA INDIVIDUAL' AND rtdc.Amparo_Id NOT IN (18647, 641, 930, 64082, 61296, -1)
			THEN 'AAV'
			ELSE ramo.Codigo_Ramo_Op
		END AS Codigo_Ramo_Aux
		,COALESCE(p.Apertura_Canal_Desc, c.Apertura_Canal_Desc, s.Apertura_Canal_Desc, 
			CASE 
				WHEN ramo.Codigo_Ramo_Op IN ('081','083') AND cia.Codigo_Op = '02' THEN 'Otros Banca'
				WHEN ramo.Codigo_Ramo_Op IN ('083') AND cia.Codigo_Op = '01' THEN 'Otros'
				ELSE 'Resto'
			END
		) AS Apertura_Canal_Aux
		,CASE WHEN n1.Nivel_Indicador_Uno_Id = 1 AND rtdc.Valor_Indicador < 0 THEN 'NEGATIVA' ELSE 'POSITIVA' END AS Tipo_Produccion
		,SUM(CASE WHEN n1.Nivel_Indicador_Uno_Id IN (1) THEN rtdc.Valor_Indicador ELSE 0 END) Prima_Bruta
		,SUM(CASE WHEN n1.Nivel_Indicador_Uno_Id IN (1,5) THEN rtdc.Valor_Indicador ELSE 0 END) Prima_Bruta_Devengada
		,SUM(CASE WHEN n1.Nivel_Indicador_Uno_Id IN (1,2) THEN rtdc.Valor_Indicador ELSE 0 END) Prima_Retenida
		,SUM(CASE WHEN n1.Nivel_Indicador_Uno_Id IN (1,2,5) THEN rtdc.Valor_Indicador ELSE 0 END) Prima_Retenida_Devengada

FROM MDB_SEGUROS_COLOMBIA.V_RT_DETALLE_COBERTURA rtdc
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_RT_NIVEL_INDICADOR_CINCO n5 ON (rtdc.Nivel_Indicador_cinco_Id = n5.Nivel_Indicador_cinco_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_RT_Nivel_Indicador_uno n1 ON (n5.Nivel_Indicador_uno_Id = n1.Nivel_Indicador_uno_Id AND n5.compania_origen_Id = n1.compania_origen_Id )
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_PLAN_INDIVIDUAL plan ON (rtdc.Plan_Individual_Id = plan.Plan_Individual_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_PRODUCTO pro ON (pro.producto_Id = plan.producto_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_RAMO ramo ON (pro.Ramo_Id = ramo.Ramo_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_POLIZA poli ON (poli.poliza_Id = rtdc.poliza_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_COMPANIA cia ON (pro.compania_Id = cia.compania_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_SUCURSAL sucu ON (poli.Sucursal_Id = sucu.Sucursal_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_CANAL_COMERCIAL canal ON (sucu.Canal_Comercial_Id = canal.Canal_Comercial_Id)
		LEFT JOIN CANAL_POLIZA p ON (rtdc.Poliza_Id = p.Poliza_Id AND Codigo_Ramo_Aux = p.Codigo_Ramo_Op AND p.Compania_Id = cia.Compania_Id)
		LEFT JOIN (SELECT DISTINCT Compania_Id, Codigo_Ramo_Op, Canal_Comercial_Id, Apertura_Canal_Desc FROM CANAL_CANAL) c
			ON (Codigo_Ramo_Aux = c.Codigo_Ramo_Op
			AND sucu.Canal_Comercial_Id = c.Canal_Comercial_Id
			AND cia.Compania_Id = c.Compania_Id)
		LEFT JOIN (SELECT DISTINCT Compania_Id, Codigo_Ramo_Op, Sucursal_Id, Apertura_Canal_Desc FROM CANAL_SUCURSAL) s
			ON (Codigo_Ramo_Aux = s.Codigo_Ramo_Op
			AND sucu.Sucursal_Id = s.Sucursal_Id
			AND cia.Compania_Id = s.Compania_Id)
		INNER JOIN FECHAS ON (fechas.Mes_Id = rtdc.Mes_Id)

	WHERE ((rtdc.Ramo_Id IN (78, 274, 57074, 140, 107, 271, 297, 204) AND pro.Compania_Id = 3)
		OR (rtdc.Ramo_Id IN (54835, 274, 140, 107) AND pro.Compania_Id = 4))
		AND n1.Nivel_Indicador_Uno_Id iN (1,2,5)

	GROUP BY 1,2,3,4,5

	UNION ALL

	SELECT 
		fechas.Mes_Id
		,CASE WHEN ramo.Codigo_Ramo_Op < '069' THEN '01' ELSE '02' END AS Codigo_Op
		,CASE
			WHEN ramo.Ramo_Desc = 'VIDA INDIVIDUAL' AND rtdc.Amparo_Id NOT IN (18647, 641, 930, 64082, 61296, -1)
			THEN 'AAV'
			ELSE ramo.Codigo_Ramo_Op
		END AS Codigo_Ramo_Aux
		,COALESCE(p.Apertura_Canal_Desc, c.Apertura_Canal_Desc, s.Apertura_Canal_Desc, 
			CASE 
				WHEN ramo.Codigo_Ramo_Op IN ('081','083') AND cia.Codigo_Op = '02' THEN 'Otros Banca'
				WHEN ramo.Codigo_Ramo_Op IN ('083') AND cia.Codigo_Op = '01' THEN 'Otros'
				ELSE 'Resto'
			END
		) AS Apertura_Canal_Aux
		,CASE WHEN n1.Nivel_Indicador_Uno_Id = 1 AND rtdc.Valor_Indicador < 0 THEN 'NEGATIVA' ELSE 'POSITIVA' END AS Tipo_Produccion
		,SUM(CASE WHEN n1.Nivel_Indicador_Uno_Id IN (1) THEN rtdc.Valor_Indicador ELSE 0 END) Prima_Bruta
		,SUM(CASE WHEN n1.Nivel_Indicador_Uno_Id IN (1,5) THEN rtdc.Valor_Indicador ELSE 0 END) Prima_Bruta_Devengada
		,SUM(CASE WHEN n1.Nivel_Indicador_Uno_Id IN (1,2) THEN rtdc.Valor_Indicador ELSE 0 END) Prima_Retenida
		,SUM(CASE WHEN n1.Nivel_Indicador_Uno_Id IN (1,2,5) THEN rtdc.Valor_Indicador ELSE 0 END) Prima_Retenida_Devengada

	FROM MDB_SEGUROS_COLOMBIA.V_RT_RAMO_SUCURSAL rtdc
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_RT_NIVEL_INDICADOR_CINCO n5 ON (rtdc.Nivel_Indicador_cinco_Id = n5.Nivel_Indicador_cinco_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_RT_Nivel_Indicador_uno n1 ON (n5.Nivel_Indicador_uno_Id = n1.Nivel_Indicador_uno_Id AND n5.compania_origen_Id = n1.compania_origen_Id ) 
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_RAMO ramo ON (rtdc.Ramo_Id = ramo.Ramo_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_POLIZA poli ON (rtdc.Poliza_Id = poli.Poliza_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_SUCURSAL sucu ON (rtdc.Sucursal_Id = sucu.Sucursal_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_CANAL_COMERCIAL canal ON (sucu.Canal_Comercial_Id = canal.Canal_Comercial_Id)
		LEFT JOIN MDB_SEGUROS_COLOMBIA.V_COMPANIA cia ON (rtdc.Compania_Origen_Id = cia.compania_Id)
		LEFT JOIN CANAL_POLIZA p ON (rtdc.Poliza_Id = p.Poliza_Id AND Codigo_Ramo_Aux = p.Codigo_Ramo_Op AND p.Compania_Id = cia.Compania_Id)
		LEFT JOIN (SELECT DISTINCT Compania_Id, Codigo_Ramo_Op, Canal_Comercial_Id, Apertura_Canal_Desc FROM CANAL_CANAL) c
			ON (Codigo_Ramo_Aux = c.Codigo_Ramo_Op
			AND sucu.Canal_Comercial_Id = c.Canal_Comercial_Id
			AND cia.Compania_Id = c.Compania_Id)
		LEFT JOIN (SELECT DISTINCT Compania_Id, Codigo_Ramo_Op, Sucursal_Id, Apertura_Canal_Desc FROM CANAL_SUCURSAL) s
			ON (Codigo_Ramo_Aux = s.Codigo_Ramo_Op
			AND rtdc.Sucursal_Id = s.Sucursal_Id
			AND cia.Compania_Id = s.Compania_Id)
		INNER JOIN FECHAS ON (fechas.Mes_Id = rtdc.Mes_Id)

	WHERE ((rtdc.Ramo_Id IN (78, 274, 57074, 140, 107, 271, 297, 204) AND rtdc.Compania_Origen_Id = 3)
		OR (rtdc.Ramo_Id IN (54835, 274, 140, 107) AND rtdc.Compania_Origen_Id = 4))
		AND n1.Nivel_Indicador_Uno_Id iN (1,2,5)

	GROUP BY 1,2,3,4,5
) base

GROUP BY 1,2,3,4,5;



CREATE MULTISET VOLATILE TABLE PRIMAS_EVPRO AS
(
	SELECT
		evpro_cob.Mes_Id
		,cia.Codigo_Op
		,CASE
			WHEN ramo.Ramo_Desc = 'VIDA INDIVIDUAL' AND evpro_cob.Amparo_Id NOT IN (18647, 641, 930, 64082, 61296, -1)
			THEN 'AAV'
			ELSE ramo.Codigo_Ramo_Op
		END AS Codigo_Ramo_Aux
		,COALESCE(p.Apertura_Canal_Desc, c.Apertura_Canal_Desc, s.Apertura_Canal_Desc, 
			CASE 
				WHEN ramo.Codigo_Ramo_Op IN ('081','083') AND cia.Codigo_Op = '02' THEN 'Otros Banca'
				WHEN ramo.Codigo_Ramo_Op IN ('083') AND cia.Codigo_Op = '01' THEN 'Otros'
				ELSE 'Resto'
			END
		) AS Apertura_Canal_Desc
		,CASE WHEN evpro_cob.Valor_Prima < 0 THEN 'NEGATIVA' ELSE 'POSITIVA' END AS Tipo_Produccion
		,SUM(CAST((evpro_cob.Valor_Prima * evpro_cob.Valor_Tasa) AS DECIMAL(18,6))) Prima_Bruta
		
	FROM MDB_SEGUROS_COLOMBIA.V_EVENTO_PROD_COBERTURA evpro_cob
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_POLIZA poli ON (evpro_cob.Poliza_Id = poli.Poliza_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_POLIZA_CERTIFICADO_ETL pcetl ON (evpro_cob.Poliza_Certificado_Id = pcetl.Poliza_Certificado_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_PLAN_INDIVIDUAL pind ON (evpro_cob.Plan_Individual_Id = pind.Plan_Individual_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_PRODUCTO pro ON (pro.Producto_Id = pind.Producto_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_RAMO ramo ON (pro.Ramo_Id = ramo.Ramo_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_COMPANIA cia ON (pro.Compania_Id = cia.Compania_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_SUCURSAL sucu ON (poli.Sucursal_Id = sucu.Sucursal_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_CANAL_COMERCIAL canal ON (sucu.Canal_Comercial_Id = canal.Canal_Comercial_Id)
		LEFT JOIN CANAL_POLIZA p ON (evpro_cob.Poliza_Id = p.Poliza_Id AND Codigo_Ramo_Aux = p.Codigo_Ramo_Op AND p.Compania_Id = cia.Compania_Id)
		LEFT JOIN (SELECT DISTINCT Compania_Id, Codigo_Ramo_Op, Canal_Comercial_Id, Apertura_Canal_Desc FROM CANAL_CANAL) c
			ON (Codigo_Ramo_Aux = c.Codigo_Ramo_Op
			AND sucu.Canal_Comercial_Id = c.Canal_Comercial_Id
			AND cia.Compania_Id = c.Compania_Id)
		LEFT JOIN (SELECT DISTINCT Compania_Id, Codigo_Ramo_Op, Sucursal_Id, Apertura_Canal_Desc FROM CANAL_SUCURSAL) s
			ON (Codigo_Ramo_Aux = s.Codigo_Ramo_Op
			AND sucu.Sucursal_Id = s.Sucursal_Id
			AND cia.Compania_Id = s.Compania_Id)
		
	WHERE ((pro.Ramo_Id IN (78, 274, 57074, 140, 107, 271, 297, 204) AND pro.Compania_Id = 3)
		OR (pro.Ramo_Id IN (54835, 274, 140, 107) AND pro.Compania_Id = 4))
		AND evpro_cob.Mes_Id = {mes_corte}
		
	GROUP BY 1,2,3,4,5

) WITH DATA PRIMARY INDEX (Mes_Id, Codigo_Op, Codigo_Ramo_Aux, Apertura_Canal_Desc, Tipo_Produccion) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE BASE_PRIMAS AS
(
	SELECT
		COALESCE(rtdc.Mes_Id, evpro.Mes_Id) AS Mes_Id
		,COALESCE(rtdc.Codigo_Op, evpro.Codigo_Op) AS Codigo_Op
		,COALESCE(rtdc.Codigo_Ramo_Op, evpro.Codigo_Ramo_Aux) AS Codigo_Ramo_Op
		,COALESCE(rtdc.Apertura_Canal_Desc, evpro.Apertura_Canal_Desc) AS Apertura_Canal_Desc
		,COALESCE(rtdc.Tipo_Produccion, evpro.Tipo_Produccion) AS Tipo_Produccion
		,SUM(CASE WHEN COALESCE(rtdc.Mes_Id, evpro.Mes_Id) = {mes_corte} AND CURRENT_DATE <= LAST_DAY((DATE {fecha_mes_corte})) + INTERVAL '15' DAY THEN evpro.Prima_Bruta ELSE rtdc.Prima_Bruta END) AS Prima_Bruta
		,SUM(CASE WHEN COALESCE(rtdc.Mes_Id, evpro.Mes_Id) = {mes_corte} AND CURRENT_DATE <= LAST_DAY((DATE {fecha_mes_corte})) + INTERVAL '15' DAY THEN evpro.Prima_Bruta - ZEROIFNULL(sap.Prima_Cedida) ELSE rtdc.Prima_Retenida END) AS Prima_Retenida
		,SUM(rtdc.Prima_Bruta_Devengada) AS Prima_Bruta_Devengada
		,SUM(rtdc.Prima_Retenida_Devengada) AS Prima_Retenida_Devengada

	FROM PRIMAS_RTDC rtdc
		FULL OUTER JOIN PRIMAS_EVPRO evpro
			ON (rtdc.Mes_Id = evpro.Mes_Id
			AND rtdc.Codigo_Op = evpro.Codigo_Op
			AND rtdc.Codigo_Ramo_Op = evpro.Codigo_Ramo_Aux
			AND rtdc.Apertura_Canal_Desc = evpro.Apertura_Canal_Desc
			AND rtdc.Tipo_Produccion = evpro.Tipo_Produccion)
		LEFT JOIN (
			SELECT 
				Mes_Id
				,Codigo_Op
				,Codigo_Ramo_Op
				,CASE
					WHEN Codigo_Op = '02' AND Codigo_Ramo_Op IN ('081','AAV','083') THEN 'No Banca'
					ELSE 'Resto'
				END AS Apertura_Canal_Desc
				,'POSITIVA' AS Tipo_Produccion
				,Prima_Cedida
			FROM PRIMAS_CEDIDAS_RPND_SAP
			) sap
			ON (sap.Mes_Id = COALESCE(rtdc.Mes_Id, evpro.Mes_Id)
			AND sap.Codigo_Op = COALESCE(rtdc.Codigo_Op, evpro.Codigo_Op)
			AND sap.Codigo_Ramo_Op = COALESCE(rtdc.Codigo_Ramo_Op, evpro.Codigo_Ramo_Aux)
			AND sap.Apertura_Canal_Desc = COALESCE(rtdc.Apertura_Canal_Desc, evpro.Apertura_Canal_Desc)
			AND sap.Tipo_Produccion = COALESCE(rtdc.Tipo_Produccion, evpro.Tipo_Produccion))

	GROUP BY 1,2,3,4,5

) WITH DATA PRIMARY INDEX (Mes_Id, Codigo_Op, Codigo_Ramo_Op, Apertura_Canal_Desc, Tipo_Produccion) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE BASE_PERFILES_DEVENGUE AS
(
	SELECT
		evpro.Mes_Id
		,cia.Codigo_Op
		,CASE WHEN ramo.Ramo_Desc = 'VIDA INDIVIDUAL' THEN 'AAV' ELSE ramo.Codigo_Ramo_Op END AS Codigo_Ramo_Aux
		,COALESCE(p.Apertura_Canal_Desc, c.Apertura_Canal_Desc, s.Apertura_Canal_Desc, 
			CASE 
				WHEN ramo.Codigo_Ramo_Op IN ('081','083') AND cia.Codigo_Op = '02' THEN 'Otros Banca'
				WHEN ramo.Codigo_Ramo_Op IN ('083') AND cia.Codigo_Op = '01' THEN 'Otros'
				ELSE 'Resto'
			END
		) AS Apertura_Canal_Desc

		,CASE
			WHEN evpro.Fecha_Fin_Vigencia_Dcto < evpro.Fecha_Registro THEN 'MV'
			WHEN evpro.Fecha_Fin_Vigencia_Dcto - evpro.Fecha_Inicio_Vigencia_Dcto + 1 <= 32 THEN 'PC'
			ELSE 'Anualizada'
		END Tipo_Vigencia
		,evpro.Fecha_Fin_Vigencia_Dcto - evpro.Fecha_Inicio_Vigencia_Dcto + 1 AS Dias_Vigencia
		,evpro.Fecha_Fin_Vigencia_Dcto - evpro.Fecha_Registro + 1 AS Dias_Constitucion
		,EXTRACT(DAY FROM
			COALESCE(
				evpro.Fecha_Registro,
				CAST(TO_CHAR(evpro.Ano_Id)||'/'||SUBSTR(evpro.Mes_Id, 5, 2)||'/'||EXTRACT(DAY FROM pcetl.Fecha_Inicio_Ultima_Vigencia) AS DATE)
			) 
		) AS Dia_Contabilizacion

		,CASE WHEN evpro.Valor_Prima < 0 THEN 'NEGATIVA' ELSE 'POSITIVA' END AS Tipo_Produccion
		,SUM(CAST((evpro.Valor_Prima * evpro.Valor_Tasa) AS DECIMAL(18,6)) - COALESCE(evpro_cob.Valor_Pdn, 0)) Prima_Bruta_Devengable
		
	FROM MDB_SEGUROS_COLOMBIA.V_EVENTO_PRODUCCION evpro
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_POLIZA poli ON (evpro.Poliza_Id = poli.Poliza_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_POLIZA_CERTIFICADO_ETL pcetl ON (evpro.Poliza_Certificado_Id = pcetl.Poliza_Certificado_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_PLAN_INDIVIDUAL pind ON (evpro.Plan_Individual_Id = pind.Plan_Individual_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_PRODUCTO pro ON (pro.Producto_Id = pind.Producto_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_RAMO ramo ON (pro.Ramo_Id = ramo.Ramo_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_COMPANIA cia ON (pro.Compania_Id = cia.Compania_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_SUCURSAL sucu ON (poli.Sucursal_Id = sucu.Sucursal_Id)
		INNER JOIN MDB_SEGUROS_COLOMBIA.V_CANAL_COMERCIAL canal ON (sucu.Canal_Comercial_Id = canal.Canal_Comercial_Id)

		LEFT JOIN ( -- Subquery que filtra los movimientos de amparos excluidos de reserva
			SELECT
				Evento_Id
				,CAST(((SUM(evpro_cob.valor_prima * evpro_cob.valor_tasa))) AS DECIMAL(18,6)) Valor_Pdn

			FROM MDB_SEGUROS_COLOMBIA.V_EVENTO_PROD_COBERTURA evpro_cob
				INNER JOIN MDB_SEGUROS_COLOMBIA.V_PLAN_INDIVIDUAL pind ON (evpro_cob.Plan_Individual_Id = pind.Plan_Individual_Id)
				INNER JOIN MDB_SEGUROS_COLOMBIA.V_PRODUCTO pro ON (pro.Producto_Id = pind.Producto_Id)
				INNER JOIN MDB_SEGUROS_COLOMBIA.V_RAMO ramo ON (pro.Ramo_Id = ramo.Ramo_Id)
			
			WHERE evpro_cob.Amparo_Id IN (63717,18647,64082,641,930,-1)
				AND ramo.Codigo_Ramo_Op = '081'
			
			GROUP BY 1
			) evpro_cob ON (evpro_cob.Evento_Id = evpro.Evento_Id)

		LEFT JOIN CANAL_POLIZA p ON (evpro.Poliza_Id = p.Poliza_Id AND Codigo_Ramo_Aux = p.Codigo_Ramo_Op AND p.Compania_Id = cia.Compania_Id)
		LEFT JOIN (SELECT DISTINCT Compania_Id, Codigo_Ramo_Op, Canal_Comercial_Id, Apertura_Canal_Desc FROM CANAL_CANAL) c
			ON (Codigo_Ramo_Aux = c.Codigo_Ramo_Op
			AND sucu.Canal_Comercial_Id = c.Canal_Comercial_Id
			AND cia.Compania_Id = c.Compania_Id)
		LEFT JOIN (SELECT DISTINCT Compania_Id, Codigo_Ramo_Op, Sucursal_Id, Apertura_Canal_Desc FROM CANAL_SUCURSAL) s
			ON (Codigo_Ramo_Aux = s.Codigo_Ramo_Op
			AND sucu.Sucursal_Id = s.Sucursal_Id
			AND cia.Compania_Id = s.Compania_Id)

	WHERE ((pro.Ramo_Id IN (78, 274, 57074, 140, 107, 271, 297, 204) AND pro.Compania_Id = 3)
		OR (pro.Ramo_Id IN (54835, 274, 140, 107) AND pro.Compania_Id = 4))
		AND evpro.Mes_Id BETWEEN {mes_corte} - 200 AND {mes_corte}
		
	GROUP BY 1,2,3,4,5,6,7,8,9
	HAVING Prima_Bruta_Devengable <> 0

) WITH DATA PRIMARY INDEX (Mes_Id, Codigo_Op, Codigo_Ramo_Aux, Apertura_Canal_Desc, Tipo_Produccion) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE PERFILES_DEVENGUE AS
(
	SELECT
		perfil2.*
		,SUM(Prima_Bruta_Devengable) OVER (PARTITION BY Mes_Id, Codigo_Op, Codigo_Ramo_Op, Apertura_Canal_Desc, Tipo_Produccion) AS Total_Pdn
		,CAST(CAST(perfil2.Prima_Bruta_Devengable AS FLOAT) / CASE WHEN Total_Pdn = 0 THEN 1E9 ELSE Total_Pdn END AS DECIMAL(18,6)) AS Porcentaje_Produccion
		
	FROM (
		SELECT
			Apertura_Canal_Desc
			,Codigo_Ramo_Aux AS Codigo_Ramo_Op
			,Codigo_Op
			,Mes_Id
			,Tipo_Vigencia
			,Tipo_Produccion
			,SUM(Dia_Contabilizacion * Peso) AS Dia_Contabilizacion
			,SUM(Dias_Vigencia * Peso) AS Dias_Vigencia
			,SUM(Dias_Constitucion * Peso) AS Dias_Constitucion
			,SUM(Prima_Bruta_Devengable) AS Prima_Bruta_Devengable

		FROM (
			SELECT
				base.*
				,SUM(Prima_Bruta_Devengable) OVER (PARTITION BY Mes_Id, Codigo_Op, Codigo_Ramo_Aux, Apertura_Canal_Desc, Tipo_Vigencia, Tipo_Produccion) AS Total_Pdn
				,CAST(CAST(Prima_Bruta_Devengable AS FLOAT) / CASE WHEN Total_Pdn = 0 THEN 1E9 ELSE Total_Pdn END AS DECIMAL(18,6)) AS Peso
			FROM BASE_PERFILES_DEVENGUE base
			) perfil1
			
		GROUP BY 1,2,3,4,5,6
		) perfil2
	
) WITH DATA PRIMARY INDEX (Mes_Id, Codigo_Op, Codigo_Ramo_Op, Apertura_Canal_Desc, Tipo_Produccion) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE INPUT_DEVENGUE AS
(
	SELECT
		perf.Apertura_Canal_Desc
		,perf.Codigo_Ramo_Op
		,perf.Codigo_Op
		,perf.Mes_Id
		,perf.Tipo_Vigencia
		,perf.Tipo_Produccion
		,ROUND(perf.Dias_Vigencia) Dias_Vigencia
		,ROUND(perf.Dias_Constitucion) Dias_Constitucion
		,ROUND(perf.Dia_Contabilizacion) Dia_Contabilizacion
		,pdn_real.Prima_Bruta * perf.Porcentaje_Produccion AS Prima_Bruta_Devengable
		,pdn_real.Prima_Retenida * perf.Porcentaje_Produccion AS Prima_Retenida_Devengable

	FROM BASE_PRIMAS pdn_real
		INNER JOIN PERFILES_DEVENGUE perf ON (pdn_real.Apertura_Canal_Desc = perf.Apertura_Canal_Desc)
									AND (pdn_real.Codigo_Ramo_Op = perf.Codigo_Ramo_Op)
									AND (pdn_real.Codigo_Op = perf.Codigo_Op)
									AND (pdn_real.Mes_Id = perf.Mes_Id)
									AND (pdn_real.Tipo_Produccion = perf.Tipo_Produccion)
								
) WITH DATA PRIMARY INDEX (Mes_Id, Codigo_Op, Codigo_Ramo_Op, Apertura_Canal_Desc, Tipo_Produccion) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE DEVENGUE AS
(
	SELECT
		dev.Codigo_Op
		,dev.Codigo_Ramo_Op
		,Apertura_Canal_Desc
		,Mes_Pdn
		,Mes_Id
		,Tipo_Vigencia
		,SUM(Prima_Bruta_Devengable) AS Prima_Bruta_Devengable
		,SUM(Prima_Retenida_Devengable) AS Prima_Retenida_Devengable
		,SUM(Constitucion_Bruta * (1 - gexp.Porcentaje_Gastos)) AS Constitucion_RPND_Bruta
		,SUM(Constitucion_Retenida * (1 - gexp.Porcentaje_Gastos)) AS Constitucion_RPND_Retenida
		,SUM(Liberacion_Bruta * (1 - gexp.Porcentaje_Gastos)) AS Liberacion_RPND_Bruta
		,SUM(Liberacion_Retenida * (1 - gexp.Porcentaje_Gastos)) AS Liberacion_RPND_Retenida
		,Constitucion_RPND_Bruta + Liberacion_RPND_Bruta AS Mov_RPND_Bruto
		,Constitucion_RPND_Retenida + Liberacion_RPND_Retenida AS Mov_RPND_Retenido
		
	FROM (
		SELECT
			base.Codigo_Op
			,base.Codigo_Ramo_Op
			,base.Apertura_Canal_Desc
			,base.Mes_Id AS Mes_Pdn

			,base.Tipo_Vigencia
			,base.Tipo_Produccion
			,base.Dias_Vigencia
			,base.Dias_Constitucion
			,base.Dia_Contabilizacion
			,CAST((base.Mes_Id - 190000) * 100 + 1 AS DATE) + GREATEST(base.Dia_Contabilizacion, 1) - 1 AS Fecha_Contabilizacion
			
			,CASE
				WHEN Tipo_Vigencia = 'PC' AND MOD(base.Mes_Id, 100) = 1 AND Dia_Contabilizacion > 25 THEN LAST_DAY(Fecha_Contabilizacion + 15) -- En esta fecha se estaban creando 3 filas para el riego, no debe ocurrir
				WHEN Tipo_Vigencia = 'PC' AND Tipo_Produccion = 'POSITIVA' AND EXTRACT(MONTH FROM Fecha_Contabilizacion + base.Dias_Constitucion) = EXTRACT(MONTH FROM Fecha_Contabilizacion) THEN LAST_DAY(Fecha_Contabilizacion + 15) -- La pdn positiva siempre debe hacer el 50/50, entonces tiene que tener dos lineas
				ELSE Fecha_Contabilizacion + GREATEST(ROUND(base.Dias_Constitucion), 1) 
			END AS Fecha_Fin_Vigencia
			,Fecha_Fin_Vigencia - GREATEST(ROUND(base.Dias_Vigencia), 1) AS Fecha_Inicio_Vigencia

			,EXTRACT(YEAR FROM Fecha_Inicio_Vigencia) * 100 + EXTRACT(MONTH FROM Fecha_Inicio_Vigencia) AS Mes_Inicio_Vigencia
			,EXTRACT(YEAR FROM Fecha_Fin_Vigencia) * 100 + EXTRACT(MONTH FROM Fecha_Fin_Vigencia) AS Mes_Fin_Vigencia
			
			,base.Prima_Bruta_Devengable
			,base.Prima_Retenida_Devengable
			,base.Prima_Bruta_Devengable / GREATEST(base.Dias_Vigencia, 1) AS Prima_Bruta_Devengable_Diaria
			,base.Prima_Retenida_Devengable / GREATEST(base.Dias_Vigencia, 1) AS Prima_Retenida_Devengable_Diaria

			,riego.Mes_Id
			,riego.Primer_Dia_Mes
			,riego.Ultimo_Dia_Mes
			,CASE 
				WHEN riego.Mes_Id = GREATEST(base.Mes_Id, Mes_Inicio_Vigencia) THEN EXTRACT(DAY FROM LEAST(riego.Ultimo_Dia_Mes, Fecha_Fin_Vigencia)) - EXTRACT(DAY FROM GREATEST(Fecha_Contabilizacion, Fecha_Inicio_Vigencia))
				WHEN riego.Mes_Id = Mes_Fin_Vigencia THEN EXTRACT(DAY FROM Fecha_Fin_Vigencia)
				ELSE riego.Num_Dias_Mes
			END AS Dias_Mes
			
			,CASE
				WHEN base.Mes_Id = riego.Mes_Id AND base.Tipo_Vigencia = 'PC' THEN -base.Prima_Bruta_Devengable * 0.5
				WHEN base.Mes_Id = riego.Mes_Id THEN -Prima_Bruta_Devengable_Diaria * base.Dias_Constitucion 
				ELSE 0 
			END AS Constitucion_Bruta
			
			,CASE
				WHEN riego.Mes_Id < Mes_Inicio_Vigencia THEN 0
				WHEN riego.Mes_Id = Mes_Fin_Vigencia AND base.Tipo_Vigencia = 'PC' THEN base.Prima_Bruta_Devengable * 0.5
				WHEN base.Tipo_Vigencia <> 'PC' THEN Prima_Bruta_Devengable_Diaria * Dias_Mes
				ELSE 0
			END AS Liberacion_Bruta

			,CASE
				WHEN base.Mes_Id = riego.Mes_Id AND base.Tipo_Vigencia = 'PC' THEN -base.Prima_Retenida_Devengable * 0.5
				WHEN base.Mes_Id = riego.Mes_Id THEN -Prima_Retenida_Devengable_Diaria * base.Dias_Constitucion 
				ELSE 0 
			END AS Constitucion_Retenida
			
			,CASE
				WHEN riego.Mes_Id < Mes_Inicio_Vigencia THEN 0
				WHEN riego.Mes_Id = Mes_Fin_Vigencia AND base.Tipo_Vigencia = 'PC' THEN base.Prima_Retenida_Devengable * 0.5
				WHEN base.Tipo_Vigencia <> 'PC' THEN Prima_Retenida_Devengable_Diaria * Dias_Mes
				ELSE 0
			END AS Liberacion_Retenida
			
		FROM INPUT_DEVENGUE base
			INNER JOIN MESES_DEVENGUE riego ON (
				riego.Ultimo_Dia_Mes >= Fecha_Contabilizacion
				AND	riego.Primer_Dia_Mes <= Fecha_Fin_Vigencia
				)

		WHERE base.Tipo_Vigencia <> 'MV'
			AND base.Dias_Constitucion > 0
		
		) dev
	
		INNER JOIN GASTOS_EXPEDICION gexp 
			ON (dev.Codigo_Ramo_Op = gexp.Codigo_Ramo_Op
			AND dev.Codigo_Op = gexp.Codigo_Op
			AND CAST(dev.Mes_Pdn / 100 AS INTEGER) = gexp.Ano_Id)

	GROUP BY 1,2,3,4,5,6

) WITH DATA PRIMARY INDEX (Mes_Id, Codigo_Op, Codigo_Ramo_Op, Apertura_Canal_Desc) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE REPARTICION_DIFS_RPND_SAP AS
(
	SELECT
		pond.Codigo_Op
		,pond.Codigo_Ramo_Op
		,pond.Apertura_Canal_Desc
		,(dev.Mov_RPND_Bruto - sap.Mov_RPND_Bruto) * pond.Peso AS Diferencia_Mov_RPND_Bruto
        ,(dev.Mov_RPND_Retenido - sap.Mov_RPND_Retenido) * pond.Peso AS Diferencia_Mov_RPND_Retenido
        
    FROM (
		SELECT DISTINCT
			Codigo_Op
			,Codigo_Ramo_Aux AS Codigo_Ramo_Op
			,Apertura_Canal_Desc
			,SUM(Prima_Bruta_Devengable) OVER (PARTITION BY Codigo_Op, Codigo_Ramo_Aux, Apertura_Canal_Desc) AS Prima_Bruta_Devengable_Ramo_Canal
			,SUM(Prima_Bruta_Devengable) OVER (PARTITION BY Codigo_Op, Codigo_Ramo_Aux) AS Prima_Bruta_Devengable_Ramo
			,Prima_Bruta_Devengable_Ramo_Canal / Prima_Bruta_Devengable_Ramo AS Peso
		FROM BASE_PERFILES_DEVENGUE
		WHERE Mes_Id = {mes_corte}
			AND Tipo_Vigencia <> 'MV'
		) pond

		INNER JOIN (
			SELECT
				Codigo_Op
				,Codigo_Ramo_Op
				,SUM(Mov_RPND_Bruto) AS Mov_RPND_Bruto
				,SUM(Mov_RPND_Retenido) AS Mov_RPND_Retenido
			FROM DEVENGUE
			WHERE Mes_Id = {mes_corte}
			GROUP BY 1,2
			) dev
			ON (pond.Codigo_Op = dev.Codigo_Op
			AND pond.Codigo_Ramo_Op = dev.Codigo_Ramo_Op)

		INNER JOIN PRIMAS_CEDIDAS_RPND_SAP sap ON (pond.Codigo_Op = sap.Codigo_Op AND pond.Codigo_Ramo_Op = sap.Codigo_Ramo_Op)

) WITH DATA PRIMARY INDEX (Codigo_Op, Codigo_Ramo_Op, Apertura_Canal_Desc) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE PRIMAS_FINAL AS
(
	SELECT
		base.Codigo_Op
		,base.Codigo_Ramo_Op
		,base.Apertura_Canal_Desc
		,base.Mes_Id
		,base.Prima_Bruta
		,base.Prima_Retenida
		,SUM(dev.Mov_RPND_Bruto) OVER (PARTITION BY base.Codigo_Op, base.Codigo_Ramo_Op, base.Apertura_Canal_Desc) AS Mov_RPND_Bruto_Ramo
		,SUM(dev.Mov_RPND_Retenido) OVER (PARTITION BY base.Codigo_Op, base.Codigo_Ramo_Op, base.Apertura_Canal_Desc) AS Mov_RPND_Retenido_Ramo
		,CASE 
			WHEN base.Mes_Id = {mes_corte} AND CURRENT_DATE <= LAST_DAY((DATE {fecha_mes_corte})) + INTERVAL '15' DAY 
			THEN base.Prima_Bruta + ZEROIFNULL(dev.Mov_RPND_Bruto) - ZEROIFNULL(difs.Diferencia_Mov_RPND_Bruto)
			ELSE base.Prima_Bruta_Devengada
		END AS Prima_Bruta_Devengada
		,CASE 
			WHEN base.Mes_Id = {mes_corte} AND CURRENT_DATE <= LAST_DAY((DATE {fecha_mes_corte})) + INTERVAL '15' DAY 
			THEN base.Prima_Retenida + ZEROIFNULL(dev.Mov_RPND_Retenido) - ZEROIFNULL(difs.Diferencia_Mov_RPND_Retenido)
			ELSE base.Prima_Retenida_Devengada
		END AS Prima_Retenida_Devengada
	
	FROM (
		SELECT 
			Codigo_Op
			,Codigo_Ramo_Op
			,Apertura_Canal_Desc
			,Mes_Id
			,SUM(Prima_Bruta) AS Prima_Bruta
			,SUM(Prima_Retenida) AS Prima_Retenida
			,SUM(Prima_Bruta_Devengada) AS Prima_Bruta_Devengada
			,SUM(Prima_Retenida_Devengada) AS Prima_Retenida_Devengada
		FROM BASE_PRIMAS GROUP BY 1,2,3,4
		) base
		LEFT JOIN (
			SELECT 
				Codigo_Op
				,Codigo_Ramo_Op
				,Apertura_Canal_Desc
				,Mes_Id
				,SUM(Mov_RPND_Bruto) AS Mov_RPND_Bruto
				,SUM(Mov_RPND_Retenido) AS Mov_RPND_Retenido
			FROM DEVENGUE GROUP BY 1,2,3,4
			) dev
				ON (base.Apertura_Canal_Desc = dev.Apertura_Canal_Desc)
				AND (base.Codigo_Ramo_Op = dev.Codigo_Ramo_Op)
				AND (base.Codigo_Op = dev.Codigo_Op)
				AND (base.Mes_Id = dev.Mes_Id)
		LEFT JOIN REPARTICION_DIFS_RPND_SAP difs
			ON (base.Apertura_Canal_Desc = difs.Apertura_Canal_Desc)
			AND (base.Codigo_Ramo_Op = difs.Codigo_Ramo_Op)
			AND (base.Codigo_Op = difs.Codigo_Op)

) WITH DATA PRIMARY INDEX (Mes_Id, Codigo_Op, Codigo_Ramo_Op, Apertura_Canal_Desc) ON COMMIT PRESERVE ROWS;



SELECT
	CASE
		WHEN base.Codigo_Op = '01'
		THEN CONCAT(TRIM(base.Codigo_Ramo_Op), 'G - ', ramo.Ramo_Desc, ' GENERALES')
		ELSE CONCAT(TRIM(base.Codigo_Ramo_Op), ' - ', CASE WHEN base.Codigo_Ramo_Op = 'AAV' THEN 'ANEXOS VI' ELSE ramo.Ramo_Desc END)
	END AS Ramo_Desc
	,COALESCE(base.Apertura_Canal_Desc, '-1') AS Apertura_Canal_Desc
	,'NO APLICA' AS Apertura_Amparo_Desc
	
	,CASE
		WHEN base.Codigo_Ramo_Op IN ('081','AAV','083') AND base.Codigo_Op = '02'
			THEN CONCAT(
				base.Codigo_Ramo_Op, '_',
				agrcanal.Apertura_Canal_Cd
			)
		WHEN base.Codigo_Ramo_Op IN ('083') AND base.Codigo_Op = '01'
			THEN CONCAT(
				CONCAT(TRIM(base.Codigo_Ramo_Op), 'G'), '_',
				agrcanal.Apertura_Canal_Cd
			)
		WHEN base.Codigo_Op = '01' AND base.Codigo_Ramo_Op NOT IN ('083') THEN CONCAT(TRIM(base.Codigo_Ramo_Op), 'G')
		ELSE base.Codigo_Ramo_Op
	END AS Agrupacion_Reservas

	,fechas.Primer_dia_mes AS Fecha_Registro

	,ZEROIFNULL(SUM(base.Prima_Bruta)) AS Prima_Bruta
	,ZEROIFNULL(SUM(base.Prima_Bruta_Devengada)) AS Prima_Bruta_Devengada
	,ZEROIFNULL(SUM(base.Prima_Retenida)) AS Prima_Retenida
	,ZEROIFNULL(SUM(base.Prima_Retenida_Devengada)) AS Prima_Retenida_Devengada

FROM PRIMAS_FINAL base
	LEFT JOIN (SELECT DISTINCT Apertura_Canal_Desc, Apertura_Canal_Cd FROM CANAL_CANAL UNION SELECT DISTINCT Apertura_Canal_Desc, Apertura_Canal_Cd FROM CANAL_SUCURSAL UNION SELECT DISTINCT Apertura_Canal_Desc, Apertura_Canal_Cd FROM CANAL_POLIZA) agrcanal ON (base.Apertura_Canal_Desc = agrcanal.Apertura_Canal_Desc)
	LEFT JOIN MDB_SEGUROS_COLOMBIA.V_RAMO ramo ON (base.Codigo_Ramo_Op = ramo.Codigo_Ramo_Op)
	INNER JOIN FECHAS fechas ON (fechas.Mes_Id = base.Mes_Id)

GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5

# Diccionario

## Conceptos de siniestros

- **Pago**: Monto efectivamente desembolsado por la compañía de seguros para cubrir siniestros en un periodo determinado.
- **Aviso**: Provisión constituida para cubrir siniestros ya reportados pero aún no pagados.
- **IBNR (_Incurred But Not Reported_)**: Provisión constituida para siniestros ocurridos pero no reportados, o insuficientemente reportados, al momento del corte.
- **Incurrido**: Suma de **Pago + Aviso**, que refleja el monto total de siniestros conocidos.
- **Ultimate**: Estimación total del costo de los siniestros ocurridos en un periodo específico, sin importar en qué momento se paguen o se reconozcan. Se compone de **Pago + Aviso + IBNR**.

## Conceptos de negocio

- **Expuestos:** Número de personas, pólizas o riesgos asegurados que estuvieron cubiertos en un periodo, susceptibles de presentar un siniestro.
- **Prima:** Ingreso reconocido por la compañía como contraprestación por otorgar cobertura de seguro.
- **Prima devengada:** Es la proporción de la prima que corresponde al periodo de riesgo corrido en la cobertura de cada seguro.

    !!! example "Ejemplo"
        Asumamos una póliza con cobertura del 1 de enero al 31 de diciembre, con una prima de $120. Cuando evalúo mis estados financieros al corte de 30 de junio, la prima devengada es $60, pues ya ha corrido la mitad del riesgo de la cobertura (asumiendo distribución de riesgo uniforme).

- **Porcentaje de siniestralidad:** Relación entre el **Ultimate** y la **Prima devengada**. Indica cuánta fracción de la prima realmente corresponde a los costos de siniestros.

## Conceptos de análisis

- **Frecuencia:** Número de siniestros ocurridos por cada unidad de exposición.
- **Severidad:** Costo promedio por siniestro.
- **Triángulos de desarrollo:** Representaciones tabulares que muestran la evolución de siniestros por periodo de ocurrencia y de desarrollo. Se utilizan para proyectar la siniestralidad futura mediante metodologías actuariales.
- **Ocurrencia**: Periodo en el que ocurren los siniestros. Corresponden a las filas de los triángulos de desarrollo.
- **Altura**: Número de periodos entre la ocurrencia de los siniestros y su respectivo pago o movimiento. Corresponden a las columnas de los triángulos de desarrollo.
- **Entremés:** Análisis simplificado que permite calcular el **Ultimate** para **Ocurrencias** nuevas que todavía no han cumplido la periodicidad suficiente para ser sometidas a **triángulos de desarrollo**.
- **Completar diagonal:** Técnica usada en análisis de **Entremés** para proyectar la porción faltante del desarrollo de una ocurrencia para alcanzar la periodicidad del **triángulo de desarrollo**.

    !!! example "Ejemplo"
        Asumamos una apertura con periodicidad trimestral, que estamos analizando a corte de mayo de 2025. Esta metodología busca responder la siguiente pregunta:

        Cuando estemos en junio de 2025, ¿cuál va a ser la cifra acumulada de pago e incurrido para cada una de las ocurrencias de esta apertura?

# Introducción

El análisis de siniestralidad para el cierre de reservas es un proceso realizado cada mes por cada negocio de la compañía (Salud, ARL, Autonomía, SOAT, Movilidad, EPS, Competitividad) a través de los equipos de Gestión Técnica. Este proceso tiene como objetivo calcular la siniestralidad última esperada en términos de los pagos futuros. Para ello, toma como insumo principal la información histórica de pagos, avisos, primas y expuestos almacenada en las bases de datos de la compañía, específicamente en Teradata.

Para llevar a cabo este análisis, se utiliza principalmente la técnica de desarrollo de triángulos conocida como el método Chain Ladder. Este enfoque permite a los equipos técnicos proyectar los pagos futuros y evaluar la evolución de la siniestralidad, proporcionando una base sólida para la toma de decisiones en la gestión de riesgos y siniestralidad.

En esta documentación se describen la forma, el método y los pasos a seguir para llevar a cabo el proceso de seguimiento de siniestralidad que realiza cada negocio de manera periódica a través de una aplicación de Python.

Adicionalmente, el alcance la herramienta aquí descrita no está limitado al cálculo de los cierres mensuales, sino que está diseñada para realizar todo tipo de análisis de siniestralidad por medio de la metodología Chain-Ladder.

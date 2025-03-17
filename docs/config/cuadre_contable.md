# Cuadre contable

Para los análisis de siniestralidad, las cifras utilizadas de pagos, aviso, y producción emitida y devengada deben ser consistentes con las cifras reportadas en los estados financieros. El umbral máximo de diferencias aceptadas corresponde al 5% de la cifra contable, pero está sujeto a variaciones dependiendo de las características de cada negocio.

Asimismo, cada negocio puede decidir adoptar una estrategia de repartición de diferencias contables, de forma que la cifra usada en el análisis sea exactamente la contable. Esta estrategia consiste en repartir la diferencia en una o varias de las aperturas definidas, las cuales se especifican en las hojas "Cuadre_Siniestros" y "Cuadre_Primas" del archivo `data/segmentacion.xlsx`.

Todos los análisis de consistencia de información se almacenan en dos carpetas: `data/controles_informacion/pre_cuadre_contable` (la información tal cual sale de Teradata) y `data/controles_informacion/post_cuadre_contable` (la información con las diferencias repartidas y la cifra contable conciliada). Si el negocio no realiza un cuadre contable, la segunda carpeta quedará vacía.

# Cuadre contable

En los análisis de siniestralidad, las cifras de pagos, avisos y primas (emitidas y devengadas) deben ser coherentes con las reportadas en los estados financieros. Se permite una diferencia máxima del 5% respecto a la cifra contable, aunque este umbral puede ajustarse según las particularidades de cada negocio.

Cada negocio puede optar por una estrategia de cuadre contable para eliminar por completo las diferencias y garantizar que las cifras analizadas coincidan exactamente con las contables. Esta estrategia consiste en distribuir la diferencia contable entre una o varias aperturas específicas, definidas en las hojas **"Cuadre_Siniestros"** y **"Cuadre_Primas"** del archivo `data/segmentacion_{negocio}.xlsx`.

El usuario tiene la flexibilidad de seleccionar qué meses y qué variables (por ejemplo: pagos, avisos, prima bruta, etc.) desea ajustar a la cifra contable. Esta configuración se realiza en las hojas **"Meses_cuadre_siniestros"** y **"Meses_cuadre_primas"**, donde las celdas correspondientes son binarias:

- Valor **1**: la variable del mes seleccionado se ajustará a la cifra contable.
- Valor **0**: no se aplicará ningún ajuste para esa variable en ese mes.

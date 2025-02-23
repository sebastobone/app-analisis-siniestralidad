import numpy as np
import numpy.typing as npt
import polars as pl

TIPO_MATRICES = npt.NDArray[np.float64]


def construir_triangulo(datos: pl.DataFrame, columna_valor: str) -> pl.DataFrame:
    return datos.sort(["periodo_ocurrencia", "index_desarrollo"]).pivot(
        on="index_desarrollo", index="periodo_ocurrencia", values=columna_valor
    )


def calcular_factores_desarrollo(
    triangulo: pl.DataFrame, inicio_ventana: int, num_periodos_ventana: int
) -> pl.DataFrame:
    valores = triangulo.select(triangulo.collect_schema().names()[1:]).to_numpy()
    factores = calcular_triangulo_factores(valores)
    relacion_alturas_ocurrencias = int(round(triangulo.shape[1] / triangulo.shape[0]))

    promedio_historia, maximo_historia, minimo_historia, promedio_ponderado_historia = (
        calcular_metricas_historia(factores, valores)
    )
    promedio_ventana, maximo_ventana, minimo_ventana, promedio_ponderado_ventana = (
        calcular_metricas_ventana(
            factores,
            valores,
            inicio_ventana,
            num_periodos_ventana,
            relacion_alturas_ocurrencias,
        )
    )

    return (
        pl.DataFrame(
            {
                "promedio_historia": promedio_historia,
                "maximo_historia": maximo_historia,
                "minimo_historia": minimo_historia,
                "promedio_ponderado_historia": promedio_ponderado_historia,
                "promedio_ventana": promedio_ventana,
                "maximo_ventana": maximo_ventana,
                "minimo_ventana": minimo_ventana,
                "promedio_ponderado_ventana": promedio_ponderado_ventana,
            },
            strict=False,
        )
        .with_row_index("altura")
        .with_columns(pl.col("altura").cast(pl.Int32))
    )


def calcular_triangulo_factores(valores: TIPO_MATRICES) -> TIPO_MATRICES:
    factores = valores.copy()
    factores[:, :-1] = factores[:, 1:] / factores[:, :-1]
    factores[factores == np.inf] = np.nan
    return factores


def calcular_metricas_historia(
    factores: TIPO_MATRICES, valores: TIPO_MATRICES
) -> tuple[list[float], list[float], list[float], list[float]]:
    promedio, maximo, minimo, promedio_ponderado = [], [], [], []

    for altura in range(factores.shape[1] - 1):
        factores_altura_no_nulos = factores[:, altura][~np.isnan(factores[:, altura])]
        valores_altura_no_nulos = valores[:, altura][~np.isnan(valores[:, altura + 1])]
        valores_altura_siguiente_no_nulos = valores[:, altura + 1][
            ~np.isnan(valores[:, altura + 1])
        ]

        if factores_altura_no_nulos.size == 0:
            factores_altura_no_nulos = np.array([1])
        if valores_altura_no_nulos.size == 0 or valores_altura_no_nulos.sum() == 0:
            valores_altura_no_nulos = valores_altura_siguiente_no_nulos = np.array([1])

        promedio.append(factores_altura_no_nulos.mean())
        maximo.append(factores_altura_no_nulos.max())
        minimo.append(factores_altura_no_nulos.min())
        promedio_ponderado.append(
            valores_altura_siguiente_no_nulos.sum() / valores_altura_no_nulos.sum()
        )

    return promedio, maximo, minimo, promedio_ponderado


def calcular_metricas_ventana(
    factores: TIPO_MATRICES,
    valores: TIPO_MATRICES,
    inicio_ventana: int,
    num_periodos_ventana: int,
    relacion_alturas_ocurrencias: int,
) -> tuple[list[float], list[float], list[float], list[float]]:
    promedio, maximo, minimo, promedio_ponderado = [], [], [], []

    for altura in range(factores.shape[1] - 1):
        periodo_inicial_ventana = (
            len(factores) - inicio_ventana - (altura // relacion_alturas_ocurrencias)
        )
        periodo_final_ventana = max(periodo_inicial_ventana - num_periodos_ventana, 0)

        factores_altura = factores[
            periodo_final_ventana:periodo_inicial_ventana, altura
        ]
        valores_altura = valores[periodo_final_ventana:periodo_inicial_ventana, altura]
        valores_altura_siguiente = valores[
            periodo_final_ventana:periodo_inicial_ventana, altura + 1
        ]

        factores_altura_no_nulos = factores_altura[~np.isnan(factores_altura)]
        valores_altura_no_nulos = valores_altura[~np.isnan(valores_altura_siguiente)]
        valores_altura_siguiente_no_nulos = valores_altura_siguiente[
            ~np.isnan(valores_altura_siguiente)
        ]

        if factores_altura_no_nulos.size == 0:
            factores_altura_no_nulos = np.array([1])
        if valores_altura_no_nulos.size == 0 or valores_altura_no_nulos.sum() == 0:
            valores_altura_no_nulos = valores_altura_siguiente_no_nulos = np.array([1])

        promedio.append(factores_altura_no_nulos.mean())
        maximo.append(factores_altura_no_nulos.max())
        minimo.append(factores_altura_no_nulos.min())
        promedio_ponderado.append(
            valores_altura_siguiente_no_nulos.sum() / valores_altura_no_nulos.sum()
        )

    return promedio, maximo, minimo, promedio_ponderado


def calcular_factores_acumulados(factores_desarrollo: pl.DataFrame) -> pl.DataFrame:
    nombre_factores = factores_desarrollo.collect_schema().names()[1:]
    return (
        factores_desarrollo.sort("altura", descending=True)
        .with_columns([pl.col(factor).cum_prod() for factor in nombre_factores])
        .sort("altura")
    )

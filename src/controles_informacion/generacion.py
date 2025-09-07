import asyncio
from datetime import date
from pathlib import Path
from typing import Literal

import polars as pl

from src import constantes as ct
from src import utils
from src.controles_informacion import cuadre_contable as cuadre
from src.controles_informacion import evidencias, sap
from src.dependencias import SessionDep
from src.informacion import almacenamiento as alm
from src.logger_config import logger
from src.models import MetadataCantidades, Parametros, SeleccionadosCuadre
from src.validation import cantidades

ARCHIVOS_PERMANENTES = [
    "siniestros.parquet",
    "siniestros.csv",
    "siniestros_teradata.parquet",
    "siniestros_teradata.csv",
    "primas.parquet",
    "primas.csv",
    "primas_teradata.parquet",
    "primas_teradata.csv",
    "expuestos.parquet",
    "expuestos.csv",
    "expuestos_teradata.parquet",
    "expuestos_teradata.csv",
]


async def generar_controles(
    p: Parametros, archivos_cuadre: SeleccionadosCuadre, session: SessionDep
) -> None:
    await verificar_existencia_afos(p.negocio)

    await generar_controles_cantidad("siniestros", p, archivos_cuadre, session)
    await generar_controles_cantidad("primas", p, archivos_cuadre, session)
    await generar_controles_cantidad("expuestos", p, archivos_cuadre, session)

    await evidencias.generar_evidencias_parametros(
        p.negocio, utils.date_to_yyyymm(p.mes_corte)
    )


async def verificar_existencia_afos(negocio: str):
    logger.info("Verificando existencia de AFOS...")
    afos_necesarios = ct.AFOS_NECESARIOS[negocio]
    for afo in afos_necesarios:
        if not Path(f"data/afo/{afo}.xlsx").exists():
            raise FileNotFoundError(
                utils.limpiar_espacios_log(
                    f"""
                    El AFO no se encuentra en la ruta data/afo/{afo}.xlsx,
                    por favor agregarlo en la carpeta.
                    """
                )
            )


async def generar_controles_cantidad(
    cantidad: ct.CANTIDADES,
    p: Parametros,
    archivos_cuadre: SeleccionadosCuadre,
    session: SessionDep,
) -> None:
    lista_archivos_cuadre = archivos_cuadre.model_dump()[cantidad]
    if lista_archivos_cuadre:
        dfs_cuadre = []
        for ruta in lista_archivos_cuadre:
            dfs_cuadre.append(pl.read_parquet(ruta))

        df_pre_cuadre = pl.DataFrame(pl.concat(dfs_cuadre)).pipe(
            cantidades.organizar_archivo,
            p.negocio,
            (p.mes_inicio, p.mes_corte),
            cantidad,
            cantidad,
        )

        alm.guardar_archivo(
            df_pre_cuadre,
            session,
            MetadataCantidades(
                ruta=f"data/pre_cuadre_contable/{cantidad}.parquet",
                nombre_original=f"{cantidad}.parquet",
                origen="pre_cuadre_contable",
                cantidad=cantidad,
                rutas_padres=lista_archivos_cuadre,
            ),
        )

        difs_sap_tera_pre_cuadre = await generar_controles_estado_cuadre(
            df_pre_cuadre,
            p.negocio,
            cantidad,
            p.mes_corte,
            estado_cuadre="pre_cuadre_contable",
        )

        if cantidad in [
            "siniestros",
            "primas",
        ] and cuadre.debe_realizar_cuadre_contable(p.negocio, cantidad):
            meses_a_cuadrar = pl.read_excel(
                f"data/segmentacion_{p.negocio}.xlsx",
                sheet_name=f"Meses_cuadre_{cantidad}",
            )
            df_post_cuadre = await cuadre.realizar_cuadre_contable(
                p.negocio,
                cantidad,
                df_pre_cuadre,
                difs_sap_tera_pre_cuadre,
                meses_a_cuadrar,
            )
            alm.guardar_archivo(
                df_post_cuadre,
                session,
                MetadataCantidades(
                    ruta=f"data/post_cuadre_contable/{cantidad}.parquet",
                    nombre_original=f"{cantidad}.parquet",
                    origen="post_cuadre_contable",
                    cantidad=cantidad,
                    rutas_padres=lista_archivos_cuadre,
                ),
            )

            _ = await generar_controles_estado_cuadre(
                df_post_cuadre,
                p.negocio,
                cantidad,
                p.mes_corte,
                estado_cuadre="post_cuadre_contable",
            )

            # if p.negocio == "soat" and cantidad == "siniestros":
            #     df_post_fraude = await ajustar_fraude(df_post_cuadre, p.mes_corte)
            #     alm.guardar_archivo(
            #         df_post_fraude,
            #         session,
            #         MetadataCantidades(
            #             ruta=f"data/post_ajustes_fraude/{cantidad}.parquet",
            #             nombre_original=f"{cantidad}.parquet",
            #             origen="post_ajustes_fraude",
            #             cantidad=cantidad,
            #         ),
            #     )
            #     _ = await generar_controles_estado_cuadre(
            #         df_post_fraude,
            #         p.negocio,
            #         cantidad,
            #         p.mes_corte,
            #         estado_cuadre="post_ajustes_fraude",
            #     )
    else:
        logger.warning(
            utils.limpiar_espacios_log(
                f"""
                No se seleccionaron archivos para la cantidad {cantidad},
                por lo cual no le generaron controles.
                """
            )
        )


async def generar_controles_estado_cuadre(
    df: pl.DataFrame,
    negocio: str,
    file: ct.CANTIDADES,
    mes_corte: date,
    estado_cuadre: Literal["pre_cuadre_contable", "post_cuadre_contable"],
) -> pl.DataFrame:
    logger.info(
        utils.limpiar_espacios_log(
            f"""
            Generando controles de informacion para {file}
            en el estado {estado_cuadre}...
            """
        )
    )

    mes_corte_int = utils.date_to_yyyymm(mes_corte)

    qtys, group_cols = definir_cantidades_control(negocio, file)
    await asyncio.to_thread(
        agrupar_tera(df, group_cols, qtys).write_excel,
        f"data/controles_informacion/{estado_cuadre}/{file}_tera_{mes_corte_int}.xlsx",
    )
    await asyncio.to_thread(
        generar_consistencia_historica,
        file,
        group_cols,
        qtys,
        estado_cuadre,
        fuente="tera",
    )

    df_tera = agrupar_tera(
        df,
        group_cols=["codigo_op", "codigo_ramo_op", "fecha_registro"],
        qtys=qtys,
    )

    if file in ("siniestros", "primas"):
        df_sap = (await sap.consolidar_sap(negocio, qtys, mes_corte)).filter(
            pl.col("codigo_ramo_op").is_in(df.get_column("codigo_ramo_op").unique())
        )
        await asyncio.to_thread(
            df_sap.write_excel,
            f"data/controles_informacion/{estado_cuadre}/{file}_sap_{mes_corte_int}.xlsx",
        )
        await asyncio.to_thread(
            generar_consistencia_historica,
            file,
            ["codigo_op", "codigo_ramo_op", "fecha_registro"],
            qtys,
            estado_cuadre,
            fuente="sap",
        )

        difs_sap_tera = await comparar_sap_tera(df_tera, df_sap, mes_corte, qtys)
        await asyncio.to_thread(
            difs_sap_tera.write_excel,
            f"data/controles_informacion/{estado_cuadre}/{file}_sap_vs_tera_{mes_corte_int}.xlsx",
        )

    elif file == "expuestos":
        difs_sap_tera = pl.DataFrame()

    await asyncio.to_thread(
        generar_integridad_exactitud, df, estado_cuadre, file, mes_corte_int, qtys
    )

    logger.success(
        utils.limpiar_espacios_log(
            f"""
            Revisiones de informacion y generacion de controles terminada
            para {file} en el estado {estado_cuadre}.
            """
        )
    )

    return difs_sap_tera


def definir_cantidades_control(
    negocio: str,
    file: Literal["siniestros", "primas", "expuestos"],
) -> tuple[list[str], list[str]]:
    if file == "siniestros":
        qtys = ct.COLUMNAS_SINIESTROS_CUADRE
        group_cols = ["apertura_reservas", "fecha_siniestro", "fecha_registro"]

    elif file == "primas":
        qtys = ct.Valores().model_dump()["primas"].keys()
        group_cols = utils.obtener_aperturas(
            negocio, "primas"
        ).collect_schema().names() + ["fecha_registro"]

    elif file == "expuestos":
        qtys = ["expuestos"]
        group_cols = utils.obtener_aperturas(
            negocio, "expuestos"
        ).collect_schema().names() + ["fecha_registro"]

    return qtys, group_cols


async def comparar_sap_tera(
    df_tera: pl.DataFrame,
    df_sap: pl.DataFrame,
    mes_corte: date,
    qtys: list[str],
) -> pl.DataFrame:
    base_comp = (
        df_tera.lazy()
        .join(
            df_sap.lazy(),
            on=["codigo_op", "codigo_ramo_op", "fecha_registro"],
            how="left",
            validate="1:1",
            suffix="_SAP",
        )
        .fill_null(0)
    )

    for qty in qtys:
        base_comp = base_comp.with_columns(
            (pl.col(f"{qty}_SAP") - pl.col(qty)).alias(f"diferencia_{qty}"),
        ).with_columns(
            (pl.col(f"diferencia_{qty}") / pl.col(f"{qty}_SAP"))
            .alias(f"dif%_{qty}")
            .fill_nan(0)
        )

        comp_mes = (
            base_comp.filter(pl.col("fecha_registro") == mes_corte)
            .filter(pl.col(f"dif%_{qty}").abs() > 0.05)
            .collect()
        )

        if comp_mes.shape[0] != 0:
            dif = comp_mes.select(
                [
                    "codigo_op",
                    "codigo_ramo_op",
                    qty,
                    f"{qty}_SAP",
                    f"diferencia_{qty}",
                    f"dif%_{qty}",
                ]
            )
            logger.warning(f"""¡Alerta! Diferencias significativas en {qty}: {dif}""")

    return base_comp.collect()


def generar_consistencia_historica(
    file: str,
    group_cols: list[str],
    qtys: list[str],
    estado_cuadre: str,
    fuente: str,
) -> None:
    available_files = [
        f.name
        for f in Path(f"data/controles_informacion/{estado_cuadre}").iterdir()
        if f"{file}_{fuente}" in f.name
        and "sap_vs_tera" not in f.name
        and "ramo" not in f.name
        and "consistencia" not in f.name
    ]

    dfs = pl.LazyFrame(schema=group_cols)
    meses = set()
    for i, f in enumerate(available_files):
        mes_file = f[-11:-5]
        df = (
            pl.read_excel(f"data/controles_informacion/{estado_cuadre}/{f}")
            .lazy()
            .rename({qty: f"{qty}_{mes_file}" for qty in qtys})
        )

        dfs = (
            df
            if i == 0
            else dfs.join(df, on=group_cols, how="full", coalesce=True).fill_null(0)
        )

        meses.add(mes_file)

    meses_list = list(meses)
    for qty in qtys:
        for n_mes, mes in enumerate(meses_list[1:]):
            dfs = dfs.with_columns(
                (pl.col(f"{qty}_{mes}") - pl.col(f"{qty}_{meses_list[n_mes]}")).alias(
                    f"diferencia_{qty}_{mes}_{meses_list[n_mes]}"
                )
            )

    dfs.sort(group_cols).collect().write_excel(
        f"data/controles_informacion/{estado_cuadre}/{file}_{fuente}_consistencia_historica.xlsx"
    )


def agrupar_tera(
    df: pl.DataFrame, group_cols: list[str], qtys: list[str]
) -> pl.DataFrame:
    return df.select(group_cols + qtys).group_by(group_cols).sum().sort(group_cols)


async def ajustar_fraude(df: pl.DataFrame, mes_corte: date) -> pl.DataFrame:
    fraude = (
        pl.read_excel("data/segmentacion_soat.xlsx", sheet_name="Ajustes_Fraude")
        .drop("tipo_ajuste")
        .filter(pl.col("fecha_registro") <= mes_corte)
    )

    df = (
        pl.concat([df, fraude], how="vertical_relaxed")
        .group_by(
            df.collect_schema().names()[
                : df.collect_schema().names().index("fecha_registro") + 1
            ]
        )
        .sum()
    )
    df.write_csv("data/raw/siniestros.csv", separator="\t")
    df.write_parquet("data/raw/siniestros.parquet")

    return df


def generar_integridad_exactitud(
    df: pl.DataFrame, estado_cuadre: str, file: str, mes_corte: int, qtys: list[str]
) -> None:
    apr_cols = df.collect_schema().names()[: df.collect_schema().names().index(qtys[0])]
    qty_cols = df.collect_schema().names()[df.collect_schema().names().index(qtys[0]) :]

    apr_cols = [col for col in apr_cols if "fecha" not in col]

    df.select(apr_cols + qty_cols).with_columns(numero_registros=1).group_by(
        apr_cols
    ).sum().sort(apr_cols).write_excel(
        f"data/controles_informacion/{estado_cuadre}/{file}_integridad_exactitud_{mes_corte}.xlsx",
    )

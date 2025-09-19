from typing import Annotated, Any, Literal
from uuid import uuid4

import polars as pl
from fastapi import APIRouter, Cookie, Query, UploadFile
from fastapi.responses import Response, StreamingResponse

from src import db, utils
from src.controles_informacion import sap
from src.dependencias import SessionDep, atrapar_excepciones
from src.informacion import almacenamiento as alm
from src.informacion import carga_manual, mocks
from src.logger_config import logger
from src.metodos_plantilla import preparar, resultados
from src.models import Parametros

router = APIRouter()


def obtener_parametros_usuario(session: SessionDep, session_id: str | None):
    parametros = db.obtener_fila(session, Parametros, Parametros.session_id, session_id)
    if not parametros:
        raise ValueError("No se encontraron parametros ingresados.")
    return parametros


def guardar_parametros(
    session: SessionDep, session_id: str, parametros: Annotated[Parametros, Query()]
) -> Parametros:
    p = Parametros.model_validate(parametros)
    p.session_id = session_id

    if p.mes_corte < p.mes_inicio:
        raise ValueError("El mes de corte debe ser posterior al mes de inicio.")

    db.eliminar_fila(session, Parametros, Parametros.session_id, session_id)
    db.guardar_fila(session, p)

    logger.info(f"Parametros ingresados: {p.model_dump()}")

    return p


@router.post("/ingresar-parametros")
@atrapar_excepciones
async def ingresar_parametros(
    response: Response,
    session: SessionDep,
    parametros: Annotated[Parametros, Query()],
    archivo_segmentacion: UploadFile | None = None,
    session_id: Annotated[str | None, Cookie()] = None,
):
    if not session_id:
        session_id = str(uuid4())
        response.set_cookie(key="session_id", value=session_id)

    p = guardar_parametros(session, session_id, parametros)

    carga_manual.procesar_archivo_segmentacion(archivo_segmentacion, p.negocio)

    mocks.eliminar_mocks(session)
    if p.negocio == "demo":
        mocks.generar_mocks(p, session)

    aperturas = obtener_lista_aperturas(p)
    tipos_analisis_mes_anterior = obtener_tipos_analisis_mes_anterior(p)
    candidatos_siniestros = alm.obtener_cantidatos_controles(session, "siniestros")
    candidatos_primas = alm.obtener_cantidatos_controles(session, "primas")
    candidatos_expuestos = alm.obtener_cantidatos_controles(session, "expuestos")
    afos = sap.determinar_afos_necesarios(p.negocio)

    return {
        "parametros": p,
        "aperturas": aperturas,
        "tipos_analisis_mes_anterior": tipos_analisis_mes_anterior,
        "candidatos_siniestros": candidatos_siniestros,
        "candidatos_primas": candidatos_primas,
        "candidatos_expuestos": candidatos_expuestos,
        "afos": afos,
    }


@router.get("/descargar-ejemplo-segmentacion")
@atrapar_excepciones
async def descargar_ejemplo_segmentacion() -> StreamingResponse:
    buffer = carga_manual.descargar_ejemplo_segmentacion()
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=segmentacion.xlsx"},
    )


@router.get("/traer-parametros")
@atrapar_excepciones
async def traer_parametros(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> dict[str, Any]:
    return obtener_parametros_usuario(session, session_id).model_dump()


def obtener_lista_aperturas(p: Parametros) -> list[str]:
    return sorted(
        utils.obtener_aperturas(p.negocio, "siniestros")
        .get_column("apertura_reservas")
        .unique()
        .to_list()
    )


def obtener_tipos_analisis_mes_anterior(
    p: Parametros,
) -> list[Literal["triangulos", "entremes"]]:
    resultados_anteriores = resultados.concatenar_archivos_resultados()
    if p.tipo_analisis == "entremes":
        if resultados_anteriores.is_empty():
            raise ValueError(
                utils.limpiar_espacios_log(
                    """
                    No se encontraron resultados anteriores. Se necesitan
                    para hacer el analisis de entremes.
                    """
                )
            )
        mes_corte_anterior = preparar.mes_anterior_corte(
            utils.date_to_yyyymm(p.mes_corte)
        )
        resultados_mes_anterior = resultados_anteriores.filter(
            pl.col("mes_corte") == mes_corte_anterior
        )
        if resultados_mes_anterior.is_empty():
            raise ValueError(
                utils.limpiar_espacios_log(
                    f"""
                    No se encontraron resultados anteriores
                    para el mes {mes_corte_anterior}. Se necesitan
                    para hacer el analisis de entremes.
                    """
                )
            )
        return resultados_mes_anterior.get_column("tipo_analisis").unique().to_list()
    else:
        return []

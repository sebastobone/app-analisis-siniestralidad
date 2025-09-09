from typing import Annotated

from fastapi import APIRouter, Cookie, Depends, Form
from fastapi.responses import StreamingResponse

from src.dependencias import SessionDep, atrapar_excepciones
from src.informacion import almacenamiento as alm
from src.informacion import carga_manual, tera_connect
from src.models import ArchivosCantidades, CredencialesTeradata
from src.routers.parametros import obtener_parametros_usuario

router = APIRouter()


@router.post("/correr-query-siniestros")
@atrapar_excepciones
async def correr_query_siniestros(
    credenciales: Annotated[CredencialesTeradata, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    params = obtener_parametros_usuario(session, session_id)
    await tera_connect.correr_query(params, "siniestros", credenciales, session)
    candidatos_siniestros = alm.obtener_cantidatos_controles(session, "siniestros")
    return {
        "message": "Query de siniestros ejecutado exitosamente",
        "candidatos": candidatos_siniestros,
    }


@router.post("/correr-query-primas")
@atrapar_excepciones
async def correr_query_primas(
    credenciales: Annotated[CredencialesTeradata, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    params = obtener_parametros_usuario(session, session_id)
    await tera_connect.correr_query(params, "primas", credenciales, session)
    candidatos_primas = alm.obtener_cantidatos_controles(session, "primas")
    return {
        "message": "Query de primas ejecutado exitosamente",
        "candidatos": candidatos_primas,
    }


@router.post("/correr-query-expuestos")
@atrapar_excepciones
async def correr_query_expuestos(
    credenciales: Annotated[CredencialesTeradata, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    params = obtener_parametros_usuario(session, session_id)
    await tera_connect.correr_query(params, "expuestos", credenciales, session)
    candidatos_expuestos = alm.obtener_cantidatos_controles(session, "expuestos")
    return {
        "message": "Query de expuestos ejecutado exitosamente",
        "candidatos": candidatos_expuestos,
    }


@router.get("/descargar-ejemplos-cantidades")
@atrapar_excepciones
async def descargar_ejemplos_cantidades() -> StreamingResponse:
    zip_buffer = carga_manual.descargar_ejemplos_cantidades()
    return StreamingResponse(
        zip_buffer,
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": "attachment; filename=ejemplos.zip"},
    )


@router.post("/cargar-archivos")
@atrapar_excepciones
async def cargar_archivos(
    archivos: Annotated[ArchivosCantidades, Depends()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    p = obtener_parametros_usuario(session, session_id)
    carga_manual.procesar_archivos_cantidades(archivos, p, session)
    candidatos_siniestros = alm.obtener_cantidatos_controles(session, "siniestros")
    candidatos_primas = alm.obtener_cantidatos_controles(session, "primas")
    candidatos_expuestos = alm.obtener_cantidatos_controles(session, "expuestos")
    return {
        "message": "Archivos cargados exitosamente",
        "candidatos_siniestros": candidatos_siniestros,
        "candidatos_primas": candidatos_primas,
        "candidatos_expuestos": candidatos_expuestos,
    }


@router.post("/eliminar-archivos-cargados")
@atrapar_excepciones
async def eliminar_archivos_cargados(session: SessionDep):
    carga_manual.eliminar_archivos(session)
    candidatos_siniestros = alm.obtener_cantidatos_controles(session, "siniestros")
    candidatos_primas = alm.obtener_cantidatos_controles(session, "primas")
    candidatos_expuestos = alm.obtener_cantidatos_controles(session, "expuestos")
    return {
        "message": "Archivos eliminados exitosamente",
        "candidatos_siniestros": candidatos_siniestros,
        "candidatos_primas": candidatos_primas,
        "candidatos_expuestos": candidatos_expuestos,
    }

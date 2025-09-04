from typing import Annotated

from fastapi import APIRouter, Cookie, Depends, Form
from fastapi.responses import StreamingResponse

from src import constantes as ct
from src.dependencias import SessionDep, atrapar_excepciones
from src.informacion import carga_manual, mocks, tera_connect
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
    await tera_connect.correr_query(params, "siniestros", credenciales)
    return {"message": "Query de siniestros ejecutado exitosamente"}


@router.post("/correr-query-primas")
@atrapar_excepciones
async def correr_query_primas(
    credenciales: Annotated[CredencialesTeradata, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    params = obtener_parametros_usuario(session, session_id)
    await tera_connect.correr_query(params, "primas", credenciales)
    return {"message": "Query de primas ejecutado exitosamente"}


@router.post("/correr-query-expuestos")
@atrapar_excepciones
async def correr_query_expuestos(
    credenciales: Annotated[CredencialesTeradata, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    params = obtener_parametros_usuario(session, session_id)
    await tera_connect.correr_query(params, "expuestos", credenciales)
    return {"message": "Query de expuestos ejecutado exitosamente"}


@router.post("/generar-mocks")
@atrapar_excepciones
async def generar_mocks(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    p = obtener_parametros_usuario(session, session_id)
    for cantidad in ["siniestros", "primas", "expuestos"]:
        df = mocks.generar_mock(
            p.mes_inicio, p.mes_corte, cantidad, ct.NUM_FILAS_DEMO[cantidad]
        )
        mocks.guardar_mock(df, cantidad)


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
    carga_manual.procesar_archivos_cantidades(archivos, p.negocio, p.mes_inicio)
    return {"message": "Archivos cargados exitosamente"}


@router.post("/eliminar-archivos-cargados")
@atrapar_excepciones
async def eliminar_archivos_cargados():
    carga_manual.eliminar_archivos()
    return {"message": "Archivos eliminados exitosamente"}

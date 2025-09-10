import json
from typing import Annotated

from fastapi import APIRouter, Cookie, Depends, Form

from src.controles_informacion import evidencias, generacion, sap
from src.dependencias import SessionDep, atrapar_excepciones
from src.models import Afos, Controles
from src.routers.parametros import obtener_parametros_usuario

router = APIRouter()


def procesar_json_controles(controles: Annotated[str, Form()]) -> Controles:
    return Controles.model_validate(json.loads(controles))


@router.post("/generar-controles")
@atrapar_excepciones
async def generar_controles(
    controles: Annotated[Controles, Depends(procesar_json_controles)],
    afos: Annotated[Afos, Depends()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    params = obtener_parametros_usuario(session, session_id)
    await sap.procesar_afos(afos, params)
    await generacion.generar_controles(params, controles, session)
    await evidencias.generar_evidencias_parametros(params)
    return {"message": "Controles generados exitosamente"}

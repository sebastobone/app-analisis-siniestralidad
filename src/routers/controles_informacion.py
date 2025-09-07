from typing import Annotated

from fastapi import APIRouter, Cookie

from src.controles_informacion import generacion
from src.dependencias import SessionDep, atrapar_excepciones
from src.models import SeleccionadosCuadre
from src.routers.parametros import obtener_parametros_usuario

router = APIRouter()


@router.post("/generar-controles")
@atrapar_excepciones
async def generar_controles(
    archivos_cuadre: SeleccionadosCuadre,
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    params = obtener_parametros_usuario(session, session_id)
    await generacion.generar_controles(params)
    return {"message": "Controles generados exitosamente"}

from typing import Annotated

from fastapi import APIRouter, Cookie, Form

from src import utils
from src.dependencias import SessionDep, atrapar_excepciones
from src.metodos_plantilla import (
    abrir,
    actualizar,
    generar,
    graficas,
    preparar,
    resultados,
)
from src.metodos_plantilla import almacenar_analisis as almacenar
from src.metodos_plantilla.guardar_traer import (
    entremes,
    guardar_apertura,
    traer_apertura,
    traer_guardar_todo,
)
from src.models import ModosPlantilla, ReferenciasEntremes
from src.procesamiento import bases
from src.routers.parametros import obtener_parametros_usuario

router = APIRouter()


@router.post("/abrir-plantilla")
async def abrir_plantilla(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    p = obtener_parametros_usuario(session, session_id)
    _ = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    return {"message": "Plantilla abierta exitosamente"}


@router.post("/preparar-plantilla")
@atrapar_excepciones
async def preparar_plantilla(
    referencias_entremes: Annotated[ReferenciasEntremes, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    p = obtener_parametros_usuario(session, session_id)
    bases.generar_bases_plantilla(p, session)
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    preparar.preparar_plantilla(wb, p, referencias_entremes)
    return {"message": "Plantilla preparada exitosamente"}


def obtener_plantilla(session: SessionDep, session_id: str | None):
    p = obtener_parametros_usuario(session, session_id)
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    preparar.verificar_plantilla_preparada(wb)
    return wb, p


@router.post("/generar-plantilla")
@atrapar_excepciones
async def generar_plantilla(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, p = obtener_plantilla(session, session_id)
    generar.generar_plantillas(wb, p, modos)
    return {"message": "Plantilla generada exitosamente"}


@router.post("/actualizar-plantilla")
@atrapar_excepciones
async def actualizar_plantilla(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, p = obtener_plantilla(session, session_id)
    actualizar.actualizar_plantillas(wb, p, modos)
    return {"message": "Plantilla actualizada exitosamente"}


@router.post("/guardar-apertura")
@atrapar_excepciones
async def guardar(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, p = obtener_plantilla(session, session_id)
    guardar_apertura.guardar_apertura(wb, p, modos)
    return {"message": f"Apertura {modos.apertura} guardada exitosamente"}


@router.post("/traer-apertura")
@atrapar_excepciones
async def traer(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, p = obtener_plantilla(session, session_id)
    try:
        actualizar.actualizar_plantillas(wb, p, modos)
    except (
        actualizar.PlantillaNoGeneradaError,
        actualizar.PeriodicidadDiferenteError,
    ):
        generar.generar_plantillas(wb, p, modos)
    traer_apertura.traer_apertura(wb, p, modos)
    return {"message": f"Apertura {modos.apertura} traida exitosamente"}


@router.post("/guardar-entremes")
@atrapar_excepciones
async def guardar_entremes(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    wb, _ = obtener_plantilla(session, session_id)
    entremes.guardar_entremes(wb)
    return {"message": "Formulas de la hoja Entremes guardadas exitosamente"}


@router.post("/traer-entremes")
@atrapar_excepciones
async def traer_entremes(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    wb, _ = obtener_plantilla(session, session_id)
    entremes.traer_entremes(wb)
    return {"message": "Formulas de la hoja Entremes traidas exitosamente"}


@router.post("/guardar-todo")
@atrapar_excepciones
async def guardar_todo(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, p = obtener_plantilla(session, session_id)
    await traer_guardar_todo.traer_y_guardar_todas_las_aperturas(wb, p, modos, False)
    return {"message": "Todas las aperturas se guardaron exitosamente"}


@router.post("/traer-guardar-todo")
@atrapar_excepciones
async def traer_guardar_todo_end(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, p = obtener_plantilla(session, session_id)
    await traer_guardar_todo.traer_y_guardar_todas_las_aperturas(wb, p, modos, True)
    return {"message": "Todas las aperturas se trajeron y guardaron exitosamente"}


@router.post("/almacenar-analisis")
@atrapar_excepciones
async def almacenar_analisis(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    wb, p = obtener_plantilla(session, session_id)
    almacenar.almacenar_analisis(
        wb, p.nombre_plantilla, utils.date_to_yyyymm(p.mes_corte), p.tipo_analisis
    )
    return {"message": "Analisis almacenado exitosamente"}


@router.post("/ajustar-grafica-factores")
@atrapar_excepciones
async def ajustar_grafica_factores(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, _ = obtener_plantilla(session, session_id)
    graficas.ajustar_grafica_factores(wb, modos)
    return {"message": "Grafica de factores ajustada exitosamente"}


@router.post("/actualizar-wb-resultados")
@atrapar_excepciones
async def actualizar_wb_resultados():
    _ = resultados.actualizar_wb_resultados()
    return {"message": "Excel de resultados actualizado exitosamente"}


@router.post("/generar-informe-ar")
@atrapar_excepciones
async def generar_informe_actuario_responsable(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    p = obtener_parametros_usuario(session, session_id)
    resultados.generar_informe_actuario_responsable(
        p.negocio, utils.date_to_yyyymm(p.mes_corte), p.tipo_analisis
    )
    return {"message": "Informe de AR generado exitosamente"}

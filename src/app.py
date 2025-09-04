from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import wraps
from pathlib import Path
from typing import Annotated
from uuid import uuid4

from fastapi import Cookie, Depends, FastAPI, Form, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, SQLModel, create_engine, select
from sse_starlette.sse import EventSourceResponse

from src import constantes as ct
from src import utils
from src.controles_informacion import generacion
from src.informacion import carga_manual, mocks, tera_connect
from src.logger_config import log_queue, logger
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
from src.models import (
    ArchivosCantidades,
    CredencialesTeradata,
    ModosPlantilla,
    Parametros,
    ReferenciasEntremes,
)
from src.procesamiento import bases

engine = create_engine(
    "sqlite:///data/database.db", connect_args={"check_same_thread": False}
)


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


def delete_db_and_tables() -> None:
    SQLModel.metadata.drop_all(engine)


def get_session():
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield
    # delete_db_and_tables()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="src/templates")
app.mount("/static", StaticFiles(directory="src/static"), name="static")


def atrapar_excepciones(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.exception(str(e))
            raise

    return wrapper


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse("src/static/favicon.ico")


@app.get("/", response_class=HTMLResponse)
async def generar_base(request: Request):
    return templates.TemplateResponse(request, "index.html")


async def obtener_logs(request: Request) -> AsyncIterator[str]:
    while True:
        if await request.is_disconnected():
            break
        message = await log_queue.get()
        nivel_log = message.record["level"].name
        color_log = ct.COLORES_LOGS[nivel_log]
        fecha_hora = message.record["time"].strftime("%Y-%m-%d %H:%M:%S")
        texto = f"{fecha_hora} | {nivel_log} | {message.record['message']}"
        yield f"""<span style="color: {color_log}">{texto}</span><br>"""


@app.get("/stream-logs")
async def stream_logs(request: Request):
    return EventSourceResponse(obtener_logs(request))


def obtener_parametros_usuario(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> Parametros:
    return session.exec(
        select(Parametros).where(Parametros.session_id == session_id)
    ).all()[0]


def eliminar_parametros_anteriores(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    try:
        existing_data = obtener_parametros_usuario(session, session_id)
        if existing_data:
            session.delete(existing_data)
            session.commit()
    except IndexError:
        pass


@app.post("/ingresar-parametros")
@atrapar_excepciones
async def ingresar_parametros(
    response: Response,
    session: SessionDep,
    parametros: Annotated[Parametros, Query()],
    archivo_segmentacion: UploadFile | None = None,
    session_id: Annotated[str | None, Cookie()] = None,
) -> Parametros:
    if not session_id:
        session_id = str(uuid4())
        response.set_cookie(key="session_id", value=session_id)

    p = Parametros.model_validate(parametros)
    p.session_id = session_id

    eliminar_parametros_anteriores(session, session_id)

    session.add(p)
    session.commit()
    session.refresh(p)

    logger.info(f"Parametros ingresados: {p.model_dump()}")

    if archivo_segmentacion:
        carga_manual.procesar_archivo_segmentacion(archivo_segmentacion, p.negocio)

    if not Path(f"data/segmentacion_{p.negocio}.xlsx").exists():
        raise FileNotFoundError(
            f"No se encontro archivo de segmentacion para el negocio {p.negocio}"
        )

    return p


@app.get("/descargar-ejemplo-segmentacion")
@atrapar_excepciones
async def descargar_ejemplo_segmentacion() -> StreamingResponse:
    buffer = carga_manual.descargar_ejemplo_segmentacion()
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=segmentacion.xlsx"},
    )


@app.get("/traer-parametros")
@atrapar_excepciones
async def traer_parametros(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> Parametros:
    return obtener_parametros_usuario(session, session_id)


@app.post("/correr-query-siniestros")
@atrapar_excepciones
async def correr_query_siniestros(
    credenciales: Annotated[CredencialesTeradata, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    params = obtener_parametros_usuario(session, session_id)
    await tera_connect.correr_query(params, "siniestros", credenciales)
    return {"message": "Query de siniestros ejecutado exitosamente"}


@app.post("/correr-query-primas")
@atrapar_excepciones
async def correr_query_primas(
    credenciales: Annotated[CredencialesTeradata, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    params = obtener_parametros_usuario(session, session_id)
    await tera_connect.correr_query(params, "primas", credenciales)
    return {"message": "Query de primas ejecutado exitosamente"}


@app.post("/correr-query-expuestos")
@atrapar_excepciones
async def correr_query_expuestos(
    credenciales: Annotated[CredencialesTeradata, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    params = obtener_parametros_usuario(session, session_id)
    await tera_connect.correr_query(params, "expuestos", credenciales)
    return {"message": "Query de expuestos ejecutado exitosamente"}


@app.post("/generar-mocks")
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


@app.get("/descargar-ejemplos-cantidades")
@atrapar_excepciones
async def descargar_ejemplos_cantidades() -> StreamingResponse:
    zip_buffer = carga_manual.descargar_ejemplos_cantidades()
    return StreamingResponse(
        zip_buffer,
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": "attachment; filename=ejemplos.zip"},
    )


@app.post("/cargar-archivos")
@atrapar_excepciones
async def cargar_archivos(
    archivos: Annotated[ArchivosCantidades, Depends()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    p = obtener_parametros_usuario(session, session_id)
    carga_manual.procesar_archivos_cantidades(archivos, p.negocio, p.mes_inicio)
    return {"message": "Archivos cargados exitosamente"}


@app.post("/eliminar-archivos-cargados")
@atrapar_excepciones
async def eliminar_archivos_cargados():
    carga_manual.eliminar_archivos()
    return {"message": "Archivos eliminados exitosamente"}


@app.post("/generar-controles")
@atrapar_excepciones
async def generar_controles(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    params = obtener_parametros_usuario(session, session_id)
    await generacion.generar_controles(params)
    return {"message": "Controles generados exitosamente"}


@app.get("/generar-aperturas")
@atrapar_excepciones
async def generar_aperturas(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    params = obtener_parametros_usuario(session, session_id)
    aperturas = sorted(
        utils.obtener_aperturas(params.negocio, "siniestros")
        .get_column("apertura_reservas")
        .unique()
        .to_list()
    )
    return {"aperturas": aperturas}


@app.get("/obtener-analisis-anteriores")
@atrapar_excepciones
async def obtener_analisis_anteriores(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    p = obtener_parametros_usuario(session, session_id)
    resultados_mes_anterior = preparar.obtener_analisis_anteriores(p.mes_corte)
    return {
        "analisis_anteriores": resultados_mes_anterior.get_column("tipo_analisis")
        .unique()
        .to_list()
    }


@app.post("/abrir-plantilla")
async def abrir_plantilla(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    p = obtener_parametros_usuario(session, session_id)
    _ = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    return {"message": "Plantilla abierta exitosamente"}


@app.post("/preparar-plantilla")
@atrapar_excepciones
async def preparar_plantilla(
    referencias_entremes: Annotated[ReferenciasEntremes, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    p = obtener_parametros_usuario(session, session_id)
    bases.generar_bases_plantilla(p)
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    preparar.preparar_plantilla(wb, p, referencias_entremes)
    return {"message": "Plantilla preparada exitosamente"}


def obtener_plantilla(session: SessionDep, session_id: str | None):
    p = obtener_parametros_usuario(session, session_id)
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    preparar.verificar_plantilla_preparada(wb)
    return wb, p


@app.post("/generar-plantilla")
@atrapar_excepciones
async def generar_plantilla(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, p = obtener_plantilla(session, session_id)
    generar.generar_plantillas(wb, p, modos)
    return {"message": "Plantilla generada exitosamente"}


@app.post("/actualizar-plantilla")
@atrapar_excepciones
async def actualizar_plantilla(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, p = obtener_plantilla(session, session_id)
    actualizar.actualizar_plantillas(wb, p, modos)
    return {"message": "Plantilla actualizada exitosamente"}


@app.post("/guardar-apertura")
@atrapar_excepciones
async def guardar(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, p = obtener_plantilla(session, session_id)
    guardar_apertura.guardar_apertura(wb, p, modos)
    return {"message": f"Apertura {modos.apertura} guardada exitosamente"}


@app.post("/traer-apertura")
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


@app.post("/guardar-entremes")
@atrapar_excepciones
async def guardar_entremes(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    wb, _ = obtener_plantilla(session, session_id)
    entremes.guardar_entremes(wb)
    return {"message": "Formulas de la hoja Entremes guardadas exitosamente"}


@app.post("/traer-entremes")
@atrapar_excepciones
async def traer_entremes(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    wb, _ = obtener_plantilla(session, session_id)
    entremes.traer_entremes(wb)
    return {"message": "Formulas de la hoja Entremes traidas exitosamente"}


@app.post("/guardar-todo")
@atrapar_excepciones
async def guardar_todo(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, p = obtener_plantilla(session, session_id)
    await traer_guardar_todo.traer_y_guardar_todas_las_aperturas(wb, p, modos, False)
    return {"message": "Todas las aperturas se guardaron exitosamente"}


@app.post("/traer-guardar-todo")
@atrapar_excepciones
async def traer_guardar_todo_end(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, p = obtener_plantilla(session, session_id)
    await traer_guardar_todo.traer_y_guardar_todas_las_aperturas(wb, p, modos, True)
    return {"message": "Todas las aperturas se trajeron y guardaron exitosamente"}


@app.post("/almacenar-analisis")
@atrapar_excepciones
async def almacenar_analisis(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    wb, p = obtener_plantilla(session, session_id)
    almacenar.almacenar_analisis(wb, p.nombre_plantilla, p.mes_corte, p.tipo_analisis)
    return {"message": "Analisis almacenado exitosamente"}


@app.post("/ajustar-grafica-factores")
@atrapar_excepciones
async def ajustar_grafica_factores(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
):
    wb, _ = obtener_plantilla(session, session_id)
    graficas.ajustar_grafica_factores(wb, modos)
    return {"message": "Grafica de factores ajustada exitosamente"}


@app.post("/actualizar-wb-resultados")
@atrapar_excepciones
async def actualizar_wb_resultados():
    _ = resultados.actualizar_wb_resultados()
    return {"message": "Excel de resultados actualizado exitosamente"}


@app.post("/generar-informe-ar")
@atrapar_excepciones
async def generar_informe_actuario_responsable(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    p = obtener_parametros_usuario(session, session_id)
    resultados.generar_informe_actuario_responsable(
        p.negocio, p.mes_corte, p.tipo_analisis
    )
    return {"message": "Informe de AR generado exitosamente"}

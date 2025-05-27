from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import wraps
from typing import Annotated
from uuid import uuid4

from fastapi import Cookie, Depends, FastAPI, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, SQLModel, create_engine, select
from sse_starlette.sse import EventSourceResponse

from src import constantes as ct
from src import main, utils
from src.logger_config import log_queue, logger
from src.metodos_plantilla import abrir, actualizar, generar, preparar, resultados
from src.metodos_plantilla import almacenar_analisis as almacenar
from src.metodos_plantilla.guardar_traer import (
    guardar_apertura,
    traer_apertura,
    traer_guardar_todo,
)
from src.models import ModosPlantilla, Parametros

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
    params: Annotated[Parametros, Form()],
    response: Response,
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
) -> Parametros:
    if not session_id:
        session_id = str(uuid4())
        response.set_cookie(key="session_id", value=session_id)

    parametros = Parametros.model_validate(params)
    parametros.session_id = session_id

    eliminar_parametros_anteriores(session, session_id)

    session.add(parametros)
    session.commit()
    session.refresh(parametros)

    logger.info(f"Parametros ingresados: {parametros.model_dump()}")

    return parametros


@app.get("/traer-parametros")
@atrapar_excepciones
async def traer_parametros(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> Parametros:
    return obtener_parametros_usuario(session, session_id)


@app.post("/correr-query-siniestros")
@atrapar_excepciones
async def correr_query_siniestros(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    params = obtener_parametros_usuario(session, session_id)
    await main.correr_query_siniestros(params)


@app.post("/correr-query-primas")
@atrapar_excepciones
async def correr_query_primas(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    params = obtener_parametros_usuario(session, session_id)
    await main.correr_query_primas(params)


@app.post("/correr-query-expuestos")
@atrapar_excepciones
async def correr_query_expuestos(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    params = obtener_parametros_usuario(session, session_id)
    await main.correr_query_expuestos(params)


@app.post("/generar-controles")
@atrapar_excepciones
async def generar_controles(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    params = obtener_parametros_usuario(session, session_id)
    await main.generar_controles(params)


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
    logger.success("Aperturas generadas.")

    return {"aperturas": aperturas}


@app.post("/abrir-plantilla")
async def abrir_plantilla(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    p = obtener_parametros_usuario(session, session_id)
    _ = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")


@app.post("/preparar-plantilla")
@atrapar_excepciones
async def preparar_plantilla(
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
    tipo_analisis_anterior: str | None = None,
):
    p = obtener_parametros_usuario(session, session_id)
    if p.tipo_analisis == "entremes":
        resultados_anteriores = resultados.concatenar_archivos_resultados()
        resultados_mes_anterior = preparar.obtener_resultados_mes_anterior(
            resultados_anteriores, p.mes_corte, tipo_analisis_anterior
        )

        if resultados_mes_anterior.get_column("tipo_analisis").unique().len() > 1:
            return {"status": "multiples_resultados_anteriores"}

    main.generar_bases_plantilla(p)
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    preparar.preparar_plantilla(wb, p, tipo_analisis_anterior)


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
) -> None:
    wb, p = obtener_plantilla(session, session_id)
    generar.generar_plantillas(wb, p, modos)


@app.post("/actualizar-plantilla")
@atrapar_excepciones
async def actualizar_plantilla(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
) -> None:
    wb, p = obtener_plantilla(session, session_id)
    actualizar.actualizar_plantillas(wb, p, modos)


@app.post("/guardar-apertura")
@atrapar_excepciones
async def guardar(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
) -> None:
    wb, p = obtener_plantilla(session, session_id)
    guardar_apertura.guardar_apertura(wb, modos)


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
    traer_apertura.traer_apertura(wb, modos)


@app.post("/guardar-todo")
@atrapar_excepciones
async def guardar_todo(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
) -> None:
    wb, p = obtener_plantilla(session, session_id)
    await traer_guardar_todo.traer_y_guardar_todas_las_aperturas(wb, p, modos, False)


@app.post("/traer-guardar-todo")
@atrapar_excepciones
async def traer_guardar_todo_end(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
) -> None:
    wb, p = obtener_plantilla(session, session_id)
    await traer_guardar_todo.traer_y_guardar_todas_las_aperturas(wb, p, modos, True)


@app.post("/almacenar-analisis")
@atrapar_excepciones
async def almacenar_analisis(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    p = obtener_parametros_usuario(session, session_id)
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    almacenar.almacenar_analisis(wb, p.nombre_plantilla, p.mes_corte, p.tipo_analisis)


@app.post("/actualizar-wb-resultados")
@atrapar_excepciones
async def actualizar_wb_resultados() -> None:
    _ = resultados.actualizar_wb_resultados()


@app.post("/generar-informe-ar")
@atrapar_excepciones
async def generar_informe_actuario_responsable(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    p = obtener_parametros_usuario(session, session_id)
    resultados.generar_informe_actuario_responsable(
        p.negocio, p.mes_corte, p.tipo_analisis
    )

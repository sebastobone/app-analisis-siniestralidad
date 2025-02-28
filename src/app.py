from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import uuid4

from fastapi import Cookie, Depends, FastAPI, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError
from sqlmodel import Session, SQLModel, create_engine, select
from sse_starlette.sse import EventSourceResponse

from src import constantes as ct
from src import main, utils
from src.logger_config import log_queue, logger
from src.metodos_plantilla import abrir, generar, preparar, resultados
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


def validar_parametros_ingresados(params: Parametros) -> Parametros:
    try:
        parametros = Parametros.model_validate(params)
    except ValidationError:
        logger.error(
            utils.limpiar_espacios_log(
                f"""
                Parametros no validos. Revise que los meses de inicio
                y corte sean numeros enteros entre 199001 y 204001,
                y vuelva a intentar. Los parametros que ingreso fueron: 
                {params.model_dump()}.
                """
            )
        )
        raise
    return parametros


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
async def ingresar_parametros(
    params: Annotated[Parametros, Form()],
    response: Response,
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
) -> Parametros:
    if not session_id:
        session_id = str(uuid4())
        response.set_cookie(key="session_id", value=session_id)

    parametros = validar_parametros_ingresados(params)
    parametros.session_id = session_id

    eliminar_parametros_anteriores(session, session_id)

    session.add(parametros)
    session.commit()
    session.refresh(parametros)

    logger.info(f"""Parametros ingresados: {parametros.model_dump()}""")

    return params


@app.post("/correr-query-siniestros")
async def correr_query_siniestros(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    params = obtener_parametros_usuario(session, session_id)
    await main.correr_query_siniestros(params)


@app.post("/correr-query-primas")
async def correr_query_primas(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    params = obtener_parametros_usuario(session, session_id)
    await main.correr_query_primas(params)


@app.post("/correr-query-expuestos")
async def correr_query_expuestos(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    params = obtener_parametros_usuario(session, session_id)
    await main.correr_query_expuestos(params)


@app.post("/generar-controles")
async def generar_controles(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    params = obtener_parametros_usuario(session, session_id)
    await main.generar_controles(params)


@app.get("/generar-aperturas")
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
async def preparar_plantilla(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    p = obtener_parametros_usuario(session, session_id)
    main.generar_bases_plantilla(p)
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    preparar.preparar_plantilla(wb, p.mes_corte, p.tipo_analisis, p.negocio)


@app.post("/modos-plantilla")
async def modos_plantilla(
    modos: Annotated[ModosPlantilla, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
) -> None:
    p = obtener_parametros_usuario(session, session_id)
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    if modos.modo == "generar":
        generar.generar_plantilla(wb, modos, p.mes_corte)
    elif modos.modo == "guardar":
        guardar_apertura.guardar_apertura(wb, modos, p.mes_corte)
    elif modos.modo == "traer":
        traer_apertura.traer_apertura(wb, modos, p.mes_corte)
    elif modos.modo == "guardar_todo":
        await traer_guardar_todo.traer_y_guardar_todas_las_aperturas(
            wb, modos, p.mes_corte, p.negocio
        )
    elif modos.modo == "traer_guardar_todo":
        await traer_guardar_todo.traer_y_guardar_todas_las_aperturas(
            wb, modos, p.mes_corte, p.negocio, traer=True
        )


@app.post("/almacenar-analisis")
async def almacenar_analisis(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    p = obtener_parametros_usuario(session, session_id)
    wb = abrir.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    almacenar.almacenar_analisis(wb, p.nombre_plantilla, p.mes_corte)


@app.post("/actualizar-wb-resultados")
async def actualizar_wb_resultados() -> None:
    _ = resultados.actualizar_wb_resultados()


@app.post("/generar-informe-ar")
async def generar_informe_actuario_responsable(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> None:
    p = obtener_parametros_usuario(session, session_id)
    resultados.generar_informe_actuario_responsable(p.negocio, p.mes_corte)

from contextlib import asynccontextmanager
from typing import Annotated, Literal
from uuid import uuid4

from fastapi import Cookie, Depends, FastAPI, Form, Request, status
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, SQLModel, create_engine, select

from src import main, plantilla, resultados
from src.logger_config import logger
from src.models import Parametros

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

SESSION_COOKIE_NAME = "session_id"


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield
    # delete_db_and_tables()


app = FastAPI(lifespan=lifespan)


templates = Jinja2Templates(directory="src/templates")
app.mount("/static", StaticFiles(directory="src/static"), name="static")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse("src/static/favicon.ico")


@app.get("/", response_class=HTMLResponse)
async def generar_base(request: Request):
    return templates.TemplateResponse(request, "index.html")


def obtener_parametros_usuario(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> Parametros:
    return session.exec(
        select(Parametros).where(Parametros.session_id == session_id)
    ).all()[0]


@app.post("/ingresar-parametros", response_model=Parametros)
async def ingresar_parametros(
    session: SessionDep,
    params: Annotated[Parametros, Form()],
    session_id: Annotated[str | None, Cookie()] = None,
):
    response = RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    if not session_id:
        session_id = str(uuid4())
        response.set_cookie(key=SESSION_COOKIE_NAME, value=session_id)

    parametros = Parametros.model_validate(params)
    parametros.session_id = session_id

    try:
        existing_data = obtener_parametros_usuario(session, session_id)
        if existing_data:
            session.delete(existing_data)
            session.commit()
    except IndexError:
        pass

    session.add(parametros)
    session.commit()
    session.refresh(parametros)
    logger.info(session.exec(select(Parametros)).all())
    return response


@app.post("/correr-query-siniestros")
async def correr_query_siniestros(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    params = obtener_parametros_usuario(session, session_id)
    main.correr_query_siniestros(params)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/correr-query-primas")
async def correr_query_primas(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    params = obtener_parametros_usuario(session, session_id)
    main.correr_query_primas(params)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/correr-query-expuestos")
async def correr_query_expuestos(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    params = obtener_parametros_usuario(session, session_id)
    main.correr_query_expuestos(params)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/generar-controles")
async def generar_controles(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    params = obtener_parametros_usuario(session, session_id)
    main.generar_controles(params)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/abrir-plantilla")
async def abrir_plantilla(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = obtener_parametros_usuario(session, session_id)
    _ = plantilla.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/preparar-plantilla")
async def preparar_plantilla(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = obtener_parametros_usuario(session, session_id)
    main.generar_bases_plantilla(
        p.negocio,
        p.tipo_analisis,
        p.mes_inicio,
        p.mes_corte,
    )
    wb = plantilla.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    plantilla.preparar_plantilla(wb, p.mes_corte, p.tipo_analisis, p.negocio)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/modos-plantilla")
async def modos_plantilla(
    plant: Annotated[Literal["frec", "seve", "plata", "entremes"], Form()],
    modo: Annotated[
        Literal["generar", "guardar", "traer", "guardar_todo", "traer_guardar_todo"],
        Form(),
    ],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
) -> RedirectResponse:
    p = obtener_parametros_usuario(session, session_id)
    wb = plantilla.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")

    if modo == "generar":
        plantilla.generar_plantilla(wb, plant, p.mes_corte, p.negocio)
    elif modo in ["guardar", "traer"]:
        plantilla.guardar_traer_fn(wb, modo, plant, p.mes_corte)
    elif modo == "guardar_todo":
        plantilla.traer_guardar_todo(wb, plant, p.mes_corte, p.negocio)
    elif modo == "traer_guardar_todo":
        plantilla.traer_guardar_todo(wb, plant, p.mes_corte, p.negocio, traer=True)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/almacenar-analisis")
async def almacenar_analisis(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = obtener_parametros_usuario(session, session_id)
    wb = plantilla.abrir_plantilla(f"plantillas/{p.nombre_plantilla}.xlsm")
    plantilla.almacenar_analisis(wb, p.nombre_plantilla, p.mes_corte)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/actualizar-wb-resultados")
async def actualizar_wb_resultados() -> RedirectResponse:
    _ = resultados.actualizar_wb_resultados()
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/generar-informe-ar")
async def generar_informe_actuario_responsable(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = obtener_parametros_usuario(session, session_id)
    df_ar = resultados.generar_informe_actuario_responsable(p.mes_corte)
    df_ar.write_excel(
        f"output/informe_ar_{p.negocio}_{p.mes_corte}.xlsx", worksheet="AR"
    )
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

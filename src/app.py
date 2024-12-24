from fastapi import FastAPI, Form, Request, status, Depends, Cookie, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from src import main
from src import plantilla
from typing import Annotated
from sqlmodel import Field, Session, SQLModel, create_engine, select
from uuid import uuid4

app = FastAPI()


templates = Jinja2Templates(directory="src/templates")
app.mount("/static", StaticFiles(directory="src/static"), name="static")


class Parametros(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    negocio: str
    mes_inicio: int
    mes_corte: int
    tipo_analisis: str
    aproximar_reaseguro: int
    nombre_plantilla: str
    session_id: str | None = Field(index=True)


sqlite_file_name = "database.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

engine = create_engine(sqlite_url, connect_args={"check_same_thread": False})


def create_db_and_tables():
    # SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]

SESSION_COOKIE_NAME = "session_id"


def parametros_usuario(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> list[Parametros]:
    return session.query(Parametros).filter(Parametros.session_id == session_id).all()


@app.on_event("startup")
def on_startup():
    create_db_and_tables()


@app.get("/", response_class=HTMLResponse)
async def generar_api(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/ingresar-parametros")
async def ingresar_parametros(
    parametros: Annotated[Parametros, Form()],
    session: SessionDep,
    response: Response,
    session_id: Annotated[str | None, Cookie()] = None,
):
    if not session_id:
        session_id = str(uuid4())
        response.set_cookie(key=SESSION_COOKIE_NAME, value=session_id)
    parametros.session_id = session_id

    existing_data = parametros_usuario(session, session_id)
    if existing_data:
        for data in existing_data:
            session.delete(data)
            session.commit()

    session.add(parametros)
    session.commit()
    session.refresh(parametros)
    print(session.exec(select(Parametros)).all())
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/correr-query-siniestros")
async def correr_query_siniestros(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    main.correr_query_siniestros(
        p.negocio,
        p.mes_inicio,
        p.mes_corte,
        p.tipo_analisis,
        p.aproximar_reaseguro == 1,
    )
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/correr-query-primas")
async def correr_query_primas(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    main.correr_query_primas(
        p.negocio, p.mes_inicio, p.mes_corte, p.aproximar_reaseguro == 1
    )
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/correr-query-expuestos")
async def correr_query_expuestos(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    main.correr_query_expuestos(p.negocio, p.mes_inicio, p.mes_corte)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/generar-controles")
async def generar_controles(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    main.generar_controles(p.negocio, p.mes_corte)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/abrir-plantilla")
async def abrir_plantilla(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    wb = main.abrir_plantilla(f"src/{p.nombre_plantilla}.xlsm")
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/preparar-plantilla")
async def preparar_plantilla(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    wb = main.abrir_plantilla(f"src/{p.nombre_plantilla}.xlsm")
    plantilla.preparar_plantilla(wb, p.mes_corte, p.tipo_analisis)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/almacenar-analisis")
async def almacenar_analisis(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    wb = main.abrir_plantilla(f"src/{p.nombre_plantilla}.xlsm")
    plantilla.almacenar_analisis(wb, p.mes_corte)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/modos-plantilla")
async def generar_plantilla(
    plant: Annotated[str, Form()],
    modo: Annotated[str, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    wb = main.abrir_plantilla(f"src/{p.nombre_plantilla}.xlsm")

    if modo == "generar":
        plantilla.generar_plantilla(
            wb,
            plant,
            wb.sheets["Aperturas"]["A2"].value,
            wb.sheets["Atributos"]["A2"].value,
            p.mes_corte,
        )
    elif modo in ["guardar", "traer"]:
        plantilla.guardar_traer_fn(
            wb,
            modo,
            plant,
            wb.sheets["Aperturas"]["A2"].value,
            wb.sheets["Atributos"]["A2"].value,
            p.mes_corte,
        )
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

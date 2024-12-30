from contextlib import asynccontextmanager
from typing import Annotated, Literal
from uuid import uuid4

from fastapi import Cookie, Depends, FastAPI, Form, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlmodel import Field, Session, SQLModel, create_engine, select

from src import main, plantilla

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


def parametros_usuario(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
):
    return session.exec(
        select(Parametros).where(Parametros.session_id == session_id)
    ).all()


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield
    # delete_db_and_tables()


app = FastAPI(lifespan=lifespan)


templates = Jinja2Templates(directory="src/templates")
app.mount("/static", StaticFiles(directory="src/static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def generar_base(request: Request):
    return templates.TemplateResponse(request, "index.html")


class Parametros(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    negocio: str
    mes_inicio: int
    mes_corte: int
    tipo_analisis: str
    aproximar_reaseguro: bool
    nombre_plantilla: str
    cuadre_contable_sinis: bool
    add_fraude_soat: bool
    cuadre_contable_primas: bool
    session_id: str | None = Field(index=True)


@app.post("/ingresar-parametros", response_model=Parametros)
async def ingresar_parametros(
    session: SessionDep,
    response: Response,
    params: Annotated[Parametros, Form()],
    session_id: Annotated[str | None, Cookie()] = None,
):
    if not session_id:
        session_id = str(uuid4())
        response.set_cookie(key=SESSION_COOKIE_NAME, value=session_id)

    parametros = Parametros.model_validate(params)
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
        p.aproximar_reaseguro,
    )
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/correr-query-primas")
async def correr_query_primas(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    main.correr_query_primas(
        p.negocio, p.mes_inicio, p.mes_corte, p.aproximar_reaseguro
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
    main.generar_controles(
        p.negocio,
        p.mes_corte,
        p.cuadre_contable_sinis,
        p.add_fraude_soat,
        p.cuadre_contable_primas,
    )
    main.generar_bases_plantilla(
        p.negocio,
        p.tipo_analisis,
        p.mes_inicio,
        p.mes_corte,
    )
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/abrir-plantilla")
async def abrir_plantilla(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    _ = plantilla.abrir_plantilla(f"src/{p.nombre_plantilla}.xlsm")
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/preparar-plantilla")
async def preparar_plantilla(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    wb = plantilla.abrir_plantilla(f"src/{p.nombre_plantilla}.xlsm")
    plantilla.preparar_plantilla(wb, p.mes_corte, p.tipo_analisis, p.negocio)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/almacenar-analisis")
async def almacenar_analisis(
    session: SessionDep, session_id: Annotated[str | None, Cookie()] = None
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    wb = plantilla.abrir_plantilla(f"src/{p.nombre_plantilla}.xlsm")
    plantilla.almacenar_analisis(wb, p.mes_corte)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/modos-plantilla")
async def generar_plantilla(
    plant: Annotated[Literal["frec", "seve", "plata", "entremes"], Form()],
    modo: Annotated[str, Form()],
    session: SessionDep,
    session_id: Annotated[str | None, Cookie()] = None,
) -> RedirectResponse:
    p = parametros_usuario(session, session_id)[0]
    wb = plantilla.abrir_plantilla(f"src/{p.nombre_plantilla}.xlsm")

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

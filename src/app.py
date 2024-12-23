from fastapi import FastAPI, Form, Request, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from src import main
from typing import Annotated

app = FastAPI()

app.mount("/static", StaticFiles(directory="src/static"), name="static")
templates = Jinja2Templates(directory="src/templates")


@app.get("/", response_class=HTMLResponse)
async def read_form(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/correr-query-siniestros")
async def correr_query_siniestros(negocio: Annotated[str, Form()]) -> RedirectResponse:
    main.correr_query_siniestros(negocio)
    return RedirectResponse(url="/")


@app.post("/correr-query-primas")
async def correr_query_primas(negocio: Annotated[str, Form()]) -> RedirectResponse:
    main.correr_query_primas(negocio)
    return RedirectResponse(url="/")


@app.post("/correr-query-expuestos")
async def correr_query_expuestos(negocio: Annotated[str, Form()]) -> RedirectResponse:
    main.correr_query_expuestos(negocio)
    return RedirectResponse(url="/")


@app.post("/generar-controles")
async def generar_controles() -> RedirectResponse:
    main.generar_controles()
    return RedirectResponse(url="/")


@app.post("/preparar-plantilla")
async def preparar_plantilla(path_wb: Annotated[str, Form()]) -> RedirectResponse:
    wb = main.abrir_plantilla(path_wb)
    main.modos_plantilla(wb, "preparar")
    return RedirectResponse(url="/")


@app.post("/almacenar-analisis")
async def almacenar_analisis(path_wb: Annotated[str, Form()]) -> RedirectResponse:
    wb = main.abrir_plantilla(path_wb)
    main.modos_plantilla(wb, "almacenar")
    return RedirectResponse(url="/")


@app.post("/generar-plantilla")
async def generar_plantilla(
    path_wb: Annotated[str, Form()], plantilla: Annotated[str, Form()]
) -> RedirectResponse:
    wb = main.abrir_plantilla(path_wb)
    main.modos_plantilla(wb, "generar", plantilla)
    return RedirectResponse(url="/")


@app.post("/guardar-plantilla")
async def guardar_plantilla(
    path_wb: Annotated[str, Form()], plantilla: Annotated[str, Form()]
) -> RedirectResponse:
    wb = main.abrir_plantilla(path_wb)
    main.modos_plantilla(wb, "guardar", plantilla)
    return RedirectResponse(url="/")


@app.post("/traer-plantilla")
async def traer_plantilla(
    path_wb: Annotated[str, Form()], plantilla: Annotated[str, Form()]
) -> RedirectResponse:
    wb = main.abrir_plantilla(path_wb)
    main.modos_plantilla(wb, "traer", plantilla)
    return RedirectResponse(url="/")


# @app.post("/boton_generar_controles")
# async def boton_generar_controles():
#     main.generar_controles()
#     return RedirectResponse(url="/", status_code=303)


# @app.post("/boton_abrir_plantilla")
# async def boton_abrir_plantilla():
#     global wb
#     wb = main.abrir_plantilla(wb_path)
#     print(wb_path)
#     print(wb)
#     return RedirectResponse(url="/", status_code=303)

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from src import main
from src import plantilla
from typing import Annotated

app = FastAPI()

app.mount("/static", StaticFiles(directory="src/static"), name="static")
templates = Jinja2Templates(directory="src/templates")


@app.get("/", response_class=HTMLResponse)
async def read_form(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/correr-query-siniestros")
async def correr_query_siniestros(
    negocio: Annotated[str, Form()],
    mes_inicio: Annotated[int, Form()],
    mes_corte: Annotated[int, Form()],
    tipo_analisis: Annotated[str, Form()],
    aproximar_reaseguro: Annotated[bool, Form()],
) -> RedirectResponse:
    main.correr_query_siniestros(
        negocio, mes_inicio, mes_corte, tipo_analisis, aproximar_reaseguro
    )
    return RedirectResponse(url="/")


@app.post("/correr-query-primas")
async def correr_query_primas(
    negocio: Annotated[str, Form()],
    mes_inicio: Annotated[int, Form()],
    mes_corte: Annotated[int, Form()],
    tipo_analisis: Annotated[str, Form()],
    aproximar_reaseguro: Annotated[bool, Form()],
) -> RedirectResponse:
    main.correr_query_primas(
        negocio, mes_inicio, mes_corte, tipo_analisis, aproximar_reaseguro
    )
    return RedirectResponse(url="/")


@app.post("/correr-query-expuestos")
async def correr_query_expuestos(
    negocio: Annotated[str, Form()],
    mes_inicio: Annotated[int, Form()],
    mes_corte: Annotated[int, Form()],
    tipo_analisis: Annotated[str, Form()],
    aproximar_reaseguro: Annotated[bool, Form()],
) -> RedirectResponse:
    main.correr_query_expuestos(
        negocio, mes_inicio, mes_corte, tipo_analisis, aproximar_reaseguro
    )
    return RedirectResponse(url="/")


@app.post("/generar-controles")
async def generar_controles(
    negocio: Annotated[str, Form()], mes_corte: Annotated[int, Form()]
) -> RedirectResponse:
    main.generar_controles(negocio, mes_corte)
    return RedirectResponse(url="/")


@app.post("/preparar-plantilla")
async def preparar_plantilla(
    path_wb: Annotated[str, Form()],
    mes_corte: Annotated[int, Form()],
    tipo_analisis: Annotated[str, Form()],
) -> RedirectResponse:
    wb = main.abrir_plantilla(path_wb)
    plantilla.preparar_plantilla(wb, mes_corte, tipo_analisis)
    return RedirectResponse(url="/")


@app.post("/almacenar-analisis")
async def almacenar_analisis(
    path_wb: Annotated[str, Form()], mes_corte: Annotated[int, Form()]
) -> RedirectResponse:
    wb = main.abrir_plantilla(path_wb)
    plantilla.almacenar_analisis(wb, mes_corte)
    return RedirectResponse(url="/")


@app.post("/generar-plantilla")
async def generar_plantilla(
    path_wb: Annotated[str, Form()],
    plant: Annotated[str, Form()],
    mes_corte: Annotated[int, Form()],
) -> RedirectResponse:
    wb = main.abrir_plantilla(path_wb)
    plantilla.generar_plantilla(
        wb,
        plant,
        wb.sheets["Aperturas"]["A2"].value,
        wb.sheets["Atributos"]["A2"].value,
        mes_corte,
    )
    return RedirectResponse(url="/")


@app.post("/guardar-plantilla")
async def guardar_plantilla(
    path_wb: Annotated[str, Form()],
    plant: Annotated[str, Form()],
    mes_corte: Annotated[int, Form()],
) -> RedirectResponse:
    wb = main.abrir_plantilla(path_wb)
    plantilla.guardar_traer_fn(
        wb,
        "guardar",
        wb.sheets["Aperturas"]["A2"].value,
        wb.sheets["Atributos"]["A2"].value,
        plant,
        mes_corte,
    )
    return RedirectResponse(url="/")


@app.post("/traer-plantilla")
async def traer_plantilla(
    path_wb: Annotated[str, Form()],
    plant: Annotated[str, Form()],
    mes_corte: Annotated[int, Form()],
) -> RedirectResponse:
    wb = main.abrir_plantilla(path_wb)
    plantilla.guardar_traer_fn(
        wb,
        "traer",
        wb.sheets["Aperturas"]["A2"].value,
        wb.sheets["Atributos"]["A2"].value,
        plant,
        mes_corte,
    )
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

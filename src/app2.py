from fastapi import FastAPI, Form, Request, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from src import main

app = FastAPI()

# Templates directory setup
templates = Jinja2Templates(directory="src/templates")
app.mount("/static", StaticFiles(directory="src/static"), name="static")

# Shared state (replacing globals)
class AppState:
    def __init__(self):
        self.negocio = None
        self.message = None
        self.wb_path = None

# Dependency to provide shared state
def get_app_state():
    return AppState()

@app.get("/", response_class=HTMLResponse)
@app.post("/", response_class=HTMLResponse)
async def index(request: Request, app_state: AppState = Depends(get_app_state)):
    if request.method == "POST":
        form_data = await request.form()
        app_state.negocio = form_data.get("dropdown_negocio")
        app_state.wb_path = form_data.get("wb_path")
    return templates.TemplateResponse(
        "index.html", 
        {"request": request, "negocio": app_state.negocio, "message": app_state.message}
    )

@app.post("/boton_correr_query_siniestros")
async def boton_correr_siniestros(app_state: AppState = Depends(get_app_state)):
    app_state.message = "Query de siniestros ejecutado exitosamente."
    main.correr_query_siniestros(app_state.negocio)
    return RedirectResponse(url="/", status_code=303)

@app.post("/boton_correr_query_primas")
async def boton_correr_primas(app_state: AppState = Depends(get_app_state)):
    app_state.message = "Query de primas ejecutado exitosamente."
    main.correr_query_primas(app_state.negocio)
    return RedirectResponse(url="/", status_code=303)

@app.post("/boton_correr_query_expuestos")
async def boton_correr_expuestos(app_state: AppState = Depends(get_app_state)):
    app_state.message = "Query de expuestos ejecutado exitosamente."
    main.correr_query_primas(app_state.negocio)
    return RedirectResponse(url="/", status_code=303)

@app.post("/boton_generar_controles")
async def boton_generar_controles():
    main.generar_controles()
    return RedirectResponse(url="/", status_code=303)
import datetime as dt
from calendar import monthrange

import numpy as np
import polars as pl
from sqlmodel import select

from src import constantes as ct
from src import utils
from src.dependencias import SessionDep
from src.informacion import almacenamiento as alm
from src.models import MetadataCantidades
from src.validation import cantidades


def generar_mocks(mes_inicio: dt.date, mes_corte: dt.date, session: SessionDep) -> None:
    for cantidad in ct.LISTA_CANTIDADES:
        generar_mock(
            (mes_inicio, mes_corte), cantidad, ct.NUM_FILAS_DEMO[cantidad]
        ).pipe(
            alm.guardar_archivo,
            session,
            MetadataCantidades(
                ruta=f"data/demo/{cantidad}.parquet",
                nombre_original=f"{cantidad}.parquet",
                origen="demo",
                cantidad=cantidad,
            ),
        )


def generar_mock(
    rango_meses: tuple[dt.date, dt.date],
    cantidad: ct.CANTIDADES,
    num_rows: int = 100000,
) -> pl.DataFrame:
    if cantidad == "siniestros":
        df = generar_mock_siniestros(rango_meses, num_rows)
    elif cantidad == "primas":
        df = generar_mock_primas(rango_meses, num_rows)
    elif cantidad == "expuestos":
        df = generar_mock_expuestos(rango_meses, num_rows)

    return df.pipe(
        cantidades.organizar_archivo, "demo", rango_meses, cantidad, cantidad
    )


def generar_mock_siniestros(
    rango_meses: tuple[dt.date, dt.date], num_rows: int = 100000
) -> pl.DataFrame:
    fecha_inicio_int = rango_meses[0].toordinal()
    fecha_fin_int = (
        rango_meses[1]
        .replace(day=monthrange(rango_meses[1].year, rango_meses[1].month)[1])
        .toordinal()
    )

    dias = np.random.randint(fecha_inicio_int, fecha_fin_int, size=num_rows)
    fecha_siniestro = np.array([dt.date.fromordinal(dia) for dia in dias])

    retrasos = np.random.lognormal(5, 0.9, size=num_rows)
    fecha_registro = np.array(
        [fecha_siniestro[i] + dt.timedelta(days=retrasos[i]) for i in range(num_rows)]
    )

    pago_bruto = np.random.lognormal(18, 1, size=num_rows)

    return pl.DataFrame(
        {
            "codigo_op": np.random.choice(["01"], size=num_rows),
            "codigo_ramo_op": np.random.choice(["040", "041"], size=num_rows),
            "apertura_1": np.random.choice(["A", "B"], size=num_rows),
            "apertura_2": np.random.choice(["D", "E"], size=num_rows),
            "atipico": np.random.choice([0, 1], size=num_rows, p=[0.95, 0.05]),
            "fecha_siniestro": fecha_siniestro.tolist(),
            "fecha_registro": fecha_registro.tolist(),
            "pago_bruto": pago_bruto,
            "pago_retenido": pago_bruto * 0.1,
            "aviso_bruto": pago_bruto * 0.001,
            "aviso_retenido": pago_bruto * 0.0001,
            "conteo_pago": np.random.poisson(10, size=num_rows),
            "conteo_incurrido": np.random.poisson(11, size=num_rows),
            "conteo_desistido": np.random.poisson(1, size=num_rows),
        }
    )


def generar_mock_primas(
    rango_meses: tuple[dt.date, dt.date], num_rows: int = 10000
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "codigo_op": np.random.choice(["01"], size=num_rows),
            "codigo_ramo_op": np.random.choice(["040", "041"], size=num_rows),
            "apertura_1": np.random.choice(["A", "B"], size=num_rows),
            "apertura_2": np.random.choice(["D", "E"], size=num_rows),
            "fecha_registro": np.random.choice(
                pl.date_range(
                    rango_meses[0], rango_meses[1], interval="1mo", eager=True
                ),
                size=num_rows,
            ),
            "prima_bruta": np.random.random(size=num_rows) * 1e10,
            "prima_retenida": np.random.random(size=num_rows) * 1e9,
            "prima_bruta_devengada": np.random.random(size=num_rows) * 1e10,
            "prima_retenida_devengada": np.random.random(size=num_rows) * 1e9,
        }
    )


def generar_mock_expuestos(
    rango_meses: tuple[dt.date, dt.date], num_rows: int = 10000
) -> pl.DataFrame:
    return (
        pl.DataFrame(
            {
                "codigo_op": np.random.choice(["01"], size=num_rows),
                "codigo_ramo_op": np.random.choice(["040", "041"], size=num_rows),
                "apertura_1": np.random.choice(["A", "B"], size=num_rows),
                "apertura_2": np.random.choice(["D", "E"], size=num_rows),
                "fecha_registro": np.random.choice(
                    pl.date_range(
                        rango_meses[0], rango_meses[1], interval="1mo", eager=True
                    ),
                    size=num_rows,
                ),
                "expuestos": np.random.random(size=num_rows) * 1e6,
                "vigentes": np.random.random(size=num_rows) * 1e6,
            }
        )
        .group_by(
            [
                "codigo_op",
                "codigo_ramo_op",
                "apertura_1",
                "apertura_2",
                "fecha_registro",
            ]
        )
        .mean()
    )


def eliminar_mocks(session: SessionDep):
    utils.vaciar_directorio("data/demo")
    query = select(MetadataCantidades).where(MetadataCantidades.origen == "demo")
    rutas = session.exec(query).all()
    for item in rutas:
        session.delete(item)
    session.commit()

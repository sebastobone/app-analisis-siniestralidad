import polars as pl

from src import utils


async def validar_tabla_a_cargar(query: str, add: pl.DataFrame) -> pl.DataFrame:
    await validar_numero_columnas_segmentacion(query, add)
    utils.validar_unicidad(
        add,
        """
        Alerta -> tiene registros duplicados en la siguiente tabla: {add}
        El proceso los elimina y va a continuar, pero se recomienda
        revisar la tabla en el Excel.
        """,
        {"add": add},
        "alerta",
    )
    utils.validar_no_nulos(
        add,
        """
        Error -> tiene valores nulos en la siguiente tabla: {add}
        Corrija estos valores antes de ejecutar el proceso.
        """,
        {"add": add},
    )
    return add.unique()


async def validar_nombre_hojas_segmentacion(segm_sheets: list[str]) -> None:
    for sheet in segm_sheets:
        partes = sheet.split("_")
        if (len(partes) < 3) or not any(char in partes[1] for char in "spe"):
            raise ValueError(
                """
                El nombre de las hojas con tablas a cargar debe seguir el formato
                "add_[indicador de queries que la utilizan]_[nombre de la tabla]".
                El indicador se escribe de la siguiente forma:
                    siniestros -> s
                    primas -> p
                    expuestos -> e
                Ejemplo: "add_spe_Canales" o "add_p_Sucursales".
                Corregir el nombre de la hoja.
                """
            )


async def validar_numero_segmentaciones(
    file_path: str, negocio: str, queries: list[str], adds: list[pl.DataFrame]
) -> None:
    num_adds_necesarios = 0
    for query in queries:
        if "?" in query:
            num_adds_necesarios += 1

    if num_adds_necesarios != len(adds):
        raise ValueError(
            f"""
            Necesita {num_adds_necesarios} tablas adicionales para 
            ejecutar el query {file_path},
            pero en el Excel "segmentacion_{negocio}.xlsx" hay {len(adds)} hojas
            de este tipo. Por favor, revise las hojas que tiene o revise que el 
            nombre de las hojas siga el formato
            "add_[indicador de queries que la utilizan]_[nombre de la tabla]".
            Hojas actuales: {adds}
            """
        )


async def validar_numero_columnas_segmentacion(query: str, add: pl.DataFrame) -> None:
    num_cols = query.count("?")
    num_cols_add = len(add.collect_schema().names())
    if num_cols != num_cols_add:
        raise ValueError(
            f"""
            Error en {query}:
            La tabla creada en Teradata recibe {num_cols} columnas, pero la
            tabla que esta intentando ingresar tiene {num_cols_add} columnas:
            {add}
            Revise que el orden de las tablas en el Excel (de izquierda a derecha)
            sea el mismo que el del query.
            """
        )

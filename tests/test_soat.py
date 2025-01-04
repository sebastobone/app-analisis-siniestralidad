# @pytest.fixture
# def params_soat() -> dict[str, str]:
#     return {
#         "negocio": "soat",
#         "mes_inicio": "201901",
#         "mes_corte": "202401",
#         "tipo_analisis": "triangulos",
#         "aproximar_reaseguro": "False",
#         "nombre_plantilla": "plantilla_test_soat",
#         "cuadre_contable_sinis": "True",
#         "add_fraude_soat": "False",
#         "cuadre_contable_primas": "False",
#     }


# @pytest.mark.soat
# @pytest.mark.end_to_end
# @pytest.mark.teradata
# def test_info_soat(client: TestClient, params_soat: dict[str, str]):
#     _ = client.post("/ingresar-parametros", data=params_soat)

#     lista_vehiculos = ["AUTO", "MOTO"]
#     lista_canales = ["ASESORES", "CERRADO", "CORBETA", "DIGITAL", "EXITO", "RESTO"]

#     for query in ["siniestros", "primas", "expuestos"]:
#         _ = client.post(f"/correr-query-{query}")
#         df = pl.read_parquet("data/raw/siniestros.parquet")
#         assert (
#             sorted(df.get_column("apertura_canal_desc").unique().to_list())
#             == lista_canales
#         )
#         assert (
#             sorted(df.get_column("tipo_vehiculo").unique().to_list()) == lista_vehiculos
#         )

#     _ = client.post("/generar-controles")

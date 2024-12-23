import polars as pl


def segm() -> dict[str, pl.DataFrame]:
    return pl.read_excel(
        source="data/segmentacion_autonomia.xlsx",
        sheet_name=[
            "add_pe_Canal-Poliza",
            "add_pe_Canal-Canal",
            "add_pe_Canal-Sucursal",
            "add_e_Amparos",
            "Atipicos",
            "Inc_Ced_Atipicos",
            "SAP_Sinis_Ced",
        ],
    )

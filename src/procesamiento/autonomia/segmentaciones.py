import polars as pl


def segm() -> dict[str, pl.DataFrame]:
    return pl.read_excel(
        source="data/segmentacion_autonomia.xlsx",
        sheet_name=[
            "add_spe_Canal-Poliza",
            "add_spe_Canal-Canal",
            "add_spe_Canal-Sucursal",
            "add_se_Amparos",
            "add_s_Atipicos",
            "add_s_Inc_Ced_Atipicos",
            "add_s_SAP_Sinis_Ced",
        ],
    )  # type: ignore

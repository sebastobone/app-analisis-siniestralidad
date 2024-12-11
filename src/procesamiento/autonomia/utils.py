import polars as pl


def lowercase_cols(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return df.rename({col: col.lower() for col in df.collect_schema().names()}).lazy()


def segm() -> dict[str, pl.DataFrame]:
    return pl.read_excel(
        "data/segmentacion_autonomia.xlsx",
        sheet_name=[
            "add_pe_Canal-Poliza",
            "add_pe_Canal-Canal",
            "add_pe_Canal-Sucursal",
            "add_pe_Amparos",
            "Atipicos",
            "Inc_Ced_Atipicos",
            "SAP_Sinis_Ced",
        ],
    )

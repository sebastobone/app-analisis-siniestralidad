import os

import polars as pl
import xlwings as xw


def concatenar_archivos_resultados() -> pl.DataFrame:
    dfs = []
    files = [file for file in os.listdir("output/resultados") if file != ".gitkeep"]
    sorted_files = sorted(
        files, key=lambda f: os.path.getmtime(os.path.join("output/resultados", f))
    )
    for file in sorted_files:
        dfs.append(
            pl.read_parquet(f"output/resultados/{file}").filter(
                pl.col("plata_ultimate_bruto")
                .sum()
                .over(["atipico", "mes_corte", "apertura_reservas"])
                != 0
            )
        )

    df_resultados = (
        pl.DataFrame(pl.concat(dfs))
        .unique(subset=["apertura_reservas", "periodo_ocurrencia"], keep="last")
        .sort(["mes_corte", "atipico", "apertura_reservas", "periodo_ocurrencia"])
    )

    return df_resultados


def actualizar_wb_resultados() -> xw.Book:
    wb = xw.Book("output/resultados.xlsx")

    wb.sheets["Resultados"].clear_contents()
    wb.sheets["Resultados"]["A1"].options(
        index=False
    ).value = concatenar_archivos_resultados().to_pandas()

    return wb


def generar_informe_actuario_responsable(mes_corte: int) -> pl.DataFrame:
    periodicidades_desc = pl.LazyFrame(
        {
            "periodicidad_ocurrencia": [
                "Mensual",
                "Trimestral",
                "Semestral",
                "Anual",
            ],
            "llave": ["M", "Q", "S", "A"],
            "numero_meses": [1, 3, 6, 12],
        }
    )

    df = concatenar_archivos_resultados().lazy()

    df_sinis = (
        df.select(
            [
                "ramo_desc",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "pago_bruto",
                "aviso_bruto",
                "ibnr_bruto",
                "plata_ultimate_contable_bruto",
                "pago_retenido",
                "aviso_retenido",
                "ibnr_retenido",
                "plata_ultimate_contable_retenido",
            ]
        )
        .group_by(["ramo_desc", "periodicidad_ocurrencia", "periodo_ocurrencia"])
        .sum()
    )

    df_primas = (
        df.select(
            [
                "ramo_desc",
                "periodicidad_ocurrencia",
                "periodo_ocurrencia",
                "prima_bruta_devengada",
                "prima_retenida_devengada",
            ]
        )
        .unique()
        .group_by(["ramo_desc", "periodicidad_ocurrencia", "periodo_ocurrencia"])
        .sum()
    )

    df_ar = (
        df_sinis.join(
            df_primas,
            on=["ramo_desc", "periodicidad_ocurrencia", "periodo_ocurrencia"],
            how="left",
        )
        .fill_null(0)
        .join(periodicidades_desc, on="periodicidad_ocurrencia", how="left")
        .with_columns(
            periodo_ocurrencia=(pl.col("periodo_ocurrencia") // pl.lit(100))
            .cast(pl.Int32)
            .cast(pl.String)
            + " "
            + pl.concat_str(
                pl.col("llave"),
                ((pl.col("periodo_ocurrencia") % pl.lit(100)) / pl.col("numero_meses"))
                .ceil()
                .cast(pl.Int8)
                .cast(pl.String),
            )
        )
        .select(
            ["ramo_desc", "periodo_ocurrencia"]
            + [column for column in df_sinis.collect_schema() if "bruto" in column]
            + ["prima_bruta_devengada"]
            + [column for column in df_sinis.collect_schema() if "retenido" in column]
            + ["prima_retenida_devengada"]
        )
        .sort(["ramo_desc", "periodo_ocurrencia"])
        .collect()
    )

    return df_ar

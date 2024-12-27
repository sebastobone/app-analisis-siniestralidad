import time

import polars as pl
import xlwings as xw

import constantes as ct


def main():
    wb = xw.Book.caller()
    path_plantilla = ct.path_plantilla(wb)

    modo = wb.sheets["Modo"]["A1"].value
    # MES_CORTE = wb.sheets["Modo"]["A4"].value

    if modo == "GENERAR_ACTUARIO_RESPONSABLE":
        s = time.time()

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

        df = ct.sheet_to_dataframe(wb, "BD")
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
                    (
                        (pl.col("periodo_ocurrencia") % pl.lit(100))
                        / pl.col("numero_meses")
                    )
                    .ceil()
                    .cast(pl.Int8)
                    .cast(pl.String),
                )
            )
            .select(
                ["ramo_desc", "periodo_ocurrencia"]
                + [column for column in df_sinis.collect_schema() if "bruto" in column]
                + ["prima_bruta_devengada"]
                + [
                    column
                    for column in df_sinis.collect_schema()
                    if "retenido" in column
                ]
                + ["prima_retenida_devengada"]
            )
            .sort(["ramo_desc", "periodo_ocurrencia"])
        )

        df_ar.collect().write_excel(
            f"{path_plantilla}/../output/ar.xlsx", worksheet="AR"
        )

        wb.sheets["Modo"]["A2"].value = time.time() - s


if __name__ == "__main__":
    xw.Book("resultados.xlsm").set_mock_caller()
    main()

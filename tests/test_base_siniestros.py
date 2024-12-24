import pytest
from src.procesamiento import base_siniestros as base
import polars as pl


def test_triangulo(mock_siniestros: pl.LazyFrame):
    mes_corte = 202406
    mock_siniestros_tri = (
        mock_siniestros.with_columns(
            pl.col("fecha_siniestro").clip(upper_bound=pl.col("fecha_registro"))
        )
        .filter(
            (pl.col("fecha_siniestro") <= pl.date(mes_corte // 100, mes_corte % 100, 1))
            & (
                pl.col("fecha_registro")
                <= pl.date(mes_corte // 100, mes_corte % 100, 1)
            )
            & (pl.col("atipico") == 0)
        )
        .collect()
        .get_column("pago_bruto")
        .sum()
    )

    processed_data = (
        base.sinis_prep(mock_siniestros, 201001, mes_corte)[0]
        .pipe(base.triangulos, "Trimestral", "Trimestral", mes_corte, "triangulos")
        .filter(pl.col("diagonal") == 1)
        .collect()
        .get_column("pago_bruto")
        .sum()
    )

    assert mock_siniestros_tri - processed_data < 100


def test_diagonales(mock_siniestros: pl.LazyFrame):
    mes_corte = 202406
    mock_siniestros_diag = (
        mock_siniestros.with_columns(
            pl.col("fecha_siniestro").clip(upper_bound=pl.col("fecha_registro"))
        )
        .filter(
            (
                pl.col("fecha_siniestro").is_between(
                    pl.date(mes_corte // 100, mes_corte % 100, 1).dt.offset_by("-3mo"),
                    pl.date(mes_corte // 100, mes_corte % 100, 1),
                )
            )
            & (
                pl.col("fecha_registro").is_between(
                    pl.date(mes_corte // 100, mes_corte % 100, 1).dt.offset_by("-3mo"),
                    pl.date(mes_corte // 100, mes_corte % 100, 1),
                )
            )
            & (pl.col("atipico") == 0)
        )
        .collect()
        .get_column("pago_bruto")
        .sum()
    )

    processed_data = (
        base.sinis_prep(mock_siniestros, 201001, mes_corte)[0]
        .pipe(base.diagonales, "Trimestral", 201001, mes_corte, True)
        .collect()
        .get_column("pago_bruto")
        .sum()
    )

    assert mock_siniestros_diag - processed_data < 100

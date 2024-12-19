import polars as pl


def lowercase_columns(df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame | pl.DataFrame:
    return df.rename({column: column.lower() for column in df.collect_schema().names()})


def col_ramo_desc() -> pl.Expr:
    return pl.concat_str(
        pl.col("codigo_op"),
        pl.col("codigo_ramo_op"),
        pl.col("ramo_desc"),
        separator=" - ",
    )

"""Polars data transformations."""

import re

import polars as pl


def pl_df_empty_str_to_null(df: pl.DataFrame) -> pl.DataFrame:
    """Convert all empty str cells to nulls."""
    return df.with_columns(pl.selectors.string().replace({"": None}))


def pl_df_clean_whitespace(df: pl.DataFrame) -> pl.DataFrame:
    """Strip all string cells, and replace consecutive whitespaces to space."""
    return df.with_columns(
        pl.selectors.string().str.replace_all(r"\s+", " ").str.strip_chars()
    )


def pl_df_all_common_str_cleaning(df: pl.DataFrame) -> pl.DataFrame:
    """Do pl_df_empty_str_to_null, pl_df_clean_whitespace."""
    df = pl_df_clean_whitespace(df)
    df = pl_df_empty_str_to_null(df)
    return df  # noqa: RET504


def clean_col_name(input_str: str) -> str:
    """Clean a string to a clean column name."""
    return re.sub(r"[^a-zA-Z0-9]+", "_", input_str).strip("_").lower()

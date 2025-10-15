"""Utility functions for the coin profitability scraper."""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl


def write_tables(df: pl.DataFrame, file_stem: str, output_folder: Path) -> None:
    """Write the DataFrame to CSV and Parquet files."""
    df.write_csv(output_folder / f"{file_stem}.csv")
    df.write_parquet(output_folder / f"{file_stem}.parquet")


def get_datetime_str() -> str:
    """Get the current datetime as a formatted string, safe for filenames."""
    return datetime.now(UTC).strftime("%Y-%m-%d-%H%M%S")

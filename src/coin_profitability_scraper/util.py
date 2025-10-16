"""Utility functions for the coin profitability scraper."""

from datetime import UTC, datetime
from pathlib import Path

import backoff
import fake_useragent
import polars as pl
import requests
from loguru import logger


def write_tables(df: pl.DataFrame, file_stem: str, output_folder: Path) -> None:
    """Write the DataFrame to CSV and Parquet files."""
    output_folder.mkdir(parents=True, exist_ok=True)

    df.write_csv(output_folder / f"{file_stem}.csv")
    df.write_parquet(output_folder / f"{file_stem}.parquet")


def get_datetime_str() -> str:
    """Get the current datetime as a formatted string, safe for filenames."""
    return datetime.now(UTC).strftime("%Y-%m-%d-%H%M%S")


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=60,
    max_tries=10,
    on_backoff=lambda x: logger.warning(f"Retrying download: {x}"),
)
def download_as_bytes(url: str) -> bytes:
    """Download the given URL and return the content as bytes."""
    response = requests.get(
        url,
        headers={"User-Agent": fake_useragent.UserAgent().random},
        timeout=120,
    )
    response.raise_for_status()
    return response.content

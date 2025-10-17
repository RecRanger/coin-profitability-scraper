"""Step 3: Ingest the scraped coin HTML pages from Step 2b."""

from pathlib import Path
from typing import Any, Literal

import dataframely as dy
import polars as pl
from bs4 import BeautifulSoup
from loguru import logger
from tqdm import tqdm

from coin_profitability_scraper.data_util import pl_df_all_common_str_cleaning
from coin_profitability_scraper.minerstat.step_2b_scrape_each_coin_page import (
    step_2b_output_folder_path,
)

step_3_output_folder = Path("./out/minerstat/") / Path(__file__).stem

_default_string_kwargs: dict[Literal["min_length", "max_length"], int] = {
    "min_length": 1,
    "max_length": 200,
}


class DySchemaMinerstatCoins(dy.Schema):
    """Schema for minerstat_coins table."""

    coin_slug = dy.String(primary_key=True, nullable=False, **_default_string_kwargs)
    reported_algorithm = dy.String(nullable=True, **_default_string_kwargs)
    reported_difficulty = dy.String(nullable=True, **_default_string_kwargs)
    reported_block_reward = dy.String(nullable=True, **_default_string_kwargs)
    reported_volume = dy.String(nullable=True, **_default_string_kwargs)
    reported_founded = dy.String(nullable=True, **_default_string_kwargs)
    reported_network_hashrate = dy.String(nullable=True, **_default_string_kwargs)
    reported_revenue = dy.String(nullable=True, **_default_string_kwargs)
    reported_block_dag = dy.String(nullable=True, **_default_string_kwargs)
    reported_block_epoch = dy.String(nullable=True, **_default_string_kwargs)
    volume_usd = dy.UInt64(nullable=True)

    @dy.rule()
    def _volume_usd_parsed_correctly() -> pl.Expr:
        """`reported_volume` must be null or non-null the same as `volume_usd`."""
        return (
            pl.col("reported_volume")
            .is_null()
            .eq_missing(pl.col("volume_usd").is_null())
        )


def _extract_key_value_pairs(soup: BeautifulSoup) -> dict[str, str]:
    """Extract key-value pairs from a <table> section (e.g., Founded, Algorithm, etc.).

    Has unit test.
    """
    result: dict[str, str] = {}

    # Find all rows in the table
    for row in soup.select("table tr"):
        label_cell = row.find("td", class_="label")
        value_cell = row.find("td", class_="value")

        if not label_cell or not value_cell:
            continue  # skip rows that don't follow the label/value structure

        # Extract the key from the label cell's class list
        # (e.g., "algorithm", "difficulty", "block_reward", etc.)
        classes: Any | list[Any] = label_cell.get("class") or []
        key = next((cls for cls in classes if cls not in {"label", "coin_type"}), None)
        if not key:
            continue

        # Extract the text or linked value
        value = value_cell.get_text(strip=True)

        result[key] = value

    return result


def _ingest_coin_page(html_content: str, *, coin_slug: str) -> dict[str, Any]:
    """Ingest the HTML content of a coin page from Minerstat.

    Main field(s) of interest:
        - Coin name.
        - Date founded.
        - Hash algorithm.
        - Hash algorithm slug.
        - Volume could be interesting.
    """
    soup = BeautifulSoup(html_content, "html.parser")

    return {
        "coin_slug": coin_slug,
    } | {"reported_" + k: v for k, v in _extract_key_value_pairs(soup=soup).items()}


def main() -> None:
    """Ingest each coin page from Minerstat."""
    input_html_file_list = sorted(step_2b_output_folder_path.glob("*.html"))
    logger.info(f"Found {len(input_html_file_list)} HTML files to ingest.")

    data: list[dict[str, Any]] = []
    for input_html_file in tqdm(input_html_file_list):
        html_content = input_html_file.read_text()
        coin_data = _ingest_coin_page(html_content, coin_slug=input_html_file.stem)
        logger.debug(f"Ingested coin data: {coin_data}")

        data.append(coin_data)

    df = pl.DataFrame(data)

    df = pl_df_all_common_str_cleaning(df)

    df = df.with_columns(
        volume_usd=(
            pl.col("reported_volume")
            .str.replace_all(" USD", "", literal=True)
            .str.replace_all(",", "", literal=True)
            .cast(pl.Float64)
            .round()
            .cast(pl.UInt64)
        ),
    )

    df = DySchemaMinerstatCoins.validate(df, cast=True)

    step_3_output_folder.mkdir(parents=True, exist_ok=True)
    df.write_parquet(step_3_output_folder / "minerstat_coins.parquet")


if __name__ == "__main__":
    main()

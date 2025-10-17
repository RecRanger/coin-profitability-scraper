"""Step 1: Scrape all data from https://www.crypto51.app table.

This is a coins list primarily.
"""

from pathlib import Path

import dataframely as dy
import polars as pl
from bs4 import BeautifulSoup
from loguru import logger

from coin_profitability_scraper.data_util import pl_df_all_common_str_cleaning
from coin_profitability_scraper.util import download_as_bytes

_URL = "https://www.crypto51.app"

crypto51_step1_output_path = Path("./out/crypto51/") / Path(__file__).stem
output_parquet_file = crypto51_step1_output_path / "crypto51_coins.parquet"


class DySchemaCrypto51Coins(dy.Schema):
    """Schema for `crypto51_coins` table."""

    coin_name = dy.String(
        primary_key=True, nullable=False, min_length=3, max_length=100
    )
    coin_symbol = dy.String(nullable=False, min_length=1, max_length=100)
    algorithm = dy.String(nullable=False, min_length=3, max_length=100)
    reported_market_cap = dy.String(nullable=True, min_length=3, max_length=100)
    reported_hash_rate = dy.String(nullable=True, min_length=3, max_length=100)
    reported_1h_attack_cost = dy.String(nullable=True, min_length=1, max_length=100)
    reported_nicehash_capability_percent = dy.String(
        nullable=True, min_length=1, max_length=100
    )
    url = dy.String(nullable=False, min_length=20, max_length=500)
    coin_slug = dy.String(nullable=False, min_length=1, max_length=100)


def _extract_table_data(page_html: str) -> list[dict[str, str | None]]:
    soup = BeautifulSoup(page_html, "html.parser")

    # Find the table.
    table = soup.find("table", class_="table")
    assert table is not None

    # Extract the table rows.
    table_header: list[str] = [
        th.get_text(strip=True)
        for th in table.find("tr").find_all("th")  # pyright: ignore[reportOptionalMemberAccess]
    ]
    data: list[dict[str, str | None]] = []
    for row in table.find_all("tr")[1:]:  # Skip header row.
        row_data: dict[str, str | None] = {}
        for idx, cell in enumerate(row.find_all("td")):
            # TODO: if idx == 1, could extract the a tag info to get the slug
            if idx == 0:
                # Special handling for first column with image and link.
                link = cell.find("a")
                if link and "href" in link.attrs:
                    row_data["link_href"] = str(link["href"]) if link["href"] else None

            row_data[table_header[idx]] = cell.get_text(strip=True)
        data.append(row_data)
    return data


def main() -> None:
    """Scrape and parse coins list."""
    crypto51_step1_output_path.mkdir(parents=True, exist_ok=True)

    # Download the page content.
    page_contents = download_as_bytes(_URL)
    (crypto51_step1_output_path / "crypto51_page.html").write_bytes(page_contents)
    logger.info(f"Downloaded page from {_URL} - {len(page_contents):,} bytes.")

    page_html = page_contents.decode("utf-8")

    data_list = _extract_table_data(page_html)
    logger.debug(f"Extracted data list with {len(data_list)} items.")

    df = pl.DataFrame(data_list)
    df = pl_df_all_common_str_cleaning(df)
    df = df.with_columns(pl.selectors.string().replace({"None": None}))

    df = df.select(
        coin_name=pl.col("Name"),
        coin_symbol=pl.col("Symbol"),
        algorithm=pl.col("Algorithm"),
        reported_market_cap=pl.col("Market Cap"),
        reported_hash_rate=pl.col("Hash Rate"),
        reported_1h_attack_cost=pl.col("1h Attack Cost"),
        reported_nicehash_capability_percent=pl.col("NiceHash-able"),
        url=(pl.lit("https://www.crypto51.app/") + pl.col("link_href")),
        coin_slug=(
            pl.col("link_href")
            .str.replace(".html", "", literal=True)
            .str.split("/")
            .list[-1]
        ),
    )

    df = DySchemaCrypto51Coins.validate(df, cast=True)
    df.write_parquet(output_parquet_file)

    logger.success("Coins list scraping completed.")


if __name__ == "__main__":
    main()

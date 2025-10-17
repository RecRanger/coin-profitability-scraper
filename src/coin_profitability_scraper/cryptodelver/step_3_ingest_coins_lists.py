"""Step 3: Ingest the scraped coin HTML pages from Step 1."""

from pathlib import Path
from typing import Any, Literal

import dataframely as dy
import polars as pl
from bs4 import BeautifulSoup
from loguru import logger
from tqdm import tqdm

from coin_profitability_scraper.cryptodelver.step_1_scrape_coins_lists import (
    cryptodelver_step1_output_path,
)
from coin_profitability_scraper.data_util import (
    clean_col_name,
    pl_df_all_common_str_cleaning,
)

cryptodelver_step_3_output_folder = Path("./out/cryptodelver/") / Path(__file__).stem
output_parquet_path = cryptodelver_step_3_output_folder / "cryptodelver_coins.parquet"

_default_string_kwargs: dict[Literal["min_length", "max_length"], int] = {
    "min_length": 1,
    "max_length": 200,
}


class DySchemaCryptodelverCoins(dy.Schema):
    """Schema for cryptodelver_coins table."""

    coin_slug = dy.String(primary_key=True, nullable=False, **_default_string_kwargs)
    coin_name = dy.String(nullable=False, **_default_string_kwargs)
    algo_name = dy.String(nullable=True, **_default_string_kwargs)
    algo_slug = dy.String(nullable=True, **_default_string_kwargs)
    reported_proof_type = dy.String(nullable=True, **_default_string_kwargs)
    reported_market_cap = dy.String(nullable=True, **_default_string_kwargs)
    reported_price_usd = dy.String(nullable=True, **_default_string_kwargs)
    reported_volume = dy.String(nullable=True, **_default_string_kwargs)
    reported_pct_change_24h = dy.Float64(nullable=True)
    reported_pct_change_7d = dy.Float64(nullable=True)
    volume_usd = dy.UInt64(nullable=True)
    market_cap_usd = dy.UInt64(nullable=True)
    coin_url = dy.String(nullable=False, min_length=20, max_length=500)
    algo_url = dy.String(nullable=True, min_length=20, max_length=500)

    @dy.rule()
    def _volume_usd_parsed_correctly() -> pl.Expr:
        """`reported_volume` must be null/non-null the same as `volume_usd`."""
        return pl.col("reported_volume").is_null() == pl.col("volume_usd").is_null()

    @dy.rule()
    def _market_cap_usd_parsed_correctly() -> pl.Expr:
        """`reported_market_cap` must be null/non-null the same as `market_cap_usd`."""
        return (
            pl.col("reported_market_cap").is_null()
            == pl.col("market_cap_usd").is_null()
        )

    @dy.rule()
    def _algo_name_and_slug_present_together() -> pl.Expr:
        """`algo_name` and `algo_slug` must both be null or both be non-null."""
        return pl.col("algo_name").is_null() == pl.col("algo_slug").is_null()

    @dy.rule()
    def _algo_name_and_url_present_together() -> pl.Expr:
        """`algo_name` and `algo_url` must both be null or both be non-null."""
        return pl.col("algo_name").is_null() == pl.col("algo_url").is_null()


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
            header_name = table_header[idx]

            # Special handling for first column with image and link.
            link = cell.find("a")
            if link and "href" in link.attrs:
                row_data[f"{header_name}_href"] = (
                    str(link["href"]) if link["href"] else None
                )

            row_data[header_name] = cell.get_text(strip=True)
        data.append(row_data)
    return data


def main() -> None:
    """Ingest each coin page from Cryptodelver."""
    cryptodelver_step_3_output_folder.mkdir(parents=True, exist_ok=True)

    input_html_file_list = sorted(cryptodelver_step1_output_path.glob("*.html"))
    logger.info(f"Found {len(input_html_file_list)} HTML files to ingest.")

    data: list[dict[str, Any]] = []
    for input_html_file in tqdm(input_html_file_list):
        html_content = input_html_file.read_text()
        coin_data = _extract_table_data(html_content)
        logger.debug(
            f"Ingested coin data: {len(coin_data)} rows from {input_html_file.name}"
        )
        data.extend(coin_data)

    df = pl.DataFrame(data, infer_schema_length=None)
    df = pl_df_all_common_str_cleaning(df)
    df = df.rename(clean_col_name)
    df = df.with_columns(pl.selectors.string().replace({"None": None}))

    df = df.select(
        coin_name=pl.col("name"),
        algo_name=pl.col("algo").replace({"N/A": None}),
        reported_proof_type=pl.col("prooftype"),
        reported_market_cap=pl.col("market_cap"),
        reported_price_usd=pl.col("price_usd"),
        reported_volume=pl.col("volume_24h"),
        reported_pct_change_24h=pl.col("change_24h").cast(pl.Float64).round(8),
        reported_pct_change_7d=pl.col("change_7d").cast(pl.Float64).round(8),
        coin_slug=pl.col("name_href").str.split("/").list.get(-1),
        algo_slug=pl.col("algo_href").str.extract(r"/algorithm/([^/]+)"),
        coin_url=(
            (pl.lit("https://cryptodelver.com/") + pl.col("name_href"))
            .str.replace_all("//", "/", literal=True)
            .str.replace_all(":/", "://", literal=True)  # Replace back the https.
        ),
        algo_url=(
            (
                pl.lit("https://cryptodelver.com/")
                + pl.col("algo_href").replace({"/assets/": None, "/algorithm/": None})
            )
            .str.replace_all("//", "/", literal=True)
            .str.replace_all(":/", "://", literal=True)  # Replace back the https.
        ),
    )

    df = df.with_columns(
        volume_usd=(
            pl.col("reported_volume")
            .str.replace_all("$", "", literal=True)
            .str.replace_all(",", "", literal=True)
            .cast(pl.Float64)
            .round()
            .cast(pl.UInt64)
        ),
        market_cap_usd=(
            pl.col("reported_market_cap")
            .str.replace_all("$", "", literal=True)
            .str.replace_all(",", "", literal=True)
            .cast(pl.Float64)
            .round()
            .cast(pl.UInt64)
        ),
    )

    df.write_parquet(
        cryptodelver_step_3_output_folder / "preview_cryptodelver_coins.parquet"
    )

    # Remove duplicates. Just bad data. Keep current sort order (by pages).
    logger.info(f"DataFrame before deduplication: {df.height} rows.")
    df = df.unique(subset=["coin_slug"], keep="first", maintain_order=True)
    logger.info(f"DataFrame after deduplication: {df.height} rows.")

    df = DySchemaCryptodelverCoins.validate(df, cast=True)

    df.write_parquet(output_parquet_path)


if __name__ == "__main__":
    main()

"""Step 1: Scrape all data from https://wheretomine.io/ table via JSON page.

This is a coins list primarily.
"""

from pathlib import Path

import dataframely as dy
import orjson
import polars as pl
from loguru import logger

from coin_profitability_scraper.data_util import pl_df_all_common_str_cleaning
from coin_profitability_scraper.util import download_as_bytes

_URL = "https://wheretomine.io/page-data/all/page-data.json"

wheretomine_step1_output_path = Path("./out/wheretomine/") / Path(__file__).stem
output_parquet_file = wheretomine_step1_output_path / "wheretomine_coins.parquet"


class DySchemaWheretomineCoins(dy.Schema):
    """Schema for `wheretomine_coins` table."""

    coin_name = dy.String(
        primary_key=True, nullable=False, min_length=1, max_length=100
    )
    coin_abbreviation = dy.String(nullable=False, min_length=1, max_length=20)
    coin_slug = dy.String(nullable=False, min_length=1, max_length=100)
    description = dy.String(nullable=True, min_length=10, max_length=5000)
    website_link = dy.String(nullable=True, max_length=500)
    explorer_link = dy.String(nullable=True, max_length=500)

    subpool_count = dy.UInt32(nullable=False)

    # Basic numeric stats
    network_hashrate = dy.Float64(nullable=False)
    network_difficulty = dy.Float64(nullable=False)
    block_reward = dy.Float64(nullable=False)
    block_time = dy.Float64(nullable=False)
    height = dy.Integer(nullable=False)
    price_btc = dy.Float64(nullable=False)
    price_usd = dy.Float64(nullable=False)
    volume_24h = dy.Float64(nullable=False)
    percentage_change_24h = dy.Float64(nullable=False)
    market_cap = dy.Float64(nullable=False)

    # Status and boolean flags
    status = dy.String(nullable=True, min_length=3, max_length=50)
    is_gathering = dy.Bool(nullable=False)
    is_lagging = dy.Bool(nullable=False)
    is_testing = dy.Bool(nullable=False)
    is_listed = dy.Bool(nullable=False)
    is_sponsored = dy.Bool(nullable=False)
    has_calculator = dy.Bool(nullable=False)

    # Metadata and general info
    reported_last_updated = dy.Datetime(nullable=False)

    algorithm_name = dy.String(nullable=False, min_length=2, max_length=100)
    algorithm_type = dy.String(nullable=False, min_length=2, max_length=100)
    algorithm_slug = dy.String(nullable=False, min_length=2, max_length=100)

    links_json = dy.String(nullable=True, min_length=2, max_length=3000)


def main() -> None:
    """Scrape and parse coins list."""
    wheretomine_step1_output_path.mkdir(parents=True, exist_ok=True)

    # Download the page content.
    page_contents = download_as_bytes(_URL)
    (wheretomine_step1_output_path / "wheretomine_coins.json").write_bytes(
        page_contents
    )
    logger.info(f"Downloaded page from {_URL} - {len(page_contents):,} bytes.")
    (wheretomine_step1_output_path / "wheretomine_coins_pretty.json").write_bytes(
        orjson.dumps(orjson.loads(page_contents), option=orjson.OPT_INDENT_2)
    )

    data = orjson.loads(page_contents)
    data_list = data["result"]["pageContext"]["coins"]
    assert isinstance(data_list, list)

    df = pl.DataFrame(data_list)
    df = pl_df_all_common_str_cleaning(df)

    df.write_parquet(
        wheretomine_step1_output_path / "preview_wheretomine_coins.parquet"
    )

    df = df.select(
        coin_name=pl.col("name"),
        coin_abbreviation=pl.col("abbreviation"),
        coin_slug=pl.col("slug"),
        description=pl.col("description"),
        website_link=pl.col("website"),
        explorer_link=pl.col("explorer"),
        # Basic numeric stats
        network_hashrate=pl.col("networkHashrate"),
        network_difficulty=pl.col("networkDifficulty"),
        block_reward=pl.col("blockReward"),
        block_time=pl.col("blockTime"),
        height=pl.col("height"),
        price_btc=pl.col("priceBTC"),
        price_usd=pl.col("priceUSD"),
        volume_24h=pl.col("volume24h"),
        percentage_change_24h=pl.col("percentageChange24h"),
        market_cap=pl.col("marketCap"),
        subpool_count=pl.col("numberOfSubPools"),
        # Status and boolean flags
        status=pl.col("status"),
        is_gathering=pl.col("isGathering"),
        is_lagging=pl.col("isLagging"),
        is_testing=pl.col("isTesting"),
        is_listed=pl.col("isListed"),
        is_sponsored=pl.col("isSponsored"),
        has_calculator=pl.col("hasCalculator"),
        # Metadata
        reported_last_updated=pl.col("lastUpdated").cast(pl.Datetime),
        # Algorithm fields (flattened)
        algorithm_name=pl.col("algorithm").struct.field("name"),
        algorithm_type=pl.col("algorithm").struct.field("type"),
        algorithm_slug=pl.col("algorithm").struct.field("slug"),
        # JSON string of links. Could flatten/unpivot into "reddit_link", etc.
        links_json=(
            pl.col("links")
            .map_elements(lambda x: orjson.dumps(x.to_list()), return_dtype=pl.Binary)
            .cast(pl.String)
        ),
    )

    df = DySchemaWheretomineCoins.validate(df, cast=True)
    df.write_parquet(output_parquet_file)

    logger.success("Coins list scraping completed.")


if __name__ == "__main__":
    main()

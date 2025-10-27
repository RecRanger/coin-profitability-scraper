"""Step 2: Parse the coin information from the API."""

from pathlib import Path
from typing import Any

import dataframely as dy
import orjson
import polars as pl
from dataframely.exc import RuleValidationError
from loguru import logger

from coin_profitability_scraper.data_util import pl_df_all_common_str_cleaning
from coin_profitability_scraper.whattomine.step_1_api_fetch import (
    coins_api_data_json_path,
)

step_2_output_folder = Path("./out/whattomine/step_2_coins_list/")
output_parquet_file = step_2_output_folder / "whattomine_coins.parquet"


class DySchemaWhattomineCoins(dy.Schema):
    """Schema for whattomine_coins table."""

    whattomine_id = dy.Int64(primary_key=True, nullable=False)
    # `coin_name` is almost unique, but just not quite.
    coin_name = dy.String(nullable=False, min_length=2, max_length=100)
    algorithm = dy.String(nullable=False, min_length=2, max_length=100)
    algo_param_name = dy.String(nullable=True, min_length=1, max_length=10)

    tag = dy.String(nullable=False, min_length=2, max_length=100)  # Like a ticker.

    is_lagging = dy.Bool(nullable=False)
    is_testing = dy.Bool(nullable=False)
    last_update = dy.Datetime(nullable=False)
    network_hashrate = dy.Float64(nullable=False)
    last_block = dy.Int64(nullable=False)
    block_time = dy.String(nullable=False, min_length=2, max_length=100)
    market_cap_usd = dy.Float64(nullable=False)

    # Extra data.
    block_reward = dy.Float64(nullable=False)
    difficulty = dy.Float64(nullable=False)

    # Exclude - block_reward24 = dy.Float64(nullable=False)
    # Exclude - block_reward3 = dy.Float64(nullable=False)
    # Exclude - block_reward7 = dy.Float64(nullable=False)
    # Exclude - block_reward14 = dy.Float64(nullable=False)
    # Exclude - block_reward30 = dy.Float64(nullable=False)
    # Exclude - difficulty24 = dy.Float64(nullable=False)
    # Exclude - difficulty3 = dy.Float64(nullable=False)
    # Exclude - difficulty7 = dy.Float64(nullable=False)
    # Exclude - difficulty14 = dy.Float64(nullable=False)
    # Exclude - difficulty30 = dy.Float64(nullable=False)
    # Exclude - difficulty_change24 = dy.Float64(nullable=False)

    # Detailed price info from a list of exchanges.
    # Note that the units of this data aren't super clear, and some/all may be measured
    # in bitcoins.
    exchanges_json = dy.String(nullable=False, min_length=2, max_length=10_000)


def load_coin_list_df(coins_api_data: list[dict[str, Any]]) -> pl.DataFrame:
    """Load the coin list from the API data."""
    df = pl.DataFrame(coins_api_data)
    logger.debug(f"Raw coin data from API: {df.height:,} rows")

    df = df.select(
        coin_name=pl.col("name"),
        algorithm=pl.col("algorithm"),
        algo_param_name=pl.col("algo_param_name"),
        whattomine_id=pl.col("id"),
        tag=pl.col("tag"),
        is_lagging=pl.col("lagging"),
        is_testing=pl.col("testing"),
        last_update=pl.col("last_update").cast(pl.Datetime),
        network_hashrate=pl.col("nethash"),
        last_block=pl.col("last_block"),
        block_time=pl.col("block_time"),
        market_cap_usd=pl.col("market_cap"),
        block_reward=pl.col("block_reward"),
        difficulty=pl.col("difficulty"),
        exchanges_json=(
            pl.col("exchanges")
            .map_elements(
                lambda list_of_dicts_of_exchanges: orjson.dumps(
                    list_of_dicts_of_exchanges.to_list()
                ),
                return_dtype=pl.Binary,
            )
            .cast(pl.String)
        ),
    )
    df = df.sort("market_cap_usd", descending=True, nulls_last=True)
    logger.info(f"Coin List: {df}")

    return df


def main() -> None:
    """Parse HTML files and extract coin information."""
    logger.info("Starting")
    step_2_output_folder.mkdir(parents=True, exist_ok=True)

    coins_api_data: list[dict[str, Any]] = orjson.loads(
        coins_api_data_json_path.read_bytes()
    )

    df = load_coin_list_df(coins_api_data)

    df = pl_df_all_common_str_cleaning(df)

    try:
        df = DySchemaWhattomineCoins.validate(df, cast=True)
    except RuleValidationError:
        _, invalid_info = DySchemaWhattomineCoins.filter(df, cast=True)
        invalid_info.write_parquet(step_2_output_folder / "invalid_schema.pq")
        raise

    logger.info(
        f"Total market cap of {len(df):,} coins here: "
        f"US${df['market_cap_usd'].sum() / 1_000_000_000_000:.3f}T"
    )

    logger.info("Wrote coin list table.")

    step_2_output_folder.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_parquet_file)


if __name__ == "__main__":
    main()

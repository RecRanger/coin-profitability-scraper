"""Step 3: Generate a report summarizing the parsed coin data."""

import json
from pathlib import Path

import polars as pl
from loguru import logger

from coin_profitability_scraper.crypto_slate.step_2_parse_scrape import (
    step_2_output_folder,
)
from coin_profitability_scraper.util import get_datetime_str, write_tables

step_3_output_folder = Path("./out/crypto_slate/step_3_algo_report/")


def summarize_by_algo(df_coins: pl.DataFrame) -> pl.DataFrame:
    """Summarize the coins by their hashing algorithm."""
    df_algos = df_coins.group_by("hash_algo", maintain_order=True).agg(
        market_cap=pl.sum("market_cap"),
        coin_count=pl.len(),
        coin_list=pl.col("coin_name"),
        earliest_year=pl.min("earliest_year"),
    )
    df_algos = (
        df_algos.with_columns(
            market_cap_per_coin=(pl.col("market_cap") / pl.col("coin_count")).round(),
        )
        .with_columns(
            algo_sort_order=(
                pl.when(
                    pl.col("hash_algo").is_in([None, "None", "Defunct"])
                    | pl.col("hash_algo").is_null()
                )
                .then(pl.lit(0))
                .otherwise(pl.col("market_cap"))
            ),
            algo_sort_order_per_coin=(
                pl.when(
                    pl.col("hash_algo").is_in([None, "None", "Defunct"])
                    | pl.col("hash_algo").is_null()
                )
                .then(pl.lit(0))
                .otherwise(pl.col("market_cap_per_coin"))
            ),
        )
        .sort("algo_sort_order", descending=True)
    )
    df_algos = df_algos.with_columns(
        coin_list=(
            pl.when(pl.col("coin_list").list.len() > pl.lit(14))
            .then(
                pl.lit("TRUNCATED\n")
                + pl.col("coin_list").list.head(15).list.join("\n")
                + pl.lit("\n...")
            )
            .otherwise(pl.col("coin_list").list.join("\n"))
        ),
        coin_list_json=(
            pl.col("coin_list")
            .list.head(200)
            .map_elements(lambda x: json.dumps(x.to_list()), return_dtype=pl.String)
        ),
    )
    logger.info(f"Top algos by market cap:\n{df_algos.head(10)}")

    return df_algos


def main() -> None:
    """Generate a report summarizing the parsed coin data."""
    latest_source = sorted(step_2_output_folder.glob("coin_list_*.parquet"))[-1]
    logger.info(f"Loading latest coin list from {latest_source.name}")

    df_coins = pl.read_parquet(latest_source)

    logger.info(
        f"Loaded {len(df_coins)} coins from {df_coins['filename'].min()} to "
        f"{df_coins['filename'].max()}"
    )

    df_algos = summarize_by_algo(df_coins)

    write_tables(
        df_algos,
        f"coin_algos_{get_datetime_str()}_{len(df_algos)}algos",
        step_3_output_folder,
    )
    logger.info("Wrote coin algos table.")

    logger.info("Finished main()")


if __name__ == "__main__":
    main()

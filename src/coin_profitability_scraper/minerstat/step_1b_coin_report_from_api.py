"""Step 1b."""

import json
import os
from pathlib import Path

import orjson
import polars as pl
from loguru import logger

from coin_profitability_scraper.util import (
    download_as_bytes,
    get_datetime_str,
    write_tables,
)

step_1b_output_folder = Path("./out/minerstat/") / Path(__file__).stem


def summarize_by_algo(df_coins: pl.DataFrame) -> pl.DataFrame:
    """Summarize the coins by their hashing algorithm."""
    df_algos = df_coins.group_by("algorithm", maintain_order=True).agg(
        volume=pl.sum("volume"),
        coin_count=pl.len(),
        coin_list=pl.col("coin"),
    )
    df_algos = (
        df_algos.with_columns(
            volume_per_coin=(pl.col("volume") / pl.col("coin_count")).round(),
        )
        .with_columns(
            algo_sort_order=(
                pl.when(
                    pl.col("algorithm").is_in([None, "None", "Defunct"])
                    | pl.col("algorithm").is_null()
                )
                .then(pl.lit(0))
                .otherwise(pl.col("volume"))
            ),
            algo_sort_order_per_coin=(
                pl.when(
                    pl.col("algorithm").is_in([None, "None", "Defunct"])
                    | pl.col("algorithm").is_null()
                )
                .then(pl.lit(0))
                .otherwise(pl.col("volume_per_coin"))
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
    logger.info(f"Starting {Path(__file__).name} main()")

    step_1b_output_folder.mkdir(parents=True, exist_ok=True)

    api_key = os.environ["MINERSTAT_API_KEY"]
    assert api_key
    data = download_as_bytes(f"https://api.minerstat.com/v2/coins?key={api_key}")
    logger.info(f"Downloaded {len(data):,} bytes of coin data from Minerstat API.")
    assert len(data) > 1_000  # noqa: PLR2004

    # Main data output used in subsequent steps: "minerstat_coins.json".
    (step_1b_output_folder / "minerstat_coins.json").write_bytes(data)
    logger.info("Wrote raw Minerstat coin data to JSON file.")

    data_list = orjson.loads(data)["data"]
    (step_1b_output_folder / "minerstat_coins.pretty.json").write_bytes(
        orjson.dumps(orjson.loads(data), option=orjson.OPT_INDENT_2)
    )
    logger.info(f"Parsed {len(data_list)} coins from Minerstat API.")
    # FIXME: Only loads the first 10 coins now because of the free API limit.

    df_coins = pl.DataFrame(
        data_list,
        schema={
            "id": pl.String,
            "coin": pl.String,
            "name": pl.String,
            "type": pl.String,
            "algorithm": pl.String,
            "network_hashrate": pl.Float64,
            "difficulty": pl.Int64,
            "reward": pl.Float64,
            "reward_unit": pl.String,
            "reward_block": pl.Int64,
            "price": pl.Float64,
            "volume": pl.Float64,
            "updated": pl.Int64,
        },
    )

    df_algos = summarize_by_algo(df_coins)

    write_tables(
        df_algos,
        f"coin_algos_{get_datetime_str()}_{len(df_algos)}algos",
        step_1b_output_folder,
    )
    logger.info("Wrote coin algos table.")

    logger.info("Finished main()")


if __name__ == "__main__":
    main()

"""Step 3: Normalize the miners data (ASICs and GPUs) from the API."""

from pathlib import Path
from typing import Any

import dataframely as dy
import orjson
import polars as pl
from dataframely.exc import RuleValidationError
from loguru import logger

from coin_profitability_scraper.data_util import pl_df_all_common_str_cleaning
from coin_profitability_scraper.whattomine.step_1_api_fetch import (
    asics_api_data_json_path,
    gpus_api_data_json_path,
)

step_3_output_folder = Path("./out/whattomine/step_3_miners_list/")
output_parquet_file = step_3_output_folder / "whattomine_miners.parquet"


class DySchemaWhattomineMiners(dy.Schema):
    """Schema for `whattomine_miners` table.

    Each row is a miner-x-algorithm performance report.
    """

    miner_algorithm_id = dy.String(
        primary_key=True, nullable=False, min_length=5, max_length=200
    )

    whattomine_miner_id = dy.UInt32(nullable=False)
    miner_name = dy.String(nullable=False, min_length=2, max_length=200)
    release_date = dy.Date(nullable=True)
    miner_type = dy.Enum(["GPU", "ASIC"], nullable=False)

    # Data for each algorithm.
    algorithm_name = dy.String(nullable=False, min_length=2, max_length=100)
    reported_hashrate = dy.String(nullable=False, min_length=2, max_length=100)
    power_watts = dy.UInt32(nullable=True)
    hashrate_hashes_per_second = dy.UInt64(nullable=False)


def load_miner_types_df(
    asics_list: list[dict[str, Any]], gpus_list: list[dict[str, Any]]
) -> pl.DataFrame:
    """Load the coin list from the API data."""
    df = pl.concat(
        [
            pl.DataFrame(asics_list, infer_schema_length=None).with_columns(
                miner_type=pl.lit("ASIC"),
            ),
            pl.DataFrame(gpus_list, infer_schema_length=None).with_columns(
                miner_type=pl.lit("GPU"),
            ),
        ]
    )
    logger.debug(f"Raw miners data from API: {df.height:,} rows")
    df = pl_df_all_common_str_cleaning(df)

    df = df.select(
        whattomine_miner_id=pl.col("id"),
        miner_name=pl.col("name"),
        release_date=pl.col("release_date").cast(pl.Date),
        miner_type=pl.col("miner_type"),
        algorithm=pl.col("algorithms"),  # List-of-structs.
    )

    df = df.explode("algorithm")
    logger.info(f"Expanded each algorithm to rows: {df.height:,} rows")

    df = df.with_columns(
        algorithm_name=pl.col("algorithm").struct["name"],
        reported_hashrate=pl.col("algorithm").struct["hashrate"],  # String.
        power_watts=pl.col("algorithm").struct["power"],
    ).drop("algorithm")

    df = df.with_columns(
        hashrate_hashes_per_second=(
            pl.col("reported_hashrate").cast(pl.Float64).round().cast(pl.UInt64)
        ),
        miner_algorithm_id=(
            pl.col("miner_name") + pl.lit(" - ") + pl.col("algorithm_name")
        ),
    )

    logger.info(f"Loaded shape: {df.shape}")

    return df


def main() -> None:
    """Parse HTML files and extract coin information."""
    logger.info("Starting")
    step_3_output_folder.mkdir(parents=True, exist_ok=True)

    df = load_miner_types_df(
        asics_list=orjson.loads(asics_api_data_json_path.read_bytes()),
        gpus_list=orjson.loads(gpus_api_data_json_path.read_bytes()),
    )

    df = pl_df_all_common_str_cleaning(df)

    # Deterministically remove duplicates. These are primarily cases where the same
    # miner (esp. GPU) has hashrates reported at different power settings.
    df = df.sort(
        [
            "miner_algorithm_id",
            pl.col("reported_hashrate").cast(pl.Float64),  # Main decider.
            pl.col("power_watts"),  # Not necessary.
        ]
    ).unique(
        # Include so many to throw off the primary key check at the end if there
        # are discrepancies in the data generating the keys.
        ["miner_algorithm_id", "miner_type", "miner_name", "algorithm_name"],
        maintain_order=True,
        keep="last",  # After sort, keep the LARGEST reported hashrate.
    )

    try:
        df = DySchemaWhattomineMiners.validate(df, cast=True)
    except RuleValidationError:
        _, invalid_info = DySchemaWhattomineMiners.filter(df, cast=True)
        invalid_info.write_parquet(step_3_output_folder / "invalid_schema.pq")
        raise

    step_3_output_folder.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_parquet_file)

    logger.info(f"Wrote miners list table: {df.height:,} miners")


if __name__ == "__main__":
    main()

"""Prepare algorithm list from MiningNow.com."""

from pathlib import Path

import dataframely as dy
import orjson
import polars as pl
from loguru import logger

from coin_profitability_scraper.data_util import pl_df_all_common_str_cleaning
from coin_profitability_scraper.miningnow.step_1_scrape_data import (
    miningnow_step1_output_path,
)

output_parquet_path = Path("./out/miningnow/") / "miningnow_algorithms.parquet"


class DySchemaMiningnowAlgorithms(dy.Schema):
    """Schema for miningnow_algorithms table."""

    algorithm_name = dy.String(
        primary_key=True, nullable=False, min_length=1, max_length=100
    )
    algorithm_slug = dy.String(nullable=False, min_length=1, max_length=100)

    @dy.rule(group_by=["algorithm_slug"])
    def _algorithm_slug_unique() -> pl.Expr:
        """Ensure algorithm_slug is unique."""
        return pl.len() == 1


def _fetch_algos_data_json_df() -> pl.DataFrame:
    """Prepare algorithms list from MiningNow.com."""
    data = orjson.loads((miningnow_step1_output_path / "algos_data.json").read_bytes())
    logger.info(f"Loaded data for {len(data)} algos from algos_data.json.")

    df = pl.DataFrame(data)
    df = df.select(
        algorithm_name=pl.col("label"),
        algorithm_slug=pl.col("value"),
    )
    logger.info(f"Prepared coin list with {df.height} entries from algos_data.json.")

    return df


def main() -> None:
    """Prepare algorithms list from MiningNow.com."""
    output_parquet_path.parent.mkdir(parents=True, exist_ok=True)

    df = _fetch_algos_data_json_df()
    df = pl_df_all_common_str_cleaning(df)

    df = DySchemaMiningnowAlgorithms.validate(df, cast=True)

    df.write_parquet(output_parquet_path)
    logger.info(f"Wrote algorithm list: {df.shape}")


if __name__ == "__main__":
    main()

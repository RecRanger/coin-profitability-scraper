"""Step 9: Write to the Dolt database."""

from pathlib import Path

import polars as pl
from loguru import logger

from coin_profitability_scraper.dolt_updater import DoltDatabaseUpdater
from coin_profitability_scraper.dolt_util import DOLT_REPO_URL, upsert_polars_rows
from coin_profitability_scraper.minerstat.step_3_ingest_each_coin_page import (
    step_3_output_folder,
)

table_to_path: dict[str, Path] = {
    "minerstat_coins": (step_3_output_folder / "minerstat_coins.parquet"),
}


def main() -> None:
    """Write data to DoltHub database."""
    with DoltDatabaseUpdater(DOLT_REPO_URL) as dolt:
        for table_name, parquet_path in table_to_path.items():
            df = pl.read_parquet(parquet_path)
            logger.info(f"Loaded {table_name}: {df.shape}")
            upsert_polars_rows(
                engine=dolt.engine,
                table_name=table_name,
                df=df,
                batch_size=10,
            )

        logger.info("Done all upserts.")

        commit_message = "Auto-updated tables: " + ", ".join(table_to_path.keys())
        dolt.dolt_commit_and_push(commit_message=commit_message)
        logger.info("Done commit and push.")

    logger.info("Done main")


if __name__ == "__main__":
    main()

"""Step 9: Write to the Dolt database."""

import sys
from collections.abc import Sequence

import polars as pl
from loguru import logger

from coin_profitability_scraper.dolt_updater import DoltDatabaseUpdater
from coin_profitability_scraper.dolt_util import DOLT_REPO_URL, upsert_polars_rows
from coin_profitability_scraper.tables import TableNameLiteral, table_to_path_and_schema


def main(tables_to_update: Sequence[TableNameLiteral]) -> None:
    """Write data to DoltHub database."""
    logger.info(f"Updating dolt tables: {', '.join(tables_to_update)}")

    with DoltDatabaseUpdater(DOLT_REPO_URL) as dolt:
        for table_name, (parquet_path, dy_schema) in table_to_path_and_schema.items():
            if table_name not in tables_to_update:
                logger.debug(f"Skipping {table_name}")
                continue

            logger.info(f"Loading {table_name}")
            df = pl.read_parquet(parquet_path)
            df = dy_schema.validate(df, cast=True)
            logger.info(f"Loaded {table_name}: {df.shape}")
            upsert_polars_rows(
                engine=dolt.engine,
                table_name=table_name,
                df=df,
            )

        logger.info("Done all upserts.")

        commit_message = "Auto-updated tables: " + ", ".join(tables_to_update)
        dolt.dolt_commit_and_push(commit_message=commit_message)
        logger.info("Done commit and push.")

    logger.info(f"Updated dolt tables: {', '.join(tables_to_update)}")


if __name__ == "__main__":
    main(
        [
            table_name
            for table_name in sys.argv
            if table_name in table_to_path_and_schema
        ]
    )

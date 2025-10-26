"""Step 9: Write to the Dolt database."""

import sys
from collections.abc import Sequence

import backoff
import polars as pl
import sqlalchemy
from loguru import logger

from coin_profitability_scraper.dolt_updater import DoltDatabaseUpdater
from coin_profitability_scraper.dolt_util import DOLT_REPO_URL, upsert_polars_rows
from coin_profitability_scraper.tables import TableNameLiteral, table_to_path_and_schema


@backoff.on_exception(
    backoff.expo,
    Exception,
    max_time=60,
    max_tries=10,
    on_backoff=lambda x: logger.warning(f"Retrying: {x}"),
)
def _push_a_table(table_name: TableNameLiteral) -> None:
    """Push a single table to DoltHub."""
    (parquet_path, dy_schema) = table_to_path_and_schema[table_name]
    with DoltDatabaseUpdater(DOLT_REPO_URL) as dolt:
        logger.info(f"Loading {table_name}")
        df = pl.read_parquet(parquet_path)
        df = dy_schema.validate(df, cast=True)
        logger.info(f"Loaded {table_name}: {df.shape}")
        upsert_polars_rows(
            engine=dolt.engine,
            table_name=table_name,
            df=df,
            # Limit batch_size for certain very-wide tables.
            batch_size={"miningnow_asics": 1, "wheretomine_coins": 1}.get(
                table_name, 500
            ),
        )

        # For certain datasets, also DELETE rows no longer in the upsert content.
        if table_name in {"gold_algorithms"}:
            assert len(dy_schema.primary_keys()) == 1, (
                "Composite primary keys not supported."
            )
            primary_key_column = dy_schema.primary_keys()[0]
            with dolt.engine.begin() as conn:
                result = conn.execute(
                    sqlalchemy.text(
                        f"DELETE FROM {table_name} "  # noqa: S608
                        f"WHERE {primary_key_column} NOT IN :ids"
                    ),
                    {"ids": tuple(df[primary_key_column].to_list())},
                )
                logger.info(f"Pruned {result.rowcount} rows from {table_name}")

        logger.info("Done all upserts.")

        commit_message = f"Auto-updated table: {table_name}"
        dolt.dolt_commit_and_push(commit_message=commit_message)
        logger.info("Done commit and push.")


def main(tables_to_update: Sequence[TableNameLiteral]) -> None:
    """Write data to DoltHub database."""
    logger.info(f"Updating dolt tables: {', '.join(tables_to_update)}")

    for table_name in tables_to_update:
        _push_a_table(table_name)

    logger.info(f"Updated dolt tables: {', '.join(tables_to_update)}")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        logger.info("Select a table to update (or re-run and pass as CLI argument).")
        table_name = input("Table name: ")
        assert table_name in table_to_path_and_schema
        main([table_name])
    else:
        assert all(table_name in table_to_path_and_schema for table_name in sys.argv), (
            f"Unknown tables passed as CLI args: {', '.join(sys.argv)}"
        )
        logger.info(f"Updating tables passed as CLI args: {', '.join(sys.argv)}")

        main(
            [
                table_name
                for table_name in sys.argv
                if table_name in table_to_path_and_schema
            ]
        )

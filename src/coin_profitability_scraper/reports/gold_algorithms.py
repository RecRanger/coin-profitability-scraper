"""Summarize all algorithms."""

from pathlib import Path

import dataframely as dy
import orjson
import polars as pl
from loguru import logger

from coin_profitability_scraper.dolt_updater import DoltDatabaseUpdater
from coin_profitability_scraper.dolt_util import DOLT_REPO_URL
from coin_profitability_scraper.tables import table_to_path_and_schema

output_folder = Path("./out/reports/") / Path(__file__).stem


class DySchemaGoldAlgorithms(dy.Schema):
    """Schema for gold algorithms."""

    algo_name = dy.String(
        primary_key=True, nullable=False, min_length=1, max_length=100
    )
    source_sites_json = dy.String(nullable=False, min_length=1, max_length=100)
    source_tables_json = dy.String(nullable=False, min_length=1, max_length=100)
    coin_count = dy.UInt32(nullable=False)

    earliest_coin_created_at = dy.Datetime(nullable=False)
    latest_coin_created_at = dy.Datetime(nullable=False)

    earliest_coin = dy.String(nullable=False, min_length=1, max_length=100)
    latest_coin = dy.String(nullable=False, min_length=1, max_length=100)


def _fetch_dolt_tables() -> None:
    """Fetch the existing tables in dolt."""
    output_folder.mkdir(parents=True, exist_ok=True)
    logger.info("Starting fetching dolt tables.")

    with DoltDatabaseUpdater(DOLT_REPO_URL) as dolt:
        for table_name in table_to_path_and_schema:
            df = pl.read_database(
                f"SELECT * FROM {table_name}",  # noqa: S608
                connection=dolt.engine,
            )
            logger.info(f"Loaded {table_name}: {df.shape}")

            df.write_parquet(output_folder / f"src_{table_name}.parquet")

        logger.info("Done fetching all tables.")


def _get_stacked_coin_list() -> pl.DataFrame:
    """Stack all coin datasets into a normalized coin list.

    TODO: Could be moved to "silver_coins" table.
    """
    df = pl.concat(
        [
            pl.read_parquet(output_folder / "src_crypto51_coins.parquet").select(
                source_site=pl.lit("crypto51"),
                source_table=pl.lit("crypto51_coins"),
                coin_name=pl.col("coin_name"),
                coin_symbol=pl.col("coin_symbol"),
                algo_name=pl.col("algorithm"),
                market_cap_usd=pl.lit(  # TODO: Data just needs cleaning.
                    None, pl.Int64
                ),
                volume_24h_usd=pl.lit(None, pl.Int64),
                coin_url=pl.col("url"),
                created_at=pl.col("created_at"),
            ),
            pl.read_parquet(output_folder / "src_cryptodelver_coins.parquet").select(
                source_site=pl.lit("cryptodelver"),
                source_table=pl.lit("cryptodelver_coins"),
                coin_name=pl.col("coin_name"),
                coin_symbol=pl.lit(None, pl.String),
                algo_name=pl.col("algo_name"),
                market_cap_usd=pl.col("market_cap_usd"),
                volume_24h_usd=pl.col("volume_usd"),
                coin_url=pl.col("coin_url"),
                created_at=pl.col("created_at"),
            ),
            pl.read_parquet(output_folder / "src_cryptoslate_coins.parquet").select(
                source_site=pl.lit("cryptoslate"),
                source_table=pl.lit("cryptoslate_coins"),
                coin_name=pl.col("coin_name"),
                coin_symbol=pl.col("coin_slug"),
                algo_name=pl.col("hash_algo"),
                market_cap_usd=pl.col("market_cap_usd"),
                volume_24h_usd=pl.lit(None, pl.Int64),
                coin_url=pl.col("url"),
                created_at=pl.col("created_at"),
            ),
            pl.read_parquet(output_folder / "src_minerstat_coins.parquet").select(
                source_site=pl.lit("minerstat"),
                source_table=pl.lit("minerstat_coins"),
                coin_name=pl.col("coin_slug"),
                coin_symbol=pl.col("coin_slug"),
                algo_name=pl.col("reported_algorithm"),
                market_cap_usd=pl.lit(None, pl.Int64),
                volume_24h_usd=pl.col("volume_usd"),
                coin_url=pl.lit(None, pl.String),  # TODO: Available somewhere.
                created_at=pl.col("created_at"),
            ),
        ]
    )

    if 0:  # TODO: Use this once we have algorithms tracked.
        pl.read_parquet(output_folder / "src_miningnow_coins.parquet").select(
            source_site=pl.lit("miningnow"),
            source_table=pl.lit("miningnow_coins"),
            coin_name=pl.col("coin_name"),
            coin_symbol=pl.col("ticker"),
            algo_name=pl.lit(None),  # FIXME: Missing.
            market_cap_usd=pl.lit(None, pl.Int64),
            volume_24h_usd=pl.lit(None, pl.Int64),
            coin_url=pl.lit(None, pl.String),  # TODO: Available somewhere.
            created_at=pl.col("created_at"),
        )

    logger.info(f"Stacked coin list with {df.height:,} entries.")
    return df


def _transform_coin_list_to_gold_algorithms(df_coin_list: pl.DataFrame) -> pl.DataFrame:
    """Transform coin list to algorithm list."""
    df = (
        df_coin_list.filter(pl.col("algo_name").is_not_null())
        .sort(["created_at"])
        .group_by(["algo_name"], maintain_order=True)
        .agg(
            source_sites=pl.col("source_site").unique().sort(),
            source_tables=pl.col("source_table").unique().sort(),
            earliest_coin_created_at=pl.col("created_at").first(),
            latest_coin_created_at=pl.col("created_at").last(),
            # `coin_count` is approximate as names lack normalization.
            coin_count=pl.col("coin_name").n_unique(),
            earliest_coin=(
                (pl.col("coin_name") + pl.lit(" @ ") + pl.col("source_site")).first()
            ),
            latest_coin=(
                (pl.col("coin_name") + pl.lit(" @ ") + pl.col("source_site")).last()
            ),
            # TODO: Add a "has_asic" column.
        )
    )

    df = df.with_columns(
        source_sites_json=(
            pl.col("source_sites")
            .map_elements(lambda x: orjson.dumps(x.to_list()), pl.Binary)
            .cast(pl.String)
        ),
        source_tables_json=(
            pl.col("source_tables")
            .map_elements(lambda x: orjson.dumps(x.to_list()), pl.Binary)
            .cast(pl.String)
        ),
    ).drop("source_sites", "source_tables")

    logger.info(f"Transformed coin list to algorithm list with {df.height:,} entries.")
    return df


def main() -> None:
    """Summarize all algorithms."""
    _fetch_dolt_tables()

    df_coins_stacked = _get_stacked_coin_list()

    df_algorithms = _transform_coin_list_to_gold_algorithms(df_coins_stacked)
    df_algorithms = DySchemaGoldAlgorithms.validate(df_algorithms, cast=True)

    df_algorithms.write_parquet(output_folder / "gold_algorithms.parquet")


if __name__ == "__main__":
    main()

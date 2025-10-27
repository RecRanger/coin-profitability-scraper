"""Summarize all algorithms into `gold_algorithms` table."""

from pathlib import Path

import dataframely as dy
import orjson
import polars as pl
from loguru import logger

from coin_profitability_scraper.dolt_updater import DoltDatabaseUpdater
from coin_profitability_scraper.dolt_util import DOLT_REPO_URL
from coin_profitability_scraper.reports.silver_stacked_coins import (
    DySchemaSilverStackedCoins,
)
from coin_profitability_scraper.reports.silver_stacked_miners import (
    DySchemaSilverStackedMiners,
)

output_folder = Path("./out/reports/") / Path(__file__).stem


class DySchemaGoldAlgorithms(dy.Schema):
    """Schema for `gold_algorithms` table."""

    algo_name = dy.String(
        primary_key=True, nullable=False, min_length=1, max_length=100
    )
    source_sites_json = dy.String(nullable=False, min_length=2, max_length=1000)
    source_tables_json = dy.String(nullable=False, min_length=2, max_length=1000)
    coin_count = dy.UInt32(nullable=False)

    earliest_coin_created_at = dy.Date(nullable=False)
    latest_coin_created_at = dy.Date(nullable=False)

    earliest_coin = dy.String(nullable=False, min_length=1, max_length=100)
    latest_coin = dy.String(nullable=False, min_length=1, max_length=100)

    volume_24h_usd = dy.Float64(nullable=True)
    market_cap_usd = dy.Float64(nullable=True)

    asic_count = dy.UInt32(nullable=True)
    earliest_asic_announcement_date = dy.Date(nullable=True)
    earliest_asic_launch_date = dy.Date(nullable=True)
    earliest_asic_created_at = dy.Date(nullable=True)
    latest_asic_created_at = dy.Date(nullable=True)

    reported_aliases_json = dy.String(nullable=False, min_length=2, max_length=1000)

    coin_names_json = dy.String(nullable=False, min_length=2, max_length=10_000)

    @dy.rule()
    def reported_aliases_json_is_not_empty_list() -> pl.Expr:
        """Ensure reported_aliases_json is not an empty list."""
        return pl.col("reported_aliases_json").map_elements(
            lambda x: len(orjson.loads(x)) > 0, return_dtype=pl.Boolean
        )


def _fetch_dolt_tables() -> None:
    """Fetch the existing tables in dolt."""
    from coin_profitability_scraper.tables import (  # noqa: PLC0415
        table_to_path_and_schema,
    )

    output_folder.mkdir(parents=True, exist_ok=True)
    logger.info("Starting fetching dolt tables.")

    with DoltDatabaseUpdater(DOLT_REPO_URL) as dolt:
        for table_name in table_to_path_and_schema:
            if not table_name.startswith("silver_"):
                continue

            logger.debug(f"Loading {table_name}")
            df = dolt.read_table_to_polars(table_name)
            logger.info(f"Loaded {table_name}: {df.shape}")

            df.write_parquet(output_folder / f"src_{table_name}.parquet")

        logger.info("Done fetching all tables.")


def _transform_coin_list_to_gold_algorithms(
    df_silver_stacked_coins: dy.DataFrame[DySchemaSilverStackedCoins],
) -> pl.DataFrame:
    """Transform coin list to algorithm list."""
    df = (
        df_silver_stacked_coins.filter(pl.col("algo_name").is_not_null())
        .sort(["coin_created_at"])
        .group_by(["algo_name"], maintain_order=True)
        .agg(
            source_sites=pl.col("source_site").unique().sort(),
            source_tables=pl.col("source_table").unique().sort(),
            earliest_coin_created_at=pl.col("coin_created_at").first(),
            latest_coin_created_at=pl.col("coin_created_at").last(),
            # `coin_count` is approximate as names lack normalization.
            coin_count=pl.col("coin_name").n_unique(),
            earliest_coin=(
                (pl.col("coin_name") + pl.lit(" @ ") + pl.col("source_site")).first()
            ),
            latest_coin=(
                (pl.col("coin_name") + pl.lit(" @ ") + pl.col("source_site")).last()
            ),
            # TODO: Could add a "first_founded_coin" and "first_founded_coin_date".
            # FIXME: These sum across ALL coin reports (including duplicate reports from
            # different sources for the same coin). Good enough heuristic though.
            market_cap_usd=pl.col("market_cap_usd").sum(),
            volume_24h_usd=pl.col("volume_24h_usd").sum(),
            reported_aliases_from_coins=pl.col("reported_algo_name").unique().sort(),
            coin_names=pl.col("coin_name").unique().sort(),
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
        coin_names_json=(
            pl.col("coin_names")
            .map_elements(lambda x: orjson.dumps(x.to_list()), pl.Binary)
            .cast(pl.String)
        ),
    ).drop("source_sites", "source_tables", "coin_names")

    logger.info(f"Transformed coin list to algorithm list with {df.height:,} entries.")
    return df


def _transform_stacked_miners_to_gold_algorithms(
    df_stacked_miners: dy.DataFrame[DySchemaSilverStackedMiners],
) -> pl.DataFrame:
    """Transform asic list to algorithm list."""
    df = (
        df_stacked_miners.filter(pl.col("algo_name").is_not_null())
        .filter(  # TODO: Add info about GPUs as well.
            pl.col("miner_type") == pl.lit("ASIC")
        )
        .group_by(["algo_name"], maintain_order=True)
        .agg(
            asic_count=pl.col("asic_name").n_unique(),
            earliest_asic_announcement_date=pl.col("announcement_date").min(),
            earliest_asic_launch_date=pl.col("launch_date").min(),
            earliest_asic_created_at=pl.col("asic_created_at").min(),
            latest_asic_created_at=pl.col("asic_created_at").max(),
            reported_aliases_from_asics=pl.col("reported_algo_name").unique().sort(),
        )
    )
    logger.info(f"Transformed asic list to algorithm list with {df.height:,} entries.")
    return df


def _warn_about_algo_mapping_opportunities(
    gold_algorithms_df: dy.DataFrame[DySchemaGoldAlgorithms],
) -> None:
    """Warn about potential mapping opportunities (e.g., same name when lowercase).

    Features:
        1. Lowercases.
        2. Removes all non-alphanumeric characters.
    """
    df = gold_algorithms_df.select(
        algo_name_normalized=(
            pl.col("algo_name").str.to_lowercase().str.replace_all(r"[^A-Za-z0-9]", "")
        ),
        algo_name_in_gold=pl.col("algo_name"),
    )
    df = df.group_by("algo_name_normalized").agg(
        algo_names_in_gold=pl.col("algo_name_in_gold").unique().sort(),
        count=pl.col("algo_name_in_gold").n_unique(),
    )
    df = df.filter(pl.col("count") > 1)
    if df.height > 0:
        logger.warning(
            f"Found {df.height} potential algorithm name mapping opportunities "
            "(add in aliases.py):"
        )
        for row in df.iter_rows(named=True):
            logger.warning(
                f"Potential algorithm name mapping opportunity for "
                f"{row['algo_name_normalized']}: {row['algo_names_in_gold']}"
            )


def main() -> None:
    """Summarize all algorithms."""
    _fetch_dolt_tables()

    df_silver_stacked_coins = pl.read_parquet(
        output_folder / "src_silver_stacked_coins.parquet"
    )
    df_silver_stacked_coins = DySchemaSilverStackedCoins.validate(
        df_silver_stacked_coins, cast=True
    )
    logger.info(
        f"Loaded silver stacked coins with {df_silver_stacked_coins.height:,} entries."
    )

    df_silver_stacked_miners = pl.read_parquet(
        output_folder / "src_silver_stacked_miners.parquet"
    )
    df_silver_stacked_miners = DySchemaSilverStackedMiners.validate(
        df_silver_stacked_miners, cast=True
    )
    logger.info(
        f"Loaded silver stacked miners with {df_silver_stacked_miners.height:,} "
        "entries."
    )

    df_algorithms = _transform_coin_list_to_gold_algorithms(df_silver_stacked_coins)

    df_algorithms_from_asics = _transform_stacked_miners_to_gold_algorithms(
        df_silver_stacked_miners
    )

    df_algorithms = df_algorithms.join(
        df_algorithms_from_asics,
        on="algo_name",
        validate="1:1",
        how="left",  # Must have 1+ coins before we care about an ASIC.
    )

    # Merge the reported aliases from coins and asics.
    df_algorithms = df_algorithms.with_columns(
        reported_aliases_json=(
            pl.concat_list(
                pl.col("reported_aliases_from_coins").fill_null(
                    pl.lit([], pl.List(pl.String))
                ),
                pl.col("reported_aliases_from_asics").fill_null(
                    pl.lit([], pl.List(pl.String))
                ),
            )
            .list.unique()
            .list.sort()
            .map_elements(lambda x: orjson.dumps(x.to_list()), pl.Binary)
            .cast(pl.String)
        )
    ).drop("reported_aliases_from_coins", "reported_aliases_from_asics")

    # Cast all datetime cols to dates.
    df_algorithms = df_algorithms.with_columns(pl.selectors.datetime().dt.date())

    df_algorithms = DySchemaGoldAlgorithms.validate(df_algorithms, cast=True)
    _warn_about_algo_mapping_opportunities(df_algorithms)

    df_algorithms.write_parquet(output_folder / "gold_algorithms.parquet")


if __name__ == "__main__":
    main()

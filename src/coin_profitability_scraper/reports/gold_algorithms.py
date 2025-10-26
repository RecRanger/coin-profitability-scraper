"""Summarize all algorithms."""

from pathlib import Path

import dataframely as dy
import orjson
import polars as pl
from loguru import logger

from coin_profitability_scraper.dolt_updater import DoltDatabaseUpdater
from coin_profitability_scraper.dolt_util import DOLT_REPO_URL
from coin_profitability_scraper.reports.aliases import normalize_algorithm_names

output_folder = Path("./out/reports/") / Path(__file__).stem


class DySchemaSilverStackedCoins(dy.Schema):
    """Schema for silver stacked coins."""

    # TODO: Use enum for source_site and source_table.
    source_site = dy.String(
        primary_key=True, nullable=False, min_length=1, max_length=100
    )
    # `coin_unique_source_id`: Should be the name if the name is unique, otherwise it is
    # the slug.
    coin_unique_source_id = dy.String(
        primary_key=True, nullable=False, min_length=1, max_length=100
    )
    coin_name = dy.String(nullable=False, min_length=1, max_length=100)
    algo_name = dy.String(nullable=True, min_length=1, max_length=100)
    reported_algo_name = dy.String(nullable=True, min_length=1, max_length=100)

    coin_url = dy.String(nullable=True, min_length=1, max_length=500)
    source_table = dy.String(nullable=False, min_length=1, max_length=100)
    coin_symbol = dy.String(nullable=True, min_length=1, max_length=100)
    market_cap_usd = dy.UInt64(nullable=True)
    volume_24h_usd = dy.UInt64(nullable=True)
    founded_date = dy.Date(nullable=True)
    coin_created_at = dy.Datetime(nullable=False)


class DySchemaGoldAlgorithms(dy.Schema):
    """Schema for gold algorithms."""

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
            if table_name.startswith(("gold_", "silver_")):
                continue

            logger.debug(f"Loading {table_name}")
            df = dolt.read_table_to_polars(table_name)
            logger.info(f"Loaded {table_name}: {df.shape}")

            df.write_parquet(output_folder / f"src_{table_name}.parquet")

        logger.info("Done fetching all tables.")


def _silver_stacked_coins() -> pl.DataFrame:
    """Stack all coin datasets into a normalized coin list."""
    df = pl.concat(
        [
            pl.read_parquet(output_folder / "src_crypto51_coins.parquet").select(
                source_site=pl.lit("crypto51"),
                source_table=pl.lit("crypto51_coins"),
                coin_unique_source_id=pl.col("coin_name"),
                coin_name=pl.col("coin_name"),
                coin_symbol=pl.col("coin_symbol"),
                reported_algo_name=pl.col("algorithm"),
                market_cap_usd=pl.lit(  # TODO: Data just needs cleaning.
                    None, pl.Int64
                ),
                volume_24h_usd=pl.lit(None, pl.Int64),
                coin_url=pl.col("url"),
                founded_date=pl.lit(None, pl.Date),
                coin_created_at=pl.col("created_at"),
            ),
            pl.read_parquet(output_folder / "src_cryptodelver_coins.parquet").select(
                source_site=pl.lit("cryptodelver"),
                source_table=pl.lit("cryptodelver_coins"),
                coin_unique_source_id=pl.col("coin_slug"),
                coin_name=pl.col("coin_name"),
                coin_symbol=pl.lit(None, pl.String),
                reported_algo_name=pl.col("algo_name"),
                market_cap_usd=pl.col("market_cap_usd"),
                volume_24h_usd=pl.col("volume_usd"),
                coin_url=pl.col("coin_url"),
                founded_date=pl.lit(None, pl.Date),
                coin_created_at=pl.col("created_at"),
            ),
            pl.read_parquet(output_folder / "src_cryptoslate_coins.parquet").select(
                source_site=pl.lit("cryptoslate"),
                source_table=pl.lit("cryptoslate_coins"),
                coin_unique_source_id=pl.col("coin_slug"),
                coin_name=pl.col("coin_name"),
                coin_symbol=pl.col("coin_slug"),
                reported_algo_name=pl.col("hash_algo"),
                market_cap_usd=pl.col("market_cap_usd"),
                volume_24h_usd=pl.lit(None, pl.Int64),
                coin_url=pl.col("url"),
                founded_date=pl.coalesce(
                    pl.col("earliest_logo_date"),
                    pl.date(pl.col("earliest_year_in_description"), 1, 1),
                ),
                coin_created_at=pl.col("created_at"),
            ),
            pl.read_parquet(output_folder / "src_minerstat_coins.parquet").select(
                source_site=pl.lit("minerstat"),
                source_table=pl.lit("minerstat_coins"),
                coin_unique_source_id=pl.col("coin_slug"),
                coin_name=pl.col("coin_slug"),
                coin_symbol=pl.col("coin_slug"),
                reported_algo_name=pl.col("reported_algorithm"),
                market_cap_usd=pl.lit(None, pl.Int64),
                volume_24h_usd=pl.col("volume_usd"),
                coin_url=pl.lit("https://minerstat.com/coin/") + pl.col("coin_slug"),
                founded_date=pl.date(pl.col("reported_founded").cast(pl.UInt32), 1, 1),
                coin_created_at=pl.col("created_at"),
            ),
            pl.read_parquet(output_folder / "src_miningnow_coins.parquet").select(
                source_site=pl.lit("miningnow"),
                source_table=pl.lit("miningnow_coins"),
                coin_unique_source_id=pl.col("coin_name"),
                coin_name=pl.col("coin_name"),
                coin_symbol=pl.col("ticker"),
                reported_algo_name=pl.col("algorithm"),
                market_cap_usd=pl.col("market_cap_usd"),
                volume_24h_usd=pl.col("volume_usd"),
                coin_url=pl.format(
                    "https://miningnow.com/coins/{}/", pl.col("coin_slug")
                ),
                founded_date=pl.col("founded_date"),
                coin_created_at=pl.col("created_at"),
            ),
            pl.read_parquet(output_folder / "src_wheretomine_coins.parquet").select(
                source_site=pl.lit("wheretomine"),
                source_table=pl.lit("wheretomine_coins"),
                coin_unique_source_id=pl.col("coin_name"),
                coin_name=pl.col("coin_name"),
                coin_symbol=pl.col("coin_abbreviation"),
                reported_algo_name=pl.col("algorithm_name"),
                # TODO: Market Cap and Volume seem to be all zeros.
                market_cap_usd=pl.col("market_cap").cast(pl.Int64),
                volume_24h_usd=pl.col("volume_24h").cast(pl.Int64),
                coin_url=pl.format(
                    "https://wheretomine.io/coins/{}/", pl.col("coin_slug")
                ),
                founded_date=pl.lit(None, pl.Date),
                coin_created_at=pl.col("created_at"),
            ),
        ]
    )

    df = df.with_columns(
        algo_name=normalize_algorithm_names(pl.col("reported_algo_name")),
    )

    # Apply schema.
    df = DySchemaSilverStackedCoins.validate(df, cast=True)

    logger.info(f"Stacked coin list with {df.height:,} entries.")
    return df


def _get_stacked_asics_list() -> pl.DataFrame:
    """Stack all asic datasets into a normalized asic list.

    TODO: Could be moved to "silver_asics" table.
    """
    df = pl.concat(
        [
            pl.read_parquet(output_folder / "src_miningnow_asics.parquet").select(
                source_site=pl.lit("crypto51"),
                source_table=pl.lit("crypto51_asics"),
                asic_name=pl.col("title"),
                reported_algo_name=pl.col("algo_title"),
                hash_rate=pl.col("hash_rate"),
                hash_rate_units=pl.col("hash_rate_type_title"),
                cooling_type=pl.col("cooling"),
                price_usd=pl.col("best_price_usd"),
                power_watts=pl.col("power_watts"),
                weight_kg=pl.col("weight_kg"),
                announcement_date=(
                    pl.col("announcement_date").cast(pl.Datetime).dt.date()
                ),
                launch_date=(pl.col("launch_date").cast(pl.Datetime).dt.date()),
                asic_created_at=pl.col("created_at"),
            ),
        ]
    )

    df = df.with_columns(
        algo_name=normalize_algorithm_names(pl.col("reported_algo_name")),
    )

    logger.info(f"Stacked asic list with {df.height:,} entries.")
    return df


def _transform_coin_list_to_gold_algorithms(
    df_silver_stacked_coins: pl.DataFrame,
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


def _transform_asic_list_to_gold_algorithms(df_asic_list: pl.DataFrame) -> pl.DataFrame:
    """Transform asic list to algorithm list."""
    df = (
        df_asic_list.filter(pl.col("algo_name").is_not_null())
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


def main() -> None:
    """Summarize all algorithms."""
    _fetch_dolt_tables()

    df_silver_stacked_coins = _silver_stacked_coins()
    df_silver_stacked_coins = DySchemaSilverStackedCoins.validate(
        df_silver_stacked_coins, cast=True
    )
    logger.info(
        f"Loaded silver stacked coins with {df_silver_stacked_coins.height:,} entries."
    )
    df_silver_stacked_coins.write_parquet(
        output_folder / "silver_stacked_coins.parquet"
    )

    df_algorithms = _transform_coin_list_to_gold_algorithms(df_silver_stacked_coins)

    df_algorithms_from_asics = _transform_asic_list_to_gold_algorithms(
        _get_stacked_asics_list()
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

    df_algorithms.write_parquet(output_folder / "gold_algorithms.parquet")


if __name__ == "__main__":
    main()

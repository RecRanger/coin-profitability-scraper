"""Summarize all ASICs/GPUs into `silver_stacked_miners` table."""

from pathlib import Path

import dataframely as dy
import polars as pl
from loguru import logger

from coin_profitability_scraper.dolt_updater import DoltDatabaseUpdater
from coin_profitability_scraper.dolt_util import DOLT_REPO_URL
from coin_profitability_scraper.reports.aliases import normalize_algorithm_names

output_folder = Path("./out/reports/") / Path(__file__).stem


class DySchemaSilverStackedMiners(dy.Schema):
    """Schema for the `silver_stacked_miners` table."""

    source_site = dy.String(
        primary_key=True, nullable=False, min_length=4, max_length=100
    )
    miner_name = dy.String(
        primary_key=True, nullable=False, min_length=2, max_length=200
    )
    algo_name = dy.String(
        primary_key=True, nullable=False, min_length=2, max_length=100
    )
    reported_algo_name = dy.String(nullable=False, min_length=2, max_length=100)
    miner_type = dy.Enum(["ASIC", "GPU"], nullable=False)

    source_table = dy.String(nullable=False, min_length=4, max_length=100)

    hashrate_hashes_per_second = dy.UInt64(nullable=False)
    cooling_type = dy.String(nullable=True, min_length=1, max_length=200)
    price_usd = dy.Float64(nullable=True)
    power_watts = dy.Float64(nullable=True)
    weight_kg = dy.Float64(nullable=True)

    announcement_date = dy.Date(nullable=True)
    launch_date = dy.Date(nullable=True)
    miner_created_at = dy.Datetime(nullable=False)


def _fetch_dolt_tables() -> None:
    """Fetch the existing tables in dolt."""
    output_folder.mkdir(parents=True, exist_ok=True)
    logger.info("Starting fetching dolt tables.")

    with DoltDatabaseUpdater(DOLT_REPO_URL) as dolt:
        for table_name in ("miningnow_asics", "whattomine_miners"):
            if table_name.startswith(("gold_", "silver_")):
                continue

            logger.debug(f"Loading {table_name}")
            df = dolt.read_table_to_polars(table_name)
            logger.info(f"Loaded {table_name}: {df.shape}")

            df.write_parquet(output_folder / f"src_{table_name}.parquet")

        logger.info("Done fetching all tables.")


def _get_silver_stacked_miners() -> pl.DataFrame:
    """Stack all ASIC/GPU datasets into a normalized miner list."""
    df = pl.concat(
        [
            pl.read_parquet(output_folder / "src_miningnow_asics.parquet")
            .select(
                source_site=pl.lit("miningnow"),
                source_table=pl.lit("miningnow_asics"),
                miner_type=pl.lit("ASIC"),
                miner_name=pl.col("title"),
                reported_algo_name=pl.col("algo_title"),
                hashrate_hashes_per_second=(  # TODO: Move upstream.
                    (
                        pl.col("hash_rate")
                        * pl.col("hash_rate_type_title").replace_strict(
                            {
                                "kH/s": 1e3,
                                "MH/s": 1e6,
                                "GH/s": 1e9,
                                "TH/s": 1e12,
                                "kSol/s": 1e3,
                                "MSol/s": 1e6,
                                "GSol/s": 1e9,
                            },
                            return_dtype=pl.Float64,
                        )
                    )
                    .round()
                    .cast(pl.Int64)
                ),
                cooling_type=pl.col("cooling"),
                price_usd=pl.col("best_price_usd"),
                power_watts=pl.col("power_watts").cast(pl.Float64),
                weight_kg=pl.col("weight_kg"),
                announcement_date=(
                    pl.col("announcement_date").cast(pl.Datetime).dt.date()
                ),
                launch_date=(pl.col("launch_date").cast(pl.Datetime).dt.date()),
                miner_created_at=pl.col("created_at"),
            )
            # Note: Have to remove duplicates here due to multiple hashrates being
            # reported for certain miners (at different power set-points).
            .sort(["hashrate_hashes_per_second", "announcement_date", "launch_date"])
            .unique(
                ["miner_name", "reported_algo_name"], maintain_order=True, keep="last"
            ),
            # Source: whattomine_miners.
            pl.read_parquet(output_folder / "src_whattomine_miners.parquet").select(
                source_site=pl.lit("whattomine"),
                source_table=pl.lit("whattomine_miners"),
                miner_type=pl.col("miner_type"),
                miner_name=pl.col("miner_name"),
                reported_algo_name=pl.col("algorithm_name"),
                hashrate_hashes_per_second=pl.col("hashrate_hashes_per_second").cast(
                    pl.Int64
                ),
                cooling_type=pl.lit(None, pl.String),
                price_usd=pl.lit(None, pl.Float64),
                power_watts=pl.col("power_watts").cast(pl.Float64),
                weight_kg=pl.lit(None, pl.Float64),
                announcement_date=pl.col("release_date"),
                launch_date=pl.col("release_date"),
                miner_created_at=pl.col("created_at"),
            ),
        ]
    )

    df = df.with_columns(
        algo_name=normalize_algorithm_names(pl.col("reported_algo_name")),
    )

    logger.info(f"Stacked miner list with {df.height:,} entries.")
    return df


def main() -> None:
    """Summarize all algorithms."""
    _fetch_dolt_tables()

    df = _get_silver_stacked_miners()
    df = DySchemaSilverStackedMiners.validate(df, cast=True)

    logger.info(f"Loaded silver stacked miners with {df.height:,} entries.")
    df.write_parquet(output_folder / "silver_stacked_miners.parquet")


if __name__ == "__main__":
    main()

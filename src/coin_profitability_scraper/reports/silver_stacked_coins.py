"""Create `silver_stacked_coins` table."""

from collections.abc import Sequence
from pathlib import Path

import dataframely as dy
import polars as pl
from loguru import logger

from coin_profitability_scraper.dolt_updater import DoltDatabaseUpdater
from coin_profitability_scraper.dolt_util import DOLT_REPO_URL
from coin_profitability_scraper.reports.aliases import normalize_algorithm_names

output_folder = Path("./out/reports/") / Path(__file__).stem


class DySchemaSilverStackedCoins(dy.Schema):
    """Schema for `silver_stacked_coins` table."""

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
    reported_coin_name = dy.String(nullable=False, min_length=1, max_length=100)
    algo_name = dy.String(nullable=True, min_length=1, max_length=100)
    reported_algo_name = dy.String(nullable=True, min_length=1, max_length=100)

    coin_url = dy.String(nullable=True, min_length=1, max_length=500)
    source_table = dy.String(nullable=False, min_length=1, max_length=100)
    coin_symbol = dy.String(nullable=True, min_length=1, max_length=100)
    market_cap_usd = dy.UInt64(nullable=True)
    volume_24h_usd = dy.UInt64(nullable=True)
    founded_date = dy.Date(nullable=True)
    coin_created_at = dy.Datetime(nullable=False)


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


def _create_coin_name_normalization_map(
    all_coin_names: Sequence[str],
) -> dict[str, str]:
    """Create a normalization map for reported_coin_name to coin_name.

    Groups by lowercased names with characters stripped (boring form), then picks
    a specific case for each group.
    """
    df = pl.DataFrame({"reported_coin_name": all_coin_names}).unique()
    input_unique_coin_name_list = set(df["reported_coin_name"].to_list())
    df = df.with_columns(
        coin_name_normalized=(
            pl.col("reported_coin_name")
            .str.to_lowercase()
            .str.replace_all(r"[^A-Za-z0-9]", "")
        )
    )
    df = df.group_by("coin_name_normalized").agg(
        coin_names_in_group=pl.col("reported_coin_name").unique().sort(),
        count=pl.col("reported_coin_name").n_unique(),
    )
    normalization_map: dict[str, str] = {}
    for row in df.iter_rows(named=True):
        # Pick the first name in sorted order as the canonical name.
        canonical_name = row["coin_names_in_group"][0]
        for name in row["coin_names_in_group"]:
            # Logical assert. Should not happen.
            assert name not in normalization_map, (
                f"Duplicate mapping for {name}: "
                f"{normalization_map[name]} vs {canonical_name}"
            )

            if canonical_name not in {None, ""}:
                normalization_map[name] = canonical_name
            else:
                normalization_map[name] = name  # Fallback to original name.

    assert set(normalization_map.keys()) == input_unique_coin_name_list, (
        "Normalization map keys do not match input unique coin name list."
    )

    return normalization_map


def _silver_stacked_coins() -> pl.DataFrame:
    """Stack all coin datasets into a normalized coin list."""
    df = pl.concat(
        [
            pl.read_parquet(output_folder / "src_crypto51_coins.parquet").select(
                source_site=pl.lit("crypto51"),
                source_table=pl.lit("crypto51_coins"),
                coin_unique_source_id=pl.col("coin_name"),
                reported_coin_name=pl.col("coin_name"),
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
                reported_coin_name=pl.col("coin_name"),
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
                reported_coin_name=pl.col("coin_name"),
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
                reported_coin_name=pl.col("coin_slug"),
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
                reported_coin_name=pl.col("coin_name"),
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
            pl.read_parquet(output_folder / "src_whattomine_coins.parquet").select(
                source_site=pl.lit("whattomine"),
                source_table=pl.lit("whattomine_coins"),
                coin_unique_source_id=pl.concat_str(
                    pl.col("whattomine_id").cast(pl.UInt32).cast(pl.String),
                    pl.col("coin_name"),
                    separator="-",
                ),
                reported_coin_name=pl.col("coin_name"),
                coin_symbol=pl.col("tag"),
                reported_algo_name=pl.col("algorithm"),
                market_cap_usd=pl.col("market_cap_usd").round().cast(pl.Int64),
                volume_24h_usd=pl.lit(None, pl.Int64),
                coin_url=pl.format(
                    "https://whattomine.com/coins/{}", pl.col("whattomine_id")
                ),
                founded_date=pl.lit(None, pl.Date),
                coin_created_at=pl.col("created_at"),
            ),
            pl.read_parquet(output_folder / "src_wheretomine_coins.parquet").select(
                source_site=pl.lit("wheretomine"),
                source_table=pl.lit("wheretomine_coins"),
                coin_unique_source_id=pl.col("coin_name"),
                reported_coin_name=pl.col("coin_name"),
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

    coin_name_mapping = _create_coin_name_normalization_map(
        df["reported_coin_name"].to_list()
    )

    df = df.with_columns(
        algo_name=normalize_algorithm_names(pl.col("reported_algo_name")),
        coin_name=(
            pl.col("reported_coin_name").replace_strict(
                coin_name_mapping, return_dtype=pl.String
            )
        ),
    )

    # Apply schema.
    df = DySchemaSilverStackedCoins.validate(df, cast=True)

    logger.info(f"Stacked coin list with {df.height:,} entries.")
    return df


def main() -> None:
    """Summarize all algorithms."""
    _fetch_dolt_tables()

    df = _silver_stacked_coins()
    df = DySchemaSilverStackedCoins.validate(df, cast=True)
    logger.info(f"Loaded silver stacked coins with {df.height:,} entries.")
    df.write_parquet(output_folder / "silver_stacked_coins.parquet")


if __name__ == "__main__":
    main()

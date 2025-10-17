"""Prepare ASIC list from MiningNow.com."""

from pathlib import Path

import dataframely as dy
import orjson
import polars as pl
from loguru import logger

from coin_profitability_scraper.data_util import pl_df_all_common_str_cleaning
from coin_profitability_scraper.miningnow.step_1_scrape_data import (
    miningnow_step1_output_path,
)

output_parquet_path = Path("./out/miningnow/") / "miningnow_asics.parquet"


class DySchemaMiningnowAsics(dy.Schema):
    """Schema for miningnow_asic table."""

    asic_slug = dy.String(
        primary_key=True, nullable=False, min_length=1, max_length=200
    )
    asic_id = dy.String(nullable=False, min_length=1, max_length=200)

    hash_rate_type_title = dy.String(nullable=True, min_length=1, max_length=200)
    hash_rate_type = dy.String(nullable=True, min_length=1, max_length=200)
    hash_rate_unit = dy.String(nullable=True, min_length=1, max_length=200)
    brand_title = dy.String(nullable=True, min_length=1, max_length=200)
    algo_title = dy.String(nullable=True, min_length=1, max_length=200)
    noise = dy.String(nullable=True, min_length=1, max_length=200)
    title = dy.String(nullable=True, min_length=1, max_length=200)
    description = dy.String(nullable=True, min_length=1, max_length=1000)
    algo_id = dy.String(nullable=True, min_length=1, max_length=200)
    hash_rate = dy.Float64(nullable=True)
    hash_rate_type_id = dy.String(nullable=True, min_length=1, max_length=200)
    power_watts = dy.Float64(nullable=True)
    weight_kg = dy.Float64(nullable=True)
    announcement_date = dy.String(nullable=True, min_length=1, max_length=200)
    launch_date = dy.String(nullable=True, min_length=1, max_length=200)
    price_index_enable = dy.Bool(nullable=True)
    reported_created_at = dy.String(nullable=True, min_length=1, max_length=200)
    reported_updated_at = dy.String(nullable=True, min_length=1, max_length=200)
    note = dy.String(nullable=True, min_length=1, max_length=200)
    cooling = dy.String(nullable=True, min_length=1, max_length=200)
    best_price_usd = dy.Float64(nullable=True)
    efficiency_value = dy.Float64(nullable=True)
    efficiency_unit = dy.String(nullable=True, min_length=1, max_length=200)

    # Images.
    brand_image_light_url = dy.String(nullable=True, min_length=50, max_length=1000)
    brand_image_dark_url = dy.String(nullable=True, min_length=50, max_length=1000)
    brand_icon_light_url = dy.String(nullable=True, min_length=50, max_length=1000)
    brand_icon_dark_url = dy.String(nullable=True, min_length=50, max_length=1000)
    image_url = dy.String(nullable=True, min_length=50, max_length=1000)
    reference_link_url = dy.String(nullable=True, min_length=1, max_length=1000)


def _fetch_products_data_json_df() -> pl.DataFrame:
    """Prepare asic list from MiningNow.com."""
    data = orjson.loads(
        (miningnow_step1_output_path / "products_data.json").read_bytes()
    )
    logger.info(f"Loaded data for {len(data)} products from products_data.json.")

    df = pl.DataFrame(data)
    df = df.with_columns(
        efficiency_value=pl.col("efficiency").struct["value"],
        efficiency_unit=pl.col("efficiency").struct["type"],
    ).drop(
        "coins",  # List of dicts about coins.
        "efficiency",  # Consumed above.
        "is_brand_title_display_in_mobile",  # Useless.
    )

    df = df.rename(
        {
            "createdAt": "reported_created_at",
            "updatedAt": "reported_updated_at",
        }
    ).with_columns(
        pl.col(col).cast(pl.Datetime)
        for col in ("reported_created_at", "reported_updated_at")
    )

    # Additional renames.
    df = df.rename(
        {
            "id": "asic_id",
            "slug": "asic_slug",
            "best_price": "best_price_usd",
            "weight": "weight_kg",
            "power": "power_watts",
        }
    )
    df = df.rename(
        {
            col: f"{col}_url"
            for col in (
                "brand_image_light",
                "brand_image_dark",
                "brand_icon_light",
                "brand_icon_dark",
                "image",
                "reference_link",
            )
        }
    )
    logger.info(f"Prepared asic list with {df.height} entries from products_data.json.")

    return df


def main() -> None:
    """Prepare asic list from MiningNow.com."""
    output_parquet_path.parent.mkdir(parents=True, exist_ok=True)

    df = _fetch_products_data_json_df()
    df = pl_df_all_common_str_cleaning(df)

    df = DySchemaMiningnowAsics.validate(df, cast=True)

    df.write_parquet(output_parquet_path)
    logger.info(f"Wrote asci list: {df.shape}")


if __name__ == "__main__":
    main()

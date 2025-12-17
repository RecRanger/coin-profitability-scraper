"""Prepare coin list from MiningNow.com."""

from pathlib import Path

import dataframely as dy
import orjson
import polars as pl
from loguru import logger

from coin_profitability_scraper.data_util import pl_df_all_common_str_cleaning
from coin_profitability_scraper.miningnow.step_1_scrape_data import (
    miningnow_step1_output_path,
)

output_parquet_path = Path("./out/miningnow/") / "miningnow_coins.parquet"


class DySchemaMiningnowCoins(dy.Schema):
    """Schema for miningnow_coins table."""

    coin_name = dy.String(
        primary_key=True, nullable=False, min_length=1, max_length=100
    )
    coin_slug = dy.String(nullable=False, min_length=1, max_length=100)
    ticker = dy.String(nullable=False, min_length=1, max_length=50)
    reported_founded = dy.String(nullable=True, min_length=1, max_length=100)
    algorithm = dy.String(nullable=True, min_length=1, max_length=100)
    price_usd = dy.Float(nullable=True)
    market_cap_usd = dy.UInt64(nullable=True)
    volume_usd = dy.UInt64(nullable=True)
    change_24h = dy.Float64(nullable=True)
    founded_date = dy.Date(nullable=True)
    chart_svg_url = dy.String(nullable=True, min_length=25, max_length=1000)
    chart_json_url = dy.String(nullable=True, min_length=25, max_length=1000)
    icon_light_url = dy.String(nullable=True, min_length=25, max_length=1000)
    icon_dark_url = dy.String(nullable=True, min_length=25, max_length=1000)

    @dy.rule(group_by=["coin_name"])
    def _coin_name_unique(cls) -> pl.Expr:
        """Ensure coin_name is unique."""
        return pl.len() == 1

    @dy.rule(group_by=["ticker"])
    def _ticker_unique(cls) -> pl.Expr:
        """Ensure ticker is unique."""
        return pl.len() == 1


def _fetch_coins_data_json_df() -> pl.DataFrame:
    """Prepare coin list from MiningNow.com."""
    data = orjson.loads((miningnow_step1_output_path / "coins_data.json").read_bytes())
    logger.info(f"Loaded data for {len(data)} coins from coins_data.json.")

    df = pl.DataFrame(data)
    df = df.select(
        coin_name=pl.col("title"),
        coin_slug=pl.col("slug"),
        ticker=pl.col("ticker"),
        icon_light_url=pl.col("icon_light"),
        icon_dark_url=pl.col("icon_dark"),
        reported_founded="founded",
        algorithm="algorithm",
        price_usd=pl.col("price").cast(pl.Float64),
        market_cap_usd=pl.col("market_cap").cast(pl.Float64).round().cast(pl.UInt64),
        chart_svg_url="chart_svg",
        chart_json_url="chart_json",
        volume_usd=pl.col("24h_volume").cast(pl.Float64).round().cast(pl.UInt64),
        change_24h=pl.col("24h_change").cast(pl.Float64),
        # Excluded fields:
        # '_id',
        # 'title',
        # 'algo_id',
        # 'coin_id',  # Hex characters.
        # 'timeframes',  # JSON object with price history.
    )

    df = df.with_columns(
        founded_date=pl.col("reported_founded").str.to_date("%b %d, %Y")
    )
    logger.info(f"Prepared coin list with {df.height} entries from coins_data.json.")

    return df


def main() -> None:
    """Prepare coin list from MiningNow.com."""
    output_parquet_path.parent.mkdir(parents=True, exist_ok=True)

    df = _fetch_coins_data_json_df()
    df = pl_df_all_common_str_cleaning(df)

    # TODO: Try to determine algorithms per coin with some complex logic.

    df = DySchemaMiningnowCoins.validate(df, cast=True)

    df.write_parquet(output_parquet_path)
    logger.info(f"Wrote coin list: {df.shape}")


if __name__ == "__main__":
    main()

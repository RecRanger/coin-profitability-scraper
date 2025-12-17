"""Step 2b: Scrape the HTML page of each coin page from Minerstat."""

from pathlib import Path

import orjson
import polars as pl
from loguru import logger
from tqdm import tqdm

from coin_profitability_scraper.minerstat.step_1b_coin_report_from_api import (
    step_1b_output_folder,
)
from coin_profitability_scraper.util import download_as_bytes

step_2b_output_folder_path = Path("./out/minerstat/") / Path(__file__).stem


def main() -> None:
    """Scrape each coin page from Minerstat.

    Main field(s) of interest:
        - Date founded.
    """
    logger.info(f"Starting {Path(__file__).name} main()")

    coins_list = orjson.loads(
        (step_1b_output_folder / "minerstat_coins.json").read_bytes()
    )
    logger.info(f"Loaded {len(coins_list)} coins from Minerstat.")
    df_coins = pl.DataFrame(coins_list, infer_schema_length=None)
    df_coins = df_coins.with_columns(
        coin_slug=pl.col("coin").str.replace_all(" ", "-", literal=True),
    ).with_columns(
        url=pl.lit("https://minerstat.com/coin/") + pl.col("coin_slug"),
    )

    step_2b_output_folder_path.mkdir(parents=True, exist_ok=True)

    for url, coin_slug in tqdm(
        list(
            df_coins.select("url", "coin_slug")
            .unique(maintain_order=True)
            .iter_rows(named=False)
        ),
        unit="coin",
        desc="Scraping Minerstat coin pages",
    ):
        assert isinstance(url, str)

        output_path = step_2b_output_folder_path / f"{coin_slug}.html"

        if 0:  # output_path.exists():
            logger.info(f"Skipping existing file: {output_path}")
            continue

        html_content = download_as_bytes(url)
        if len(html_content) < 5_000:  # noqa: PLR2004
            msg = f"Downloaded content too short for URL: {url}"
            raise ValueError(msg)

        output_path.write_bytes(html_content)
        logger.debug(f"Wrote {len(html_content):,} bytes to {output_path.name}")

    logger.info("Completed scraping all coin pages from Minerstat.")


if __name__ == "__main__":
    main()

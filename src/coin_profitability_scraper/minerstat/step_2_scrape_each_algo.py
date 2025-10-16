"""Scrape the HTML page of each algorithm from Minerstat."""

from pathlib import Path

import polars as pl
from loguru import logger

from coin_profitability_scraper.minerstat.step_1_algo_list import (
    step_1_output_folder_path,
)
from coin_profitability_scraper.util import download_as_bytes

step_2_output_folder_path = Path("./out/minerstat/step_2_scrape_each_algo/")


def main() -> None:
    """Scrape each algorithm page from Minerstat."""
    df = pl.read_parquet(step_1_output_folder_path / "minerstat_algorithms.parquet")
    logger.info(f"Loaded {len(df)} algorithms from Minerstat.")

    step_2_output_folder_path.mkdir(parents=True, exist_ok=True)

    for url, algo_slug in (
        df.select("url", "algo_slug").unique(maintain_order=True).iter_rows(named=False)
    ):
        assert isinstance(url, str)

        algo_slug_validate = url.split("/")[-1]
        assert algo_slug == algo_slug_validate, (algo_slug, algo_slug_validate)
        output_path = step_2_output_folder_path / f"{algo_slug}.html"

        if 0:  # output_path.exists():
            logger.info(f"Skipping existing file: {output_path}")
            continue

        html_content = download_as_bytes(url)
        if len(html_content) < 5_000:  # noqa: PLR2004
            msg = f"Downloaded content too short for URL: {url}"
            raise ValueError(msg)

        output_path.write_bytes(html_content)
        logger.debug(f"Wrote {len(html_content):,} bytes to {output_path.name}")

    logger.info("Completed scraping all algorithm pages from Minerstat.")


if __name__ == "__main__":
    main()

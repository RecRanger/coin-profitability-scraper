"""Step 1: Download all pages of the https://cryptodelver.com/all-coins coins list.

Example pagination: https://cryptodelver.com/all-coins/6
"""

from pathlib import Path

from loguru import logger
from tqdm import tqdm

from coin_profitability_scraper.util import download_as_bytes

_URL = "https://cryptodelver.com/all-coins/"

cryptodelver_step1_output_path = Path("./out/cryptodelver/") / Path(__file__).stem


def main() -> None:
    """Scrape and parse coins list."""
    cryptodelver_step1_output_path.mkdir(parents=True, exist_ok=True)

    page_num = -1  # Fix unbound error for logging outside loop.
    for page_num in tqdm(range(1, 1000), unit="page"):  # 228 pages currently.
        page_url = _URL if page_num == 1 else f"{_URL}{page_num}"

        # Download the page content.
        page_contents = download_as_bytes(page_url)
        (cryptodelver_step1_output_path / f"coins_page_{page_num:04}.html").write_bytes(
            page_contents
        )
        logger.debug(f"Downloaded page from {page_url} - {len(page_contents):,} bytes.")

        # Check to see if this is the last page.
        # Checks to see if the "next page" button is disabled.
        # Note that it's also disabled on the first page, so only check on pages >1.
        if page_num > 1 and '<li class="page-item disabled' in page_contents.decode(
            "utf-8"
        ):
            logger.info(f"Last page reached at page number {page_num}.")
            break

    logger.success(f"Coins list scraping completed. Last page number: {page_num}")


if __name__ == "__main__":
    main()

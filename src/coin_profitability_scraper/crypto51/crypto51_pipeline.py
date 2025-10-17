"""Run the whole Crypto51 pipeline."""

from coin_profitability_scraper import step_9_dolt_write
from coin_profitability_scraper.crypto51 import step_1_scrape_main_page


def main_crypto51_pipeline() -> None:
    """Run the whole Crypto51 pipeline."""
    step_1_scrape_main_page.main()

    step_9_dolt_write.main(("crypto51_coins",))


if __name__ == "__main__":
    main_crypto51_pipeline()

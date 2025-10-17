"""Run the whole CryptoDelver pipeline."""

from coin_profitability_scraper import step_9_dolt_write
from coin_profitability_scraper.cryptodelver import (
    step_1_scrape_coins_lists,
    step_3_ingest_coins_lists,
)


def main_cryptodelver_pipeline() -> None:
    """Run the whole cryptodelver pipeline."""
    step_1_scrape_coins_lists.main()
    step_3_ingest_coins_lists.main()

    step_9_dolt_write.main(("cryptodelver_coins",))


if __name__ == "__main__":
    main_cryptodelver_pipeline()

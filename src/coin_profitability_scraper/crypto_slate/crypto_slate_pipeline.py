"""Run the whole Cypto Slate pipeline."""

from coin_profitability_scraper import step_9_dolt_write
from coin_profitability_scraper.crypto_slate import (
    step_1_scrape,
    step_2_parse_scrape,
    step_3_algo_report,
)


def main_minerstat_pipeline() -> None:
    """Run the whole Cypto Slate pipeline."""
    step_1_scrape.main()
    step_2_parse_scrape.main()
    step_3_algo_report.main()

    step_9_dolt_write.main(("cryptoslate_coins",))


if __name__ == "__main__":
    main_minerstat_pipeline()

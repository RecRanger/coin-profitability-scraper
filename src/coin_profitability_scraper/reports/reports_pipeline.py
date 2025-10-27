"""Run the whole reports pipeline."""

from coin_profitability_scraper import step_9_dolt_write
from coin_profitability_scraper.reports import (
    gold_algorithms,
    silver_stacked_coins,
    silver_stacked_miners,
)


def main_reports_pipeline() -> None:
    """Run the whole reports pipeline."""
    silver_stacked_coins.main()
    silver_stacked_miners.main()
    step_9_dolt_write.main(("silver_stacked_coins", "silver_stacked_miners"))

    gold_algorithms.main()
    step_9_dolt_write.main(("gold_algorithms",))


if __name__ == "__main__":
    main_reports_pipeline()

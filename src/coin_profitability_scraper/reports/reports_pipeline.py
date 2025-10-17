"""Run the whole reports pipeline."""

from coin_profitability_scraper import step_9_dolt_write
from coin_profitability_scraper.reports import gold_algorithms


def main_reports_pipeline() -> None:
    """Run the whole reports pipeline."""
    gold_algorithms.main()

    step_9_dolt_write.main(("gold_algorithms",))


if __name__ == "__main__":
    main_reports_pipeline()

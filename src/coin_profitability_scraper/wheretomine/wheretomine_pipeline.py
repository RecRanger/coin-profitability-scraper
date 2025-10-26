"""Run the whole wheretomine pipeline."""

from coin_profitability_scraper import step_9_dolt_write
from coin_profitability_scraper.wheretomine import step_1_scrape_coins_page


def main_wheretomine_pipeline() -> None:
    """Run the whole wheretomine pipeline."""
    step_1_scrape_coins_page.main()

    step_9_dolt_write.main(("wheretomine_coins",))


if __name__ == "__main__":
    main_wheretomine_pipeline()

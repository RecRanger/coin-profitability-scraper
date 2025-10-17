"""Run the whole Minerstat pipeline."""

from coin_profitability_scraper.minerstat import (
    step_1a_algo_list,
    step_1b_coin_report_from_api,
    step_2a_scrape_each_algo_page,
    step_2b_scrape_each_coin_page,
    step_3b_ingest_each_coin_page,
    step_9_dolt_write,
)


def main_minerstat_pipeline() -> None:
    """Run the whole Minerstat pipeline."""
    step_1a_algo_list.main()
    step_1b_coin_report_from_api.main()
    step_2a_scrape_each_algo_page.main()
    step_2b_scrape_each_coin_page.main()
    step_3b_ingest_each_coin_page.main()
    step_9_dolt_write.main()


if __name__ == "__main__":
    main_minerstat_pipeline()

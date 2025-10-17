"""Run the whole MiningNow pipeline."""

from coin_profitability_scraper import step_9_dolt_write
from coin_profitability_scraper.miningnow import (
    step_1_scrape_data,
    step_2a_coin_list,
    step_2b_algo_list,
    step_2c_asic_list,
)


def main_miningnow_pipeline() -> None:
    """Run the whole MiningNow pipeline."""
    step_1_scrape_data.main()
    step_2a_coin_list.main()
    step_2b_algo_list.main()
    step_2c_asic_list.main()

    step_9_dolt_write.main(
        ("miningnow_coins", "miningnow_algorithms", "miningnow_asics")
    )


if __name__ == "__main__":
    main_miningnow_pipeline()

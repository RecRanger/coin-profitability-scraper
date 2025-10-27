"""Run the whole WhatToMine pipeline."""

from coin_profitability_scraper import step_9_dolt_write
from coin_profitability_scraper.whattomine import (
    step_1_api_fetch,
    step_2_ingest_coins_api,
    step_3_ingest_miners_apis,
)


def main_whattomine_pipeline() -> None:
    """Run the whole WhatToMine pipeline."""
    step_1_api_fetch.main()
    step_2_ingest_coins_api.main()
    step_3_ingest_miners_apis.main()

    step_9_dolt_write.main(("whattomine_coins",))


if __name__ == "__main__":
    main_whattomine_pipeline()

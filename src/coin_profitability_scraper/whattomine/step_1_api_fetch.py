"""Step 1: Scrape coin data from WhatToMine."""

import os
from pathlib import Path

import orjson
from loguru import logger

from coin_profitability_scraper.util import download_as_bytes

step_1_api_data_folder_path = Path("./out/whattomine/step_1_downloaded_coin_pages/")
coins_api_data_json_path = step_1_api_data_folder_path / "v1_coins.json"
asics_api_data_json_path = step_1_api_data_folder_path / "v1_asics.json"
gpus_api_data_json_path = step_1_api_data_folder_path / "v1_gpus.json"


def get_whattomine_api_key() -> str:
    """Get the WhatToMine API key from the environment."""
    key = os.environ["WHAT_TO_MINE_API_KEY"]
    key = key.strip()
    assert len(key) == 64  # noqa: PLR2004
    return key


def main() -> None:
    """Scrape coin data from API.

    Docs: https://whattomine.com/api-docs
    """
    # Create a folder to store downloaded pages.
    step_1_api_data_folder_path.mkdir(parents=True, exist_ok=True)

    # Fetch coins data.
    for api_endpoint, output_file_path in (
        ("v1/coins", coins_api_data_json_path),
        ("v1/asics", asics_api_data_json_path),
        ("v1/gpus", gpus_api_data_json_path),
    ):
        data_bytes = download_as_bytes(
            f"https://whattomine.com/api/{api_endpoint}?api_token={get_whattomine_api_key()}"
        )
        logger.info(f"Fetched the {api_endpoint} API: {len(data_bytes):,} bytes.")
        data = orjson.loads(data_bytes)
        logger.info(f"Endpoint {api_endpoint} contains list of {len(data):,} entries.")
        output_file_path.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2))

    logger.info("Download complete.")


if __name__ == "__main__":
    main()

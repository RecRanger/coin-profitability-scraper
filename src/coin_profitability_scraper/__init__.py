"""Tool for assessing profitability opportunities of obscure cryptocurrencies."""

import os
from pathlib import Path

from loguru import logger

PACKAGE_ROOT = Path(__file__).parent


def is_dry_run() -> bool:
    """Return True if the current run is a dry run (e.g., triggered by a pull request).

    Determined by the environment variable DRY_RUN set in the GitHub Actions workflow.
    """
    dry_run: bool = os.getenv("DRY_RUN", "").lower() == "true"
    if dry_run:
        logger.info("⚠️ Running in DRY RUN mode. No data will be written to Dolt.")
    return dry_run

"""Inventory of all tables."""

from pathlib import Path
from typing import Literal

import dataframely as dy

from coin_profitability_scraper.crypto_slate import (
    step_2_parse_scrape as cryptoslate_coins_module,
)
from coin_profitability_scraper.minerstat import (
    step_3b_ingest_each_coin_page as minerstat_coins_module,
)

TableNameLiteral = Literal["minerstat_coins", "cryptoslate_coins"]

table_to_path_and_schema: dict[TableNameLiteral, tuple[Path, type[dy.Schema]]] = {
    "minerstat_coins": (
        minerstat_coins_module.step_3b_output_folder / "minerstat_coins.parquet",
        minerstat_coins_module.DySchemaMinerstatCoins,
    ),
    "cryptoslate_coins": (
        cryptoslate_coins_module.step_2_output_folder / "cryptoslate_coins.parquet",
        cryptoslate_coins_module.DySchemaCryptoslateCoins,
    ),
}

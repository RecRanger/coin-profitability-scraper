"""Inventory of all tables."""

from pathlib import Path
from typing import Literal

import dataframely as dy

from coin_profitability_scraper.crypto51 import (
    step_1_scrape_main_page as crypto51_coins_module,
)
from coin_profitability_scraper.crypto_slate import (
    step_2_parse_scrape as cryptoslate_coins_module,
)
from coin_profitability_scraper.minerstat import (
    step_3b_ingest_each_coin_page as minerstat_coins_module,
)
from coin_profitability_scraper.miningnow import (
    step_2a_coin_list as miningnow_coins_module,
)
from coin_profitability_scraper.miningnow import (
    step_2b_algo_list as miningnow_algorithms_module,
)
from coin_profitability_scraper.miningnow import (
    step_2c_asic_list as miningnow_asics_module,
)

TableNameLiteral = Literal[
    "minerstat_coins",
    "cryptoslate_coins",
    "miningnow_coins",
    "miningnow_algorithms",
    "miningnow_asics",
    "crypto51_coins",
]

table_to_path_and_schema: dict[TableNameLiteral, tuple[Path, type[dy.Schema]]] = {
    "minerstat_coins": (
        minerstat_coins_module.step_3b_output_folder / "minerstat_coins.parquet",
        minerstat_coins_module.DySchemaMinerstatCoins,
    ),
    "cryptoslate_coins": (
        cryptoslate_coins_module.step_2_output_folder / "cryptoslate_coins.parquet",
        cryptoslate_coins_module.DySchemaCryptoslateCoins,
    ),
    "miningnow_coins": (
        miningnow_coins_module.output_parquet_path,
        miningnow_coins_module.DySchemaMiningnowCoins,
    ),
    "miningnow_algorithms": (
        miningnow_algorithms_module.output_parquet_path,
        miningnow_algorithms_module.DySchemaMiningnowAlgorithms,
    ),
    "miningnow_asics": (
        miningnow_asics_module.output_parquet_path,
        miningnow_asics_module.DySchemaMiningnowAsics,
    ),
    "crypto51_coins": (
        crypto51_coins_module.output_parquet_file,
        crypto51_coins_module.DySchemaCrypto51Coins,
    ),
}

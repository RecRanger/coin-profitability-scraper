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
from coin_profitability_scraper.cryptodelver import (
    step_3_ingest_coins_lists as cryptodelver_coins_module,
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
from coin_profitability_scraper.notify import (
    notify_new_gold_algorithms as notify_new_gold_algorithms_module,
)
from coin_profitability_scraper.reports import gold_algorithms as gold_algorithms_module
from coin_profitability_scraper.reports import (
    silver_stacked_coins as silver_stacked_coins_module,
)
from coin_profitability_scraper.reports import (
    silver_stacked_miners as silver_stacked_miners_module,
)
from coin_profitability_scraper.whattomine import (
    step_2_ingest_coins_api as whattomine_coins_module,
)
from coin_profitability_scraper.whattomine import (
    step_3_ingest_miners_apis as whattomine_miners_module,
)
from coin_profitability_scraper.wheretomine import (
    step_1_scrape_coins_page as wheretomine_coins_module,
)

TableNameLiteral = Literal[
    "minerstat_coins",
    "cryptoslate_coins",
    "miningnow_coins",
    "miningnow_algorithms",
    "miningnow_asics",
    "crypto51_coins",
    "cryptodelver_coins",
    "whattomine_coins",
    "whattomine_miners",
    "wheretomine_coins",
    "silver_stacked_coins",
    "silver_stacked_miners",
    "gold_algorithms",
    "notify_log_new_algorithms",
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
    "cryptodelver_coins": (
        cryptodelver_coins_module.output_parquet_path,
        cryptodelver_coins_module.DySchemaCryptodelverCoins,
    ),
    "whattomine_coins": (
        whattomine_coins_module.output_parquet_file,
        whattomine_coins_module.DySchemaWhattomineCoins,
    ),
    "whattomine_miners": (
        whattomine_miners_module.output_parquet_file,
        whattomine_miners_module.DySchemaWhattomineMiners,
    ),
    "wheretomine_coins": (
        wheretomine_coins_module.output_parquet_file,
        wheretomine_coins_module.DySchemaWheretomineCoins,
    ),
    "silver_stacked_coins": (
        silver_stacked_coins_module.output_folder / "silver_stacked_coins.parquet",
        silver_stacked_coins_module.DySchemaSilverStackedCoins,
    ),
    "silver_stacked_miners": (
        silver_stacked_miners_module.output_folder / "silver_stacked_miners.parquet",
        silver_stacked_miners_module.DySchemaSilverStackedMiners,
    ),
    "gold_algorithms": (
        gold_algorithms_module.output_folder / "gold_algorithms.parquet",
        gold_algorithms_module.DySchemaGoldAlgorithms,
    ),
    "notify_log_new_algorithms": (
        (
            notify_new_gold_algorithms_module.store_folder
            / "notify_log_new_algorithms.parquet"
        ),
        notify_new_gold_algorithms_module.DySchemaNotifyLogNewAlgorithms,
    ),
}

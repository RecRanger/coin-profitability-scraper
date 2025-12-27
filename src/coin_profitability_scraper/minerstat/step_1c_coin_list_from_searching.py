"""Step 1c: Get the coin list by searching.

Could use their expensive API to query this as well, but this is simpler.

The endpoint used here was discovered by looking at the network requests made by
performing a search in the search box.

Example response (search for "0x"):
```json
[{
    "coinTag":"0XBTC","coin":"0XBTC","algo":"Solidity-SHA3","marketcap":null,
    "name":"0XBTC","img":"0xbtc","multipool":0,"repeated":1
}]
```
"""

import itertools
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import backoff
import fake_useragent
import orjson
import polars as pl
import requests
from loguru import logger
from tqdm import tqdm

step_1c_output_folder = Path("./out/minerstat/") / Path(__file__).stem


@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException,),
    on_backoff=lambda details: logger.debug(f"Backing off: {details}"),
    max_tries=5,
)
def _fetch_coins_for_search(search_term: str) -> list[dict[str, Any]]:
    response = requests.post(
        "https://minerstat.com/coins",
        # Didn't work - data={"search": search_term},
        data=f"search={search_term}",
        headers={
            "accept": "*/*",
            "accept-language": "en-US",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://minerstat.com",
            "priority": "u=1, i",
            "referer": "https://minerstat.com/coins",
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "User-Agent": fake_useragent.UserAgent().random,
            "x-requested-with": "XMLHttpRequest",
        },
        timeout=15,
    )
    response.raise_for_status()

    if response.content in {b"", b"null"}:
        return []

    data: list[dict[str, Any]] = response.json()
    assert isinstance(data, list)
    return data


def _search_all_coins(max_workers: int = 32) -> list[dict[str, Any]]:
    coin_list: list[dict[str, Any]] = []

    search_terms = [
        f"{char_0}{char_1}"
        for char_0, char_1 in itertools.product(
            "0123456789abcdefghijklmnopqrstuvwxyz", repeat=2
        )
    ]
    logger.info(f"Searching for {len(search_terms):,} terms to find all coins.")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in tqdm(
            executor.map(_fetch_coins_for_search, search_terms),
            total=len(search_terms),
        ):
            coin_list.extend(result)

    if len(coin_list) < 500:  # noqa: PLR2004
        msg = f"Unexpectedly low number of coin search results found: {len(coin_list)}"
        raise RuntimeError(msg)

    return coin_list


def main() -> None:
    """Generate a report summarizing the parsed coin data."""
    logger.info(f"Starting {Path(__file__).name} main()")

    step_1c_output_folder.mkdir(parents=True, exist_ok=True)

    coins_list = _search_all_coins()
    logger.info(
        f"Found {len(coins_list)} coin search results from exhaustive Minerstat search."
    )

    # Main data output used in subsequent steps: "minerstat_coins.json".
    (step_1c_output_folder / "minerstat_coins.json").write_bytes(
        orjson.dumps(coins_list, option=orjson.OPT_INDENT_2)
    )
    logger.info("Wrote raw Minerstat coin data to JSON file.")

    df = pl.DataFrame(coins_list)
    df = df.lazy().unique().sort(df.columns).collect()
    df.write_parquet(step_1c_output_folder / "minerstat_coins.parquet")
    logger.info(
        f"Wrote Minerstat coin data to Parquet file. {df.height:,} distinct coins."
    )
    if df.height < 100:  # noqa: PLR2004
        msg = f"Unexpectedly low number of distinct coins found: {df.height}"
        raise RuntimeError(msg)

    logger.info(f"Finished {Path(__file__).name} main()")


if __name__ == "__main__":
    main()

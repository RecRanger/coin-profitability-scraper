"""Step 1: Scrape all data from MiningNow.com.

All data is loaded on the page as JSON embedded in JavaScript.
"""

import re
from pathlib import Path
from typing import Any

import orjson
from bs4 import BeautifulSoup
from loguru import logger
from tqdm import tqdm

from coin_profitability_scraper.util import download_as_bytes

_URL = "https://miningnow.com/latest-asic-miner-list/"

miningnow_step1_output_path = Path("./out/miningnow/") / Path(__file__).stem


def extract_valid_json_substrings(s: str) -> list[dict[str, Any]]:
    """Yield every valid JSON substring in the given string.

    It starts parsing at each '{' and tries to find the smallest valid JSON object.
    """
    results: list[dict[str, Any]] = []

    start = 0
    while start < len(s):
        if s[start] == "{":
            for end in range(start + 1, len(s) + 1):
                if s[end - 1] != "}":  # Optimization: No need to try parsing.
                    continue

                candidate = s[start:end]
                try:
                    results.append(orjson.loads(candidate))

                except orjson.JSONDecodeError:
                    continue
                else:
                    # Mark as the new start position, and stop at the first valid one
                    # starting here.
                    start = end - 1
                    break
        start += 1
    return results


def _extract_asics_data(page_html: str) -> list[dict[str, str]]:
    # --- Step 2. Parse the HTML ---
    soup = BeautifulSoup(page_html, "html.parser")

    # --- Step 3. Extract <script> contents with Next.js chunks ---
    scripts = soup.find_all("script")

    data_fragments: list[str] = [
        s.string.strip()
        for s in scripts
        if s.string  # Other option maybe? - "self.__NEXT_DATA__" in s.string
        and "self.__next_f.push" in s.string
    ]

    # --- Step 4. Extract raw push() payloads ---
    pattern = re.compile(r"self\.__next_f\.push\(\[\d+,\s*\"(.*?)\"\]\)", re.DOTALL)
    matches = pattern.findall("\n".join(data_fragments))

    data_fragments_together = ""
    for data_fragment in data_fragments:
        result = pattern.match(data_fragment)
        if result:
            data_fragments_together += result.group(1)
        else:
            msg = "No match found in data fragment."
            raise ValueError(msg)
    del matches, data_fragments

    # Debugging: Save raw extracted scripts.
    (
        miningnow_step1_output_path / "checkpoint_2_miningnow_asics_raw_scripts.txt"
    ).write_text(data_fragments_together)

    text = data_fragments_together.replace(r"\"", '"').replace(r"\/", "/")
    start_delimiter = '{"id":"json-ld-webpage-asic-miners","type":"application/ld+json"'
    text = start_delimiter + text.split(start_delimiter)[1]
    # Debugging: Save after split.
    (
        miningnow_step1_output_path / "checkpoint_3_miningnow_asics_after_split.txt"
    ).write_text(text)

    data = list(
        tqdm(extract_valid_json_substrings(text), desc="Extracting JSON substrings")
    )
    logger.info(f"Found {len(data)} JSON substrings.")

    (miningnow_step1_output_path / "checkpoint_4_json_substrings.json").write_bytes(
        orjson.dumps(data, option=orjson.OPT_INDENT_2)
    )

    return data


def main() -> None:
    """Scrape and parse ASICs list and other datasets."""
    miningnow_step1_output_path.mkdir(parents=True, exist_ok=True)

    # Download the page content.
    page_contents = download_as_bytes(_URL)
    (miningnow_step1_output_path / "miningnow_asics_list.html").write_bytes(
        page_contents
    )
    logger.info(
        f"Downloaded ASICs list page from {_URL} - {len(page_contents):,} bytes."
    )

    page_html = page_contents.decode("utf-8")

    data_list = _extract_asics_data(page_html)
    logger.debug(f"Extracted data list with {len(data_list)} items.")

    # Navigate to the interesting data.
    dig = data_list[0]["children"]
    data_types: dict[str, list[Any]] = next(x for x in dig if isinstance(x, dict))
    assert isinstance(data_types, dict)

    for data_type_key in ("coins", "algos", "brands", "products"):
        data: list[Any] = data_types[data_type_key]  # pyright: ignore[reportArgumentType,reportAssignmentType]
        assert isinstance(data, list)

        logger.info(f"Found {len(data)} items of type '{data_type_key}'.")
        (miningnow_step1_output_path / f"{data_type_key}_data.json").write_bytes(
            orjson.dumps(data, option=orjson.OPT_INDENT_2)
        )

    logger.success("ASICs list scraping completed.")


if __name__ == "__main__":
    main()

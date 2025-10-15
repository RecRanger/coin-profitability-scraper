"""Step 2: Parse the downloaded HTML files to extract coin information."""

import re
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup
from loguru import logger
from tqdm import tqdm

from coin_profitability_scraper.step_1_scrape import step_1_html_folder_path
from coin_profitability_scraper.util import get_datetime_str, write_tables

step_2_output_folder = Path("./out/step_2_coins_list/")


def _get_hash_algo_from_html(soup: BeautifulSoup) -> str | None:
    """Extract the hash algorithm from the HTML soup."""
    try:
        # Find the span with class="info" containing "Hash Algorithm"
        info_span = soup.find(
            "span",
            class_="info",
            string="Hash Algorithm",  # pyright: ignore[reportArgumentType]
        )

        # If the span is found, get the next sibling span with class="value"
        if info_span:
            value_span = info_span.find_next("span", class_="value")

            # If the value span is found, return its text content
            if value_span:
                return value_span.text.strip()

    except Exception as e:  # noqa: BLE001
        # Handle any exceptions (e.g., file not found, parsing errors)
        logger.error(f"Error parsing: {e}")

    # Return None if the value is not found or an error occurs.
    return None


def _get_market_cap_from_html(soup: BeautifulSoup) -> float | None:
    """Extract the market cap from the HTML soup."""
    span_element = soup.find("span", class_="holepunch holepunch-coin_market_cap_usd")

    if span_element:
        # Extract the text content and remove the '$' and 'M'
        dollar_value_str: str = span_element.text.strip().replace("$", "").strip()
        dollar_value_float: float | None = None

        if dollar_value_str.lower().endswith("k"):
            dollar_value_str = dollar_value_str.lower().replace("k", "").strip()
            dollar_value_float = float(dollar_value_str) * 1_000
        elif dollar_value_str.endswith("M"):
            dollar_value_str = dollar_value_str.replace("M", "").strip()
            dollar_value_float = float(dollar_value_str) * 1_000_000
        elif dollar_value_str.endswith("B"):
            dollar_value_str = dollar_value_str.replace("B", "").strip()
            dollar_value_float = float(dollar_value_str) * 1_000_000_000
        elif dollar_value_str.endswith("T"):
            dollar_value_str = dollar_value_str.replace("T", "").strip()
            dollar_value_float = float(dollar_value_str) * 1_000_000_000_000
        else:
            dollar_value_str = dollar_value_str.strip()
            dollar_value_float = float(dollar_value_str)
        return dollar_value_float
    return None


def _get_earliest_year_from_html(html: str) -> int | None:
    """Extract the earliest year from the HTML content."""
    matches = re.findall(r"in (20\d{2})", html)
    years = [
        int(year)
        for year in matches
        if 2000 <= int(year) <= datetime.now(UTC).year  # noqa: PLR2004
    ]
    return min(years) if years else None


def load_coin_list_df() -> pl.DataFrame:
    """Load the coin list from the downloaded HTML files."""
    data: list[dict[str, str | float | None]] = []
    for html_file_path in tqdm(
        sorted(step_1_html_folder_path.glob("*.html")), unit="file"
    ):
        html_content = html_file_path.read_text()
        soup = BeautifulSoup(html_content, "html.parser")

        hash_algo = _get_hash_algo_from_html(soup)
        market_cap = _get_market_cap_from_html(soup)

        data.append(
            {
                "filename": html_file_path.name,
                "coin": html_file_path.stem,
                "hash_algo": hash_algo,
                "market_cap": market_cap,
                "earliest_year": _get_earliest_year_from_html(html_content),
            }
        )

    df = pl.DataFrame(data)

    df = df.with_columns(
        url=pl.format("https://cryptoslate.com/coins/{}/", pl.col("coin")),
    )
    df = df.sort("market_cap", descending=True, nulls_last=True)
    logger.info(f"Coin List: {df}")

    return df


def main() -> None:
    """Parse HTML files and extract coin information."""
    logger.info("Starting")
    step_2_output_folder.mkdir(parents=True, exist_ok=True)

    df = load_coin_list_df()

    logger.info(
        f"Total market cap of {len(df):,} coins here: "
        f"US${df['market_cap'].sum() / 1_000_000_000_000:.3f}T"
    )

    write_tables(
        df,
        f"coin_list_{get_datetime_str()}_{len(df)}coins",
        step_2_output_folder,
    )
    logger.info("Wrote coin list table.")

    logger.info(f"Top 10 coins by market cap:\n{df.head(10)}")


if __name__ == "__main__":
    main()

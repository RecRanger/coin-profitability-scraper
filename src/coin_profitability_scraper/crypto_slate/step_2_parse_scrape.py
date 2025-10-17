"""Step 2: Parse the downloaded HTML files to extract coin information."""

import re
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import dataframely as dy
import polars as pl
from bs4 import BeautifulSoup
from loguru import logger
from tqdm import tqdm

from coin_profitability_scraper.crypto_slate.step_1_scrape import (
    step_1_html_folder_path,
)
from coin_profitability_scraper.data_util import (
    clean_col_name,
    pl_df_all_common_str_cleaning,
)

step_2_output_folder = Path("./out/crypto_slate/step_2_coins_list/")

_default_string_kwargs: dict[Literal["min_length", "max_length"], int] = {
    "min_length": 1,
    "max_length": 200,
}


class DySchemaCryptoslateCoins(dy.Schema):
    """Schema for cryptoslate_coins table."""

    coin_slug = dy.String(primary_key=True, nullable=False, **_default_string_kwargs)

    # `coin_name` is not unique.
    coin_name = dy.String(nullable=False, **_default_string_kwargs)
    hash_algo = dy.String(nullable=True, **_default_string_kwargs)
    market_cap_usd = dy.UInt64(nullable=True)
    earliest_year_in_description = dy.UInt16(nullable=True)
    earliest_logo_date = dy.Date(nullable=True)
    html_file_size_bytes = dy.UInt32(nullable=False)
    reported_blockchain = dy.String(nullable=True, **_default_string_kwargs)
    reported_consensus = dy.String(nullable=True, **_default_string_kwargs)
    reported_hash_algorithm = dy.String(nullable=True, **_default_string_kwargs)
    reported_org_structure = dy.String(nullable=True, **_default_string_kwargs)
    reported_development_status = dy.String(nullable=True, **_default_string_kwargs)
    reported_open_source = dy.String(nullable=True, **_default_string_kwargs)
    reported_hard_wallet_support = dy.String(nullable=True, **_default_string_kwargs)
    reported_block_time = dy.String(nullable=True, **_default_string_kwargs)
    reported_staking_apr = dy.String(nullable=True, **_default_string_kwargs)
    reported_inflation = dy.String(nullable=True, **_default_string_kwargs)
    url = dy.String(nullable=True, **_default_string_kwargs)
    earliest_year = dy.UInt16(nullable=True)


def _extract_technical_key_value_from_soup(
    soup: BeautifulSoup, *, coin_slug: str
) -> dict[str, str | None]:
    """Extract the hash algorithm from the HTML soup.

    Most importantly, returns the Hash Algorithm.
    """
    technical_info_key_value_section = soup.find("div", class_="technical")
    if technical_info_key_value_section is None:
        msg = f"Could not find technical info section. Coin: {coin_slug}"
        raise ValueError(msg)

    out: dict[str, str | None] = {}
    for li_element in technical_info_key_value_section.find_all("li"):
        if "hidden" in li_element.attrs.get("class", []):
            continue  # Skip hidden elements.

        key_element = li_element.find("span", class_="info")
        if key_element is None:
            msg = (
                f"Could not find key in technical info section. "
                f"Coin: {coin_slug}, li: {li_element}"
            )
            raise ValueError(msg)

        if a_tags := li_element.find_all("a"):
            # Special case: if there are links, join their text with commas.
            value_str = ", ".join(a.text.strip() for a in a_tags if a.text.strip())

        else:
            value_element = li_element.find("span", class_="value")

            if value_element is None:
                msg = (
                    f"Could not find key and/or value in technical info section. "
                    f"Coin: {coin_slug}, li: {li_element}"
                )
                raise ValueError(msg)

            if key_element.text in out:
                msg = (
                    f"Repeated technical info key in coin {coin_slug}: "
                    f"{key_element.text}"
                )
                raise ValueError(msg)

            value_str = re.sub(r"\s+", " ", value_element.text.strip())

        out[key_element.text] = value_str
    return out


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


def _get_earliest_year_from_html_text_description(html: str) -> int | None:
    """Extract the earliest year from the HTML content."""
    matches = re.findall(r"in (20\d{2})", html, flags=re.IGNORECASE)
    years = [
        int(year)
        for year in matches
        if 2000 <= int(year) <= datetime.now(UTC).year  # noqa: PLR2004
    ]
    return min(years) if years else None


def _get_earliest_logo_date_from_soup(
    soup: BeautifulSoup, *, coin_slug: str
) -> date | None:
    """Extract the earliest logo date from the HTML content.

    Example logo URL:
    https://cryptoslate.com/wp-content/uploads/2023/05/ArbDoge-AI-logo.jpg
    """
    # First, only look within: <div class='name-logo'>
    logo_div = soup.find("div", class_="name-logo")
    if not logo_div:
        logger.warning(f'No "name-logo" div found for coin_slug="{coin_slug}"')
        return None

    # Now find all logo image URLs within that div.
    matches = re.findall(
        r"/wp-content/uploads/(\d{4})/(\d{2})/(?:[A-Za-z0-9\-_]+)-logo\.(?:jpg|png|jpeg)",
        str(logo_div),
        flags=re.IGNORECASE,
    )
    dates: list[date] = []
    for match in matches:
        year, month = int(match[0]), int(match[1])
        if 2000 <= year <= datetime.now(UTC).year and 1 <= month <= 12:  # noqa: PLR2004
            dates.append(date(year, month, 1))

    return min(dates) if dates else None


def _get_coin_name_from_soup(soup: BeautifulSoup) -> str | None:
    """Extract the coin name from the HTML soup."""
    try:
        title_tag = soup.find("title")
        if title_tag and title_tag.text:
            # The title format is usually "Coin Name (SYMBOL) - CryptoSlate"
            title_text = title_tag.text.strip()
            return title_text.split(" (")[0]  # Get text before " ("

    except Exception as e:  # noqa: BLE001
        logger.error(f"Error parsing coin name: {e}")

    return None


def _load_file_fetch_data(html_file_path: Path) -> dict[str, date | str | float | None]:
    """Load and parse a single HTML file to extract coin information."""
    html_content = html_file_path.read_text()
    soup = BeautifulSoup(html_content, "html.parser")

    technical_kv_data = _extract_technical_key_value_from_soup(
        soup, coin_slug=html_file_path.stem
    )
    technical_kv_data_cols = {
        "reported_" + clean_col_name(k): v for k, v in technical_kv_data.items()
    }
    market_cap_usd = _get_market_cap_from_html(soup)

    main_data: dict[str, date | str | float | None] = {
        "coin_slug": html_file_path.stem,
        "coin_name": _get_coin_name_from_soup(soup),
        "hash_algo": technical_kv_data.get("Hash Algorithm"),
        "market_cap_usd": market_cap_usd,
        "earliest_year_in_description": (
            _get_earliest_year_from_html_text_description(html_content)
        ),
        "earliest_logo_date": _get_earliest_logo_date_from_soup(
            soup, coin_slug=html_file_path.stem
        ),
        "html_file_size_bytes": html_file_path.stat().st_size,
    }
    return main_data | technical_kv_data_cols


def load_coin_list_df() -> pl.DataFrame:
    """Load the coin list from the downloaded HTML files."""
    file_list = sorted(step_1_html_folder_path.glob("*.html"))
    logger.info(f"Found {len(file_list):,} HTML files to parse.")

    with ThreadPoolExecutor(max_workers=32) as executor:
        data = list(
            tqdm(
                executor.map(_load_file_fetch_data, file_list),
                total=len(file_list),
                unit="file",
                smoothing=0,  # Average over all files. No smoothing.
            )
        )

    df = pl.DataFrame(data)
    logger.debug(f"Raw coin data: {df.height:,} rows")

    df = df.with_columns(
        pl.col("market_cap_usd").round().cast(pl.UInt64),  # Float to int.
        url=pl.format("https://cryptoslate.com/coins/{}/", pl.col("coin_slug")),
        earliest_year=pl.min_horizontal(
            pl.col("earliest_logo_date").cast(pl.Date).dt.year(),
            pl.col("earliest_year_in_description"),
        ),
    )
    df = df.sort("market_cap_usd", descending=True, nulls_last=True)
    logger.info(f"Coin List: {df}")

    return df


def main() -> None:
    """Parse HTML files and extract coin information."""
    logger.info("Starting")
    step_2_output_folder.mkdir(parents=True, exist_ok=True)

    df = load_coin_list_df()

    df = pl_df_all_common_str_cleaning(df)

    # Add missing columns. Mostly a hack.
    for col, dtype in DySchemaCryptoslateCoins.polars_schema().items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))

    df = DySchemaCryptoslateCoins.validate(df, cast=True)

    logger.info(
        f"Total market cap of {len(df):,} coins here: "
        f"US${df['market_cap_usd'].sum() / 1_000_000_000_000:.3f}T"
    )

    logger.info("Wrote coin list table.")
    logger.info(f"Top 10 coins by market cap:\n{df.head(10)}")

    step_2_output_folder.mkdir(parents=True, exist_ok=True)
    df.write_parquet(step_2_output_folder / "cryptoslate_coins.parquet")


if __name__ == "__main__":
    main()

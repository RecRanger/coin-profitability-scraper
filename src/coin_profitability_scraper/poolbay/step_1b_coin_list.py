"""Step 1: Get the coin list."""

from pathlib import Path

import dataframely as dy
import polars as pl
import requests
from bs4 import BeautifulSoup
from loguru import logger

from coin_profitability_scraper.data_util import pl_df_all_common_str_cleaning

step_1_output_folder = Path("./out/poolbay/") / Path(__file__).stem


# class DySchemaMinerstatCoins(dy.Schema):
#     """Schema for minerstat_coins table."""

#     coin_slug = dy.String(primary_key=True, nullable=False, **_default_string_kwargs)
#     reported_algorithm = dy.String(nullable=True, **_default_string_kwargs)
#     reported_difficulty = dy.String(nullable=True, **_default_string_kwargs)
#     reported_block_reward = dy.String(nullable=True, **_default_string_kwargs)
#     reported_volume = dy.String(nullable=True, **_default_string_kwargs)
#     reported_founded = dy.String(nullable=True, **_default_string_kwargs)
#     reported_network_hashrate = dy.String(nullable=True, **_default_string_kwargs)
#     reported_revenue = dy.String(nullable=True, **_default_string_kwargs)
#     reported_block_dag = dy.String(nullable=True, **_default_string_kwargs)
#     reported_block_epoch = dy.String(nullable=True, **_default_string_kwargs)
#     volume_usd = dy.UInt64(nullable=True)

#     @dy.rule()
#     def _volume_usd_parsed_correctly(cls) -> pl.Expr:
#         """`reported_volume` must be null or non-null the same as `volume_usd`."""
#         return (
#             pl.col("reported_volume")
#             .is_null()
#             .eq_missing(pl.col("volume_usd").is_null())
#         )


def _extract_table_data(page_html: str) -> list[dict[str, str | None]]:
    soup = BeautifulSoup(page_html, "html.parser")

    # Find the table.
    table = soup.find("table", class_="table")
    assert table is not None

    # Extract the table rows.
    table_header: list[str] = [
        th.get_text(strip=True)
        for th in table.find("tr").find_all("th")  # pyright: ignore[reportOptionalMemberAccess]
    ]
    data: list[dict[str, str | None]] = []
    for row in table.find_all("tr")[1:]:  # Skip header row.
        row_data: dict[str, str | None] = {}
        for idx, cell in enumerate(row.find_all("td")):
            column_name = table_header[idx]

            # Store the cell link, if applicable.
            if column_name == "Name":
                # The coin name column contains 3 parts:
                # Image, linked coin name, and ticker.
                link = cell.find("a")
                if link and ("href" in link.attrs):
                    # Add columns like "gpu_count" and "gpu_href".
                    row_data["name"] = link.get_text(strip=True)
                    row_data["name_href"] = str(link["href"])

                small_ticker = cell.find("small")
                if small_ticker:
                    row_data["ticker"] = small_ticker.get_text(strip=True)
            else:
                # Store the cell contents.
                row_data[column_name] = cell.get_text(strip=True)

                # Store the cell link, if applicable.
                if len(cell.find_all("a")) > 1:
                    logger.warning(
                        f"Multiple links found in column {column_name}, "
                        f"cell: {cell.get_text(strip=True)}"
                    )
                link = cell.find("a")
                if link and "href" in link.attrs:
                    row_data[f"{column_name}_href"] = (
                        str(link["href"]) if link["href"] else None
                    )

        data.append(row_data)
    return data


def main() -> None:
    """Fetch and process data from Poolbay algorithm list."""
    logger.info(f"Starting {Path(__file__).name} main()")

    df_list: list[pl.DataFrame] = []

    for page_num in range(1, 8):  # Only 5 pages right now.
        logger.info(f"Fetching page {page_num}")
        url = f"https://poolbay.io/coins?page={page_num}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()

        df = _extract_table_data(r.text)
        df_list.append(pl.DataFrame(df))

    df = pl.concat(df_list, how="diagonal")
    del df_list

    df = df.unique()

    df = pl_df_all_common_str_cleaning(df)

    # df = DySchemaMinerstatCoins.validate(df, cast=True)

    step_1_output_folder.mkdir(parents=True, exist_ok=True)
    df.write_parquet(step_1_output_folder / "poolbay_coins.parquet")
    df.write_csv(step_1_output_folder / "poolbay_coins.csv")

    logger.info(f"Finished {Path(__file__).name} main(). Final shape: {df.shape}")


if __name__ == "__main__":
    main()

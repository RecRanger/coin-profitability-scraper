"""Tool to fetch and process data from Minerstat algorithm list."""

from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup
from loguru import logger

from coin_profitability_scraper.util import download_as_bytes, write_tables

step_1a_output_folder_path = Path("./out/minerstat/") / Path(__file__).stem


def _fetch_minerstat_page() -> bytes:
    """Fetch the HTML content of the Minerstat algorithms page."""
    url = "https://minerstat.com/algorithms"
    return download_as_bytes(url)


def load_minerstat_table_from_html(
    html_content: bytes, *, url_col_name: str = "url"
) -> pl.DataFrame:
    """Process the HTML content to extract the Minerstat algorithms table.

    Has a unit test.
    """
    soup = BeautifulSoup(html_content.decode(), "html.parser")

    # Find the table container.
    table = soup.find("div", class_="box_table")
    if not table:
        msg = "Could not find the table in the provided HTML content."
        raise ValueError(msg)

    # Extract rows (div.tr)
    rows = table.find_all("div", class_="tr")

    # The first row contains headers.
    header_divs = rows[0].find_all("div", class_="th")
    headers = [h.get_text(strip=True) for h in header_divs]

    # We'll add a new column for algorithm links.
    headers.append(url_col_name)

    # Data rows.
    data: list[list[str]] = []
    for row in rows[1:]:
        cols = row.find_all("div", class_="td")
        row_data: list[str] = []
        algo_link = ""

        for col in cols:
            # Capture the algorithm link if this is the algorithm column
            if "flexListAlgo" in col.attrs.get("class", []):
                a_tag = col.find("a", href=True)
                if a_tag:
                    algo_link_element = a_tag["href"]
                    if algo_link_element:
                        assert isinstance(algo_link_element, str)
                        assert algo_link_element.startswith("/algorithm/")
                        algo_link = f"https://minerstat.com{algo_link_element}"

            # If the column has tags (like <a> or <div> with class "tag"),
            # collect their text.
            tags = col.find_all(["a", "div"], class_="tag")
            if tags:
                cell_text = ", ".join(tag.get_text(strip=True) for tag in tags)
            else:
                cell_text = col.get_text(strip=True)

            row_data.append(cell_text)

        row_data.append(algo_link)  # Add the link as the last column.
        data.append(row_data)

    # Create a Polars DataFrame.
    df = pl.DataFrame(data, schema=headers, orient="row")

    # Sanity check.
    if df.height != len(data):
        msg = "DataFrame height does not match the number of data rows extracted."
        raise RuntimeError(msg)

    return df


def transform_add_extra_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Add columns for easier analysis."""
    df = df.with_columns(
        hardware_asic=pl.col("Hardware").str.contains("ASIC"),
        hardware_amd=pl.col("Hardware").str.contains("AMD"),
        hardware_nvidia=pl.col("Hardware").str.contains("NVIDIA"),
        algo_slug=pl.col("url").str.split("/").list.get(-1),
    )
    return df  # noqa: RET504


def main() -> None:
    """Fetch and process Minerstat algorithm data."""
    step_1a_output_folder_path.mkdir(parents=True, exist_ok=True)

    html_content_path = step_1a_output_folder_path / "minerstat_algorithms.html"

    html_content = _fetch_minerstat_page()
    logger.info(f"Fetched Minerstat page content: {len(html_content):,} bytes.")

    if len(html_content) < 50_000:  # noqa: PLR2004
        if html_content_path.exists():
            logger.warning(
                f"Downloaded content is unexpectedly small ({len(html_content)}). "
                f"Using previously saved content of size "
                f"{html_content_path.stat().st_size} bytes."
            )
            html_content = html_content_path.read_bytes()
        else:
            msg = (
                f"Downloaded content is unexpectedly small ({len(html_content)}), "
                "and no previous content exists to fall back on."
            )
            raise RuntimeError(msg)
    else:
        # Store the raw HTML content.
        (step_1a_output_folder_path / "minerstat_algorithms.html").write_bytes(
            html_content
        )
        logger.info("Saved Minerstat HTML content.")

    assert len(html_content) > 50_000, (  # noqa: PLR2004
        f"Downloaded content is unexpectedly small ({len(html_content)})"
    )

    # Process the HTML content as needed.
    df = load_minerstat_table_from_html(html_content)
    logger.info(f"Processed Minerstat algorithms table: {df.height} rows.")

    df = transform_add_extra_columns(df)

    write_tables(df, "minerstat_algorithms", step_1a_output_folder_path)

    logger.info("Minerstat data processing complete.")


if __name__ == "__main__":
    main()

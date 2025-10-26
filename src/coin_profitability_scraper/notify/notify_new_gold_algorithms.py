"""Tool for notifying about newly discovered gold algorithms."""

import argparse
import os
import time
from pathlib import Path
from typing import Literal

import arrow
import dataframely as dy
import polars as pl
import requests
from loguru import logger

from coin_profitability_scraper import is_dry_run
from coin_profitability_scraper.dolt_updater import DoltDatabaseUpdater
from coin_profitability_scraper.dolt_util import DOLT_REPO_URL

NTFY_URL = "https://ntfy.sh/{topic_name}"

store_folder = Path(__file__).parent / "out"
preview_folder = store_folder / "preview"
preview_folder.mkdir(exist_ok=True, parents=True)

logger.add(store_folder / "app.log", rotation="10 MB")


class DySchemaNotifyLogNewAlgorithms(dy.Schema):
    """Schema `notify_log_new_algorithms` table."""

    algo_name = dy.String(
        primary_key=True, nullable=False, min_length=1, max_length=100
    )


def fetch_table_by_name(table_name: Literal["gold_algorithms"]) -> pl.DataFrame:
    """Fetch a table by its name from the DoltHub repository."""
    res = requests.get(
        f"https://www.dolthub.com/csv/recranger/cryptocurrency-coin-algo-data/main/{table_name}",
        timeout=30,
    )
    res.raise_for_status()
    return pl.read_csv(res.content)


def notify_new_algorithms(df: pl.DataFrame) -> None:
    """Send a notification via ntfy.sh about newly discovered algorithms."""
    max_notifications = 5
    if df.is_empty():
        logger.info(f"No new algorithms to notify. Shape: {df.shape}")
        return

    intro_message = f"🧠 Detected {len(df)} new algorithms! See following messages!"
    if df.height > max_notifications:
        intro_message += f" (only showing first {max_notifications} of {df.height})"
    send_ntfy_notification(intro_message)
    del intro_message

    indent4 = " " * 4

    for idx, row in enumerate(df.head(5).iter_rows(named=True), start=1):
        message = f"🧠 New algorithm (#{idx}/{df.height}): ✨ {row['algo_name']} ✨\n"
        message += f"{indent4}- Tracked since {row['duration_since_algo_created']}.\n"
        if row.get("asic_count"):
            message += f"{indent4}- 🔴 ASIC Count: {row['asic_count']}\n"
        else:
            message += f"{indent4}- 🟢 No ASICs reported. You could be the first!\n"

        message += "\n"

        message += "Details:\n"
        for key, val in row.items():
            if (
                key not in {"algo_name", "duration_since_algo_created", "asic_count"}
                and val is not None
            ):
                if isinstance(val, float | int):
                    message += f"{indent4}- {key}: {val:,}\n"
                else:
                    message += f"{indent4}- {key}: {val}\n"

        send_ntfy_notification(message)


def send_ntfy_notification(message: str) -> None:
    """Send a notification via ntfy.sh."""
    logger.debug(f"Notification message ({len(message):,} bytes):\n{message}")
    if is_dry_run():
        logger.info("Dry run mode: not sending notification.")
        return

    try:
        res = requests.post(
            NTFY_URL.format(topic_name=os.environ["NTFY_TOPIC_NAME"]),
            data=message.encode(),
            timeout=25,
        )
        res.raise_for_status()
        logger.info(f"Notification sent: {res.status_code}. {len(message):,} bytes.")
    except Exception as e:  # noqa: BLE001
        logger.error(f"Failed to send notification: {e}")


def _transform_gold_algorithms_df(df: pl.DataFrame) -> pl.DataFrame:
    # Cast date columns safely.
    df = df.with_columns(
        pl.col(col).cast(pl.Date)
        for col in [
            "earliest_coin_created_at",
            "latest_coin_created_at",
            "earliest_asic_announcement_date",
            "earliest_asic_launch_date",
            "earliest_asic_created_at",
            "latest_asic_created_at",
        ]
        if col in df.columns
    )

    # Humanize the duration since algorithm creation.
    df = df.with_columns(
        pl.col("created_at")
        .map_elements(
            lambda d: arrow.get(d).humanize(arrow.utcnow()) if d else "unknown",
            return_dtype=pl.String,
        )
        .alias("duration_since_algo_created")
    )
    return df


def check_and_send_notifications() -> None:
    """Check for new algorithms and send notifications if any are found.

    Main function to call in a loop.
    """
    logger.info("Starting data fetch...")
    with DoltDatabaseUpdater(DOLT_REPO_URL) as dolt:
        df = dolt.read_table_to_polars("gold_algorithms")
        df_known_algos = dolt.read_table_to_polars("notify_log_new_algorithms")

    assert isinstance(df, pl.DataFrame)  # pyright: ignore[reportPossiblyUnboundVariable]
    assert isinstance(df_known_algos, pl.DataFrame)  # pyright: ignore[reportPossiblyUnboundVariable]

    df = _transform_gold_algorithms_df(df)

    # Store a preview of the newest algorithms.
    df_newest_algos = df.sort("created_at", descending=True)
    df_newest_algos.write_parquet(preview_folder / "newest_algorithms.parquet")

    # Load previously known algorithms.
    known_algos: set[str] = set(df_known_algos["algo_name"].unique().to_list())

    # Detect new algorithms.
    current_algos = set(df["algo_name"].to_list())
    new_algos = sorted(current_algos - known_algos)

    if new_algos:
        logger.info(f"Detected {len(new_algos)} new algorithms: {new_algos}")
        notify_new_algorithms(
            df_newest_algos.filter(pl.col("algo_name").is_in(new_algos))
        )
    else:
        logger.info("No new algorithms detected.")

    # Save the full current list for future runs.
    df = df.select("algo_name")
    df = DySchemaNotifyLogNewAlgorithms.validate(df, cast=True)
    df.write_parquet(store_folder / "notify_log_new_algorithms.parquet")
    logger.info("Stored updated algorithm list.")

    # Call the dolt pushing tool.
    from coin_profitability_scraper.step_9_dolt_write import (  # noqa: PLC0415
        main as dolt_write_main,
    )

    dolt_write_main(("notify_log_new_algorithms",))


def main() -> None:
    """Run main entry point."""
    parser = argparse.ArgumentParser(
        description="Check and send notifications (once or as a daemon)."
    )
    parser.add_argument(
        "-d",
        "--daemon",
        action="store_true",
        help="Run continuously in a loop instead of once.",
    )
    args = parser.parse_args()

    if args.daemon:
        while True:
            try:
                check_and_send_notifications()
            except Exception as e:  # noqa: BLE001
                logger.error(f"Error during check: {e}")
            logger.info("Sleeping for 1 hour before next check...")
            time.sleep(3600)
    else:
        check_and_send_notifications()


if __name__ == "__main__":
    main()

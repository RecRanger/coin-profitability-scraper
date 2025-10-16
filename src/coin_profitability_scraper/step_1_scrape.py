"""Step 1: Scrape coin pages from CryptoSlate."""

import queue
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar

import backoff
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from loguru import logger

ua = UserAgent()

step_1_html_folder_path = Path("./out/downloaded_pages/")
start_timestamp = datetime.now(UTC)


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=60,
    max_tries=10,
    on_backoff=lambda x: logger.warning(f"Retrying download: {x}"),
)
def download_as_bytes(url: str) -> bytes:
    """Download the given URL and return the content as bytes."""
    response = requests.get(url, headers={"User-Agent": ua.random}, timeout=120)
    return response.content


def extract_next_button_urls(html_content: str | bytes) -> list[str]:
    """Extract the URLs from the "Next" buttons on a page."""
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        next_links = soup.find_all("a", text=re.compile(r"Next \d+"))
        return [str(link.get("href")) for link in next_links if link.get("href")]

    except Exception as e:  # noqa: BLE001
        logger.error(f"Parse error for next button: {e}")

    return []


def is_direct_coin_url(url: str) -> bool:
    """Check if the URL is a direct coin URL."""
    return bool(re.match(r"^https://cryptoslate\.com/coins/[^/]+/?$", url))


def main() -> None:
    """Scrape coin pages."""
    # Create a folder to store downloaded pages.
    step_1_html_folder_path.mkdir(parents=True, exist_ok=True)

    top_level_urls_queue: ScrapeQueue[str] = ScrapeQueue(
        [
            "https://cryptoslate.com/cryptos/proof-of-work/",
            "https://cryptoslate.com/coins/?show=all",
        ]
    )
    coin_urls_queue: ScrapeQueue[str] = ScrapeQueue()

    while len(top_level_urls_queue) > 0:
        url = top_level_urls_queue.pop()

        html_content = download_as_bytes(url)

        # Parse out the coins and add them to the queue_store.
        soup = BeautifulSoup(html_content, "html.parser")
        new_coin_urls: list[str] = [
            str(a["href"]).rstrip(".")
            for a in soup.find_all("a", href=True)
            if is_direct_coin_url(str(a["href"]))
        ]
        coin_urls_queue.extend(new_coin_urls)

        next_button_urls = extract_next_button_urls(html_content)
        top_level_urls_queue.extend(next_button_urls)

        tl_completed_count = top_level_urls_queue.completed_count()
        logger.info(
            f'Processed top-level URL #{tl_completed_count}: "{url}". '
            f"Got {len(new_coin_urls)} coins and {len(next_button_urls)} next buttons."
        )

        # Scrape the coin pages.
        while len(coin_urls_queue) > 0:
            coin_url = coin_urls_queue.pop()

            completed_count = coin_urls_queue.completed_count()
            if completed_count % 50 == 0:
                coins_per_second = (
                    completed_count
                    / (datetime.now(UTC) - start_timestamp).total_seconds()
                )
                logger.info(
                    f'Processing coin URL #{completed_count}: "{coin_url}". '
                    f"{coins_per_second:.2f} coins per second."
                )

            page_content = download_as_bytes(coin_url)

            (step_1_html_folder_path / (coin_url.split("/")[-2] + ".html")).write_bytes(
                page_content,
            )
            # TODO: write the scrape date as a comment to the file for retrieval later

    logger.info(
        f"Download complete. {top_level_urls_queue.total_count()} top-level URLs "
        f"and {coin_urls_queue.total_count():,} coin URLs processed."
    )


T = TypeVar("T")


class ScrapeQueue[T]:
    """A queue which can be added to and popped from, but silently ignores duplicates.

    Keeps track of total items added, items completed, and items remaining.
    """

    def __init__(self, init_items: Iterable[T] = ()) -> None:
        """Initialize the queue with optional initial items."""
        self.queue: queue.Queue[T] = queue.Queue()
        self.unique_set: set[T] = set()

        # Add the init items.
        self.extend(init_items)

    def push(self, item: T) -> None:
        """Add an item to the queue if it is not already present."""
        if item not in self.unique_set:
            self.queue.put(item)
            self.unique_set.add(item)

    def pop(self) -> T:
        """Remove and return an item from the queue.

        Raises IndexError if the queue is empty.
        """
        if not self.queue.empty():
            return self.queue.get()

        msg = "pop from an empty queue"
        raise IndexError(msg)

    def extend(self, items: Iterable[T]) -> None:
        """Add multiple items to the queue."""
        for item in items:
            self.push(item)

    def total_count(self) -> int:
        """Return the total number of unique items added to the queue."""
        return len(self.unique_set)

    def completed_count(self) -> int:
        """Return the number of items that have been processed."""
        return self.total_count() - self.remaining_count()

    def remaining_count(self) -> int:
        """Return the number of items remaining in the queue."""
        return self.queue.qsize()

    def __len__(self) -> int:
        """Return the number of items in the queue."""
        return self.queue.qsize()

    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise."""
        return self.queue.empty()


if __name__ == "__main__":
    main()

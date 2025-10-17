"""Test step_1_scrape_data.py functions."""

from typing import Any

from coin_profitability_scraper.miningnow.step_1_scrape_data import (
    extract_valid_json_substrings,
)


def test_extract_valid_json_substrings() -> None:
    """Test extract_valid_json_substrings()."""
    test_str = 'Some text {"key1": "value1"} some more text {"key2": 123, "key3": [1, 2, 3]} end text'
    expected_outputs: list[dict[str, Any]] = [
        {"key1": "value1"},
        {"key2": 123, "key3": [1, 2, 3]},
    ]

    outputs = extract_valid_json_substrings(test_str)
    assert outputs == expected_outputs

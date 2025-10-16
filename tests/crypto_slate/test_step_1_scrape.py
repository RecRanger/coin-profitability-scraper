"""Unit tests for the step_1_scrape module."""

from coin_profitability_scraper.crypto_slate.step_1_scrape import is_direct_coin_url


def test_is_direct_coin_url() -> None:
    """Test the is_direct_coin_url function."""
    assert is_direct_coin_url("https://cryptoslate.com/coins/bitcoin/") is True
    assert is_direct_coin_url("https://cryptoslate.com/coins/ethereum") is True
    assert is_direct_coin_url("https://cryptoslate.com/coins/") is False
    assert is_direct_coin_url("https://cryptoslate.com/tokens/bitcoin/") is False
    assert is_direct_coin_url("https://example.com/coins/bitcoin/") is False
    assert is_direct_coin_url("https://cryptoslate.com/coins/bitcoin/extras") is False
    assert is_direct_coin_url("https://cryptoslate.com/coins") is False
    assert (
        is_direct_coin_url("https://cryptoslate.com/coins/bitcoin/something/else")
        is False
    )

    # Specific examples to avoid.
    assert is_direct_coin_url("https://cryptoslate.com/coins/page/14/") is False
    assert is_direct_coin_url("https://cryptoslate.com/coins/?show=all") is False

    # Amp pages must be excluded.
    assert is_direct_coin_url("https://cryptoslate.com/coins/amp") is False
    assert is_direct_coin_url("https://cryptoslate.com/coins/amp/") is False
    assert is_direct_coin_url("https://cryptoslate.com/coins/bitcoin/amp") is False
    assert is_direct_coin_url("https://cryptoslate.com/coins/bitcoin/amp/") is False

"""Test algorithm aliasing operations."""

import polars as pl

from coin_profitability_scraper.reports.aliases import (
    pre_mapping_normalize_algorithm_names,
)


def test_pre_mapping_normalize_algorithm_names() -> None:
    """Test the pre-mapping algorithm name normalization."""
    df = pl.DataFrame(
        [
            {"input": "CryptoNight", "expected": "CryptoNight"},
            {"input": "CryptoNight Heavy", "expected": "CryptoNight-Heavy"},
            {"input": "Cryptonight V7", "expected": "CryptoNight-V7"},
            {"input": "KawPow", "expected": "KawPow"},
            {"input": "EquiHash 96,5", "expected": "Equihash(96,5)"},
            {"input": "EquiHash 96_15", "expected": "Equihash(96,15)"},
        ]
    )

    df = df.with_columns(
        result=pre_mapping_normalize_algorithm_names(pl.col("input")),
    ).with_columns(
        is_pass=pl.col("result").eq_missing(pl.col("expected")),
    )

    df_fail = df.filter(pl.col("is_pass") == pl.lit(False, pl.Boolean))
    if df_fail.height > 0:
        print(df_fail)  # noqa: T201

    assert len(df_fail) == 0

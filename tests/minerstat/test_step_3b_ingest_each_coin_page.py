"""Tests for step_3_ingest_each_coin_page.py."""

from bs4 import BeautifulSoup

from coin_profitability_scraper.minerstat.step_3b_ingest_each_coin_page import (
    _extract_key_value_pairs,  # pyright: ignore[reportPrivateUsage]
)


def test__extract_key_value_pairs() -> None:
    """Test the _extract_key_value_pairs() function."""
    html_content = """
  <div class="box">
<table>
   <tr>
      <th class="label">Data</td>
      <th class="value">Value</td>
   </tr>
   <tr>
      <td class="label coin_type algorithm">Algorithm:</td>
      <td class="value"><a href="/algorithm/sha-256" title="SHA-256">SHA-256</a></td>
   </tr>
   <tr>
      <td class="label coin_type difficulty">BCH difficulty:</td>
      <td class="value">687.031G</td>
   </tr>
   <tr>
      <td class="label coin_type block_reward">BCH block reward:</td>
      <td class="value">3.1279</td>
   </tr>
   <tr>
      <td class="label coin_type volume">BCH 24h volume:</td>
      <td class="value">504,025,790.86 USD</td>
   </tr>
   <tr>
      <td class="label coin_type revenue">Price for 1 BCH:</td>
      <td class="value">504.27 USD</td>aa
   </tr>
   <tr>
      <td class="label coin_type founded">Founded:</td>
      <td class="value">2017</td>
   </tr>
</table>
"""
    soup = BeautifulSoup(html_content, "html.parser")
    result = _extract_key_value_pairs(soup)

    assert result == {
        "algorithm": "SHA-256",
        "difficulty": "687.031G",
        "block_reward": "3.1279",
        "volume": "504,025,790.86 USD",
        "revenue": "504.27 USD",
        "founded": "2017",
    }

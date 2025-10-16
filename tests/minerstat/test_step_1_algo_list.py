"""Tests for load_minerstat_data.py."""

from coin_profitability_scraper.minerstat.step_1_algo_list import (
    load_minerstat_table_from_html,
)


def test_load_minerstat_table_from_html() -> None:
    """Test the `load_minerstat_table_from_html()` function."""
    sample_html = """
<html><body>

<div class="box_table">
   <div class="tr">
      <div class="flexListId th">#</div>
      <div class="flexListAlgo th">Algorithm</div>
      <div class="flexListCoins th rmv1">Coins</div>
      <div class="flexListClients th rmv1">Clients</div>
      <div class="flexListHardware th rmv1">Hardware</div>
      <div class="flexListSoftware th rmv1">System</div>
   </div>
   <div class="tr">
      <div class="flexListId td">1</div>
      <div class="flexListAlgo td" data-responsive="Algorithm"><a href="/algorithm/allium" title="Allium">Allium</a></div>
      <div class="flexListCoins td rmv1" data-responsive="Coins">1</div>
      <div class="flexListClients td rmv1" data-responsive="Clients">1</div>
      <div class="flexListHardware td rmv1" data-responsive="Hardware">
         <div class="tag">CPU</div>
      </div>
      <div class="flexListSoftware td rmv1" data-responsive="System"><a href="/software/mining-os" class="tag blue">msOS</a><a href="/software/windows" class="tag blue">Windows</a></div>
   </div>
   <div class="tr">
      <div class="flexListId td">2</div>
      <div class="flexListAlgo td" data-responsive="Algorithm"><a href="/algorithm/argon2d" title="Argon2d">Argon2d</a></div>
      <div class="flexListCoins td rmv1" data-responsive="Coins">1</div>
      <div class="flexListClients td rmv1" data-responsive="Clients">0</div>
      <div class="flexListHardware td rmv1" data-responsive="Hardware"></div>
      <div class="flexListSoftware td rmv1" data-responsive="System"></div>
   </div>
</div>

</body></html>
"""
    df = load_minerstat_table_from_html(sample_html.encode())

    assert df.height == 2  # noqa: PLR2004
    assert df.columns == [
        "#",
        "Algorithm",
        "Coins",
        "Clients",
        "Hardware",
        "System",
        "url",
    ]

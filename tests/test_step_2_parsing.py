"""Tests for step_2_parsing.py."""

from datetime import date

from bs4 import BeautifulSoup

from coin_profitability_scraper.step_2_parse_scrape import (
    _extract_technical_key_value_from_soup,  # pyright: ignore[reportPrivateUsage]
    _get_earliest_logo_date_from_soup,  # pyright: ignore[reportPrivateUsage]
)


def test__get_earliest_logo_date_from_soup() -> None:
    """Test the _get_earliest_logo_date_from_soup function."""
    html_content = """
    <html><body>
        <div class='name-logo'>
            <div class='logo-container'> <noscript><img class='logo' alt="Agoras: Currency of Tau"
                  src="https://cryptoslate.com/wp-content/themes/cryptoslate-2020/imgresize/timthumb.php?src=https://cryptoslate.com/wp-content/uploads/2017/10/Agoras-logo.jpg&amp;w=100&amp;h=100&amp;q=75"
                  srcset="https://cryptoslate.com/wp-content/themes/cryptoslate-2020/imgresize/timthumb.php?src=https://cryptoslate.com/wp-content/uploads/2017/10/Agoras-logo.jpg&amp;w=150&amp;h=150&amp;q=75 1.5x, https://cryptoslate.com/wp-content/themes/cryptoslate-2020/imgresize/timthumb.php?src=https://cryptoslate.com/wp-content/uploads/2017/10/Agoras-logo.jpg&amp;w=200&amp;h=200&amp;q=75 2x"></noscript><img
                class='lazyload logo' alt="Agoras: Currency of Tau"
                src='data:image/svg+xml,%3Csvg%20xmlns=%22http://www.w3.org/2000/svg%22%20viewBox=%220%200%20210%20140%22%3E%3C/svg%3E'
                data-src="https://cryptoslate.com/wp-content/themes/cryptoslate-2020/imgresize/timthumb.php?src=https://cryptoslate.com/wp-content/uploads/2017/10/Agoras-logo.jpg&amp;w=100&amp;h=100&amp;q=75"
                data-srcset="https://cryptoslate.com/wp-content/themes/cryptoslate-2020/imgresize/timthumb.php?src=https://cryptoslate.com/wp-content/uploads/2017/10/Agoras-logo.jpg&amp;w=150&amp;h=150&amp;q=75 1.5x, https://cryptoslate.com/wp-content/themes/cryptoslate-2020/imgresize/timthumb.php?src=https://cryptoslate.com/wp-content/uploads/2017/10/Agoras-logo.jpg&amp;w=200&amp;h=200&amp;q=75 2x">
            </div>
            <div class='coin-info'>
              <div class='rank'> Rank #<span class="holepunch holepunch-coin_rank">747</span></div>
              <h1> <span class='ticker'>AGRS</span> <span class='name'>Agoras: Currency of Tau</span></h1>
            </div>
          </div>
    </body></html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    result = _get_earliest_logo_date_from_soup(soup, coin_slug="test-coin")
    assert result == date(2017, 10, 1), f"Expected date(2017, 10, 1), but got {result}"


def test__extract_technical_key_value_from_soup() -> None:
    """Test the _extract_technical_key_value_from_soup() function."""
    html_content = """
<html><body>

<div class='col col-2'>
              <div class='widget technical'>
                <header>
                  <h2>0x Technical Info</h2>
                </header>
                <ul class='list'>
                  <li> <span class='info'>Blockchain</span> <span class='value'> <a
                        href="https://cryptoslate.com/blockchain/ethereum/" rel="tag">Ethereum</a> </span></li>
                  <li class=''> <span class='info'>Consensus</span> <span class='value' title='Not mineable'>Not
                      mineable</span></li>
                  <li> <span class='info'>Hash Algorithm</span> <span class='value' title='None'>None</span></li>
                  <li> <span class='info'>Org. Structure</span> <span class='value'
                      title='Centralized'>Centralized</span></li>
                  <li class='hidden'> <span class='info'>Open Source</span> <span class='value' title='1'>1</span></li>
                  <li> <span class='info'>Development Status</span> <span class='value' title='Working product'>Working
                      product</span></li>
                  <li> <span class='info'>Open Source</span> <span class='value' title='Yes'>Yes</span></li>
                  <li> <span class='info'>Hard Wallet Support</span> <span class='value' title='Yes'>Yes</span></li>
                </ul>
              </div>
            </div>

</body></html>
"""

    soup = BeautifulSoup(html_content, "html.parser")
    result = _extract_technical_key_value_from_soup(soup, coin_slug="test-coin")
    assert result == {
        "Blockchain": "Ethereum",
        "Consensus": "Not mineable",
        "Hash Algorithm": "None",
        "Org. Structure": "Centralized",
        "Open Source": "Yes",
        "Development Status": "Working product",
        "Hard Wallet Support": "Yes",
    }


def test__extract_technical_key_value_from_soup_list_formatting() -> None:
    """Test the _extract_technical_key_value_from_soup() function.

    The "Blockchain" field has a list of links, which we will turn into a CSV field.
    """
    html_content = """
<html><body>

<div class="col col-2"><div class="widget technical"><header><h2>USDC  Technical Info</h2></header><ul class="list">
<li>
 <span class="info">Blockchain</span> <span class="value"> <a href="https://cryptoslate.com/blockchain/algorand/" rel="tag">Algorand</a><a href="https://cryptoslate.com/blockchain/arbitrum/" rel="tag">Arbitrum</a><a href="https://cryptoslate.com/blockchain/avalanche/" rel="tag">Avalanche</a><a href="https://cryptoslate.com/blockchain/ethereum/" rel="tag">Ethereum</a><a href="https://cryptoslate.com/blockchain/flow/" rel="tag">Flow</a><a href="https://cryptoslate.com/blockchain/solana/" rel="tag">Solana</a><a href="https://cryptoslate.com/blockchain/stellar/" rel="tag">Stellar</a><a href="https://cryptoslate.com/blockchain/tron/" rel="tag">TRON</a> </span></li>
<li>
 <span class="info">Hash Algorithm</span> <span class="value" title="None">None</span></li>
<li>
 <span class="info">Org. Structure</span> <span class="value" title="Centralized">Centralized</span></li>
<li>
 <span class="info">Development Status</span> <span class="value" title="Working product">Working product</span></li>
<li>
 <span class="info">Hard Wallet Support</span> <span class="value" title="Yes">Yes</span></li></ul></div></div>

</body></html>
"""

    soup = BeautifulSoup(html_content, "html.parser")
    result = _extract_technical_key_value_from_soup(soup, coin_slug="test-coin")
    assert result == {
        "Blockchain": "Algorand, Arbitrum, Avalanche, Ethereum, Flow, Solana, Stellar, TRON",
        "Hash Algorithm": "None",
        "Org. Structure": "Centralized",
        "Development Status": "Working product",
        "Hard Wallet Support": "Yes",
    }

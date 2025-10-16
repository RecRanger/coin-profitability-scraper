"""Tests for step_2_parsing.py."""

from datetime import date

from bs4 import BeautifulSoup

from coin_profitability_scraper.step_2_parse_scrape import (
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

from datetime import date

import pandas as pd
import pytest


def make_prices(closes, ticker="TLV.RO", start="2024-01-01"):
    days = pd.bdate_range(start, periods=len(closes)).date
    return pd.DataFrame({
        "ticker": ticker, "trade_date": days,
        "open": closes, "high": [c * 1.01 for c in closes], "low": [c * 0.99 for c in closes],
        "close": closes, "adj_close": closes, "volume": [1000] * len(closes),
    })


@pytest.fixture
def window():
    return date(2023, 1, 2), date(2024, 6, 28)

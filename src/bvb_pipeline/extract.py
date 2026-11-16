"""Extract daily OHLCV prices into a tidy long-format DataFrame.

Output columns: ticker, trade_date, open, high, low, close, adj_close, volume
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

PRICE_COLUMNS = ["ticker", "trade_date", "open", "high", "low", "close", "adj_close", "volume"]


def from_yahoo(tickers: list[str], start: date, end: date) -> pd.DataFrame:
    """Download prices from Yahoo Finance for [start, end] inclusive."""
    import yfinance as yf  # imported lazily: tests and the synthetic source don't need network access

    raw = yf.download(
        tickers,
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),  # yfinance's end is exclusive
        auto_adjust=False,
        group_by="ticker",
        progress=False,
        threads=True,
    )
    if raw.empty:
        log.warning("Yahoo returned no rows for %s..%s", start, end)
        return pd.DataFrame(columns=PRICE_COLUMNS)

    frames = []
    for ticker in tickers:
        if ticker not in raw.columns.get_level_values(0):
            log.warning("No data for %s", ticker)
            continue
        df = raw[ticker].dropna(how="all").reset_index()
        df = df.rename(
            columns={
                "Date": "trade_date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume",
            }
        )
        df["ticker"] = ticker
        frames.append(df)

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=PRICE_COLUMNS)
    out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.date
    return out[PRICE_COLUMNS]


def synthetic(tickers: list[str], start: date, end: date, seed: int = 42) -> pd.DataFrame:
    """Deterministic random-walk prices on business days: offline demos and tests."""
    rng = np.random.default_rng(seed)
    days = pd.bdate_range(start, end)
    frames = []
    for i, ticker in enumerate(tickers):
        returns = rng.normal(0.0004, 0.015, len(days))
        close = (20 + 10 * i) * np.exp(np.cumsum(returns))
        open_ = close * (1 + rng.normal(0, 0.004, len(days)))
        high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.006, len(days))))
        low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.006, len(days))))
        frames.append(
            pd.DataFrame(
                {
                    "ticker": ticker,
                    "trade_date": days.date,
                    "open": open_.round(4),
                    "high": high.round(4),
                    "low": low.round(4),
                    "close": close.round(4),
                    "adj_close": close.round(4),
                    "volume": rng.integers(10_000, 2_000_000, len(days)),
                }
            )
        )
    return pd.concat(frames, ignore_index=True)[PRICE_COLUMNS]


SOURCES = {"yahoo": from_yahoo, "synthetic": synthetic}

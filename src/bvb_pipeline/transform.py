"""Technical indicators per ticker, computed on the cleaned price history."""

from __future__ import annotations

import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252


def compute_indicators(prices: pd.DataFrame) -> pd.DataFrame:
    """Input: clean daily prices (any number of tickers). Output: one row per ticker/day with
    returns, moving averages, 52-week drawdown, annualised volatility and signal flags."""
    df = prices.sort_values(["ticker", "trade_date"]).copy()
    g = df.groupby("ticker", group_keys=False)["adj_close"]

    df["daily_return"] = g.pct_change()
    df["sma_20"] = g.transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["sma_50"] = g.transform(lambda s: s.rolling(50, min_periods=50).mean())
    df["sma_200"] = g.transform(lambda s: s.rolling(200, min_periods=200).mean())
    df["high_52w"] = g.transform(lambda s: s.rolling(TRADING_DAYS_PER_YEAR, min_periods=1).max())
    df["drawdown_52w"] = df["adj_close"] / df["high_52w"] - 1
    df["volatility_20d"] = df.groupby("ticker", group_keys=False)["daily_return"].transform(
        lambda s: s.rolling(20, min_periods=20).std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    )

    # Same alert rule as the BOT-BVB project: meaningful drawdown while below the long-term trend
    df["below_sma_200"] = df["sma_200"].notna() & (df["adj_close"] < df["sma_200"])
    df["drawdown_alert"] = df["below_sma_200"] & (df["drawdown_52w"] <= -0.08)
    df["golden_cross"] = (df["sma_50"] > df["sma_200"]) & (
        df.groupby("ticker")["sma_50"].shift() <= df.groupby("ticker")["sma_200"].shift()
    )

    cols = ["ticker", "trade_date", "adj_close", "daily_return", "sma_20", "sma_50", "sma_200",
            "high_52w", "drawdown_52w", "volatility_20d", "below_sma_200", "drawdown_alert", "golden_cross"]
    return df[cols].reset_index(drop=True)

import numpy as np
import pandas as pd
import pytest
from conftest import make_prices

from bvb_pipeline.transform import compute_indicators


def test_returns_and_moving_average():
    out = compute_indicators(make_prices([float(x) for x in range(1, 26)]))

    assert np.isnan(out.loc[0, "daily_return"])
    assert out.loc[1, "daily_return"] == pytest.approx(1.0)       # 1 -> 2
    assert out["sma_20"].iloc[18] != out["sma_20"].iloc[18]       # NaN before 20 observations
    assert out["sma_20"].iloc[19] == pytest.approx(10.5)          # mean of 1..20


def test_drawdown_from_52_week_high():
    out = compute_indicators(make_prices([100.0, 120.0, 90.0]))
    assert out["high_52w"].tolist() == [100.0, 120.0, 120.0]
    assert out["drawdown_52w"].iloc[-1] == pytest.approx(-0.25)


def test_drawdown_alert_needs_200_day_history_and_a_real_drop():
    rising = [100 + i * 0.5 for i in range(220)]
    crash = [rising[-1] * f for f in (0.95, 0.90, 0.80, 0.70)]
    out = compute_indicators(make_prices(rising + crash))

    assert not out["drawdown_alert"].iloc[:220].any()
    last = out.iloc[-1]
    assert last["below_sma_200"]
    assert last["drawdown_52w"] == pytest.approx(-0.30)
    assert last["drawdown_alert"]


def test_tickers_are_computed_independently():
    a = make_prices([10.0, 20.0], ticker="AAA.RO")
    b = make_prices([50.0, 25.0], ticker="BBB.RO")
    out = compute_indicators(pd.concat([b, a])).set_index("ticker")

    assert out.loc["AAA.RO", "daily_return"].iloc[1] == pytest.approx(1.0)
    assert out.loc["BBB.RO", "daily_return"].iloc[1] == pytest.approx(-0.5)

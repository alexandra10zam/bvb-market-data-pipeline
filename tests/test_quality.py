import numpy as np
import pandas as pd
from conftest import make_prices

from bvb_pipeline.quality import validate


def test_clean_data_passes_untouched():
    report = validate(make_prices([10.0, 10.5, 11.0]))
    assert len(report.clean) == 3
    assert report.rejected.empty
    assert report.reject_ratio == 0


def test_each_rule_rejects_with_reason():
    df = make_prices([10.0] * 5)
    df.loc[0, "close"] = np.nan                 # missing_price
    df.loc[1, "low"] = -1                       # non_positive_price
    df.loc[2, ["high", "low"]] = [9.0, 9.5]     # high_below_low
    df.loc[3, "close"] = 20.0                   # close_outside_range
    df.loc[4, "volume"] = -5                    # negative_volume

    report = validate(df)

    assert report.clean.empty
    assert list(report.rejected["reason"]) == [
        "missing_price", "non_positive_price", "high_below_low", "close_outside_range", "negative_volume",
    ]
    assert report.summary()["rejected"] == 5


def test_duplicates_keep_latest_row():
    df = make_prices([10.0, 11.0])
    dup = df.iloc[[1]].assign(close=10.95, adj_close=10.95)
    report = validate(pd.concat([df, dup], ignore_index=True))

    assert report.duplicates_dropped == 1
    assert len(report.clean) == 2
    assert report.clean.iloc[-1]["close"] == 10.95


def test_missing_volume_is_filled_with_zero():
    df = make_prices([10.0])
    df["volume"] = df["volume"].astype("float")
    df.loc[0, "volume"] = np.nan
    assert validate(df).clean.loc[0, "volume"] == 0

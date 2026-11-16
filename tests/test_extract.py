from datetime import date

from bvb_pipeline.extract import PRICE_COLUMNS, synthetic
from bvb_pipeline.quality import validate


def test_synthetic_is_deterministic_and_valid():
    a = synthetic(["TLV.RO", "H2O.RO"], date(2024, 1, 1), date(2024, 3, 29))
    b = synthetic(["TLV.RO", "H2O.RO"], date(2024, 1, 1), date(2024, 3, 29))

    assert list(a.columns) == PRICE_COLUMNS
    assert a.equals(b)
    assert set(a["ticker"]) == {"TLV.RO", "H2O.RO"}
    assert all(d.weekday() < 5 for d in a["trade_date"])
    assert validate(a).rejected.empty

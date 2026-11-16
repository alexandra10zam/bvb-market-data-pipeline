"""Data-quality gate: split extracted rows into clean and rejected (with a reason per row)."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

# Rule name -> predicate returning True for BAD rows
RULES = {
    "missing_price": lambda d: d[["open", "high", "low", "close"]].isna().any(axis=1),
    "non_positive_price": lambda d: (d[["open", "high", "low", "close"]] <= 0).any(axis=1),
    "high_below_low": lambda d: d["high"] < d["low"],
    "close_outside_range": lambda d: (d["close"] > d["high"] * 1.001) | (d["close"] < d["low"] * 0.999),
    "negative_volume": lambda d: d["volume"].fillna(0) < 0,
}


@dataclass
class QualityReport:
    clean: pd.DataFrame
    rejected: pd.DataFrame
    duplicates_dropped: int

    @property
    def total(self) -> int:
        return len(self.clean) + len(self.rejected)

    @property
    def reject_ratio(self) -> float:
        return len(self.rejected) / self.total if self.total else 0.0

    def summary(self) -> dict[str, int]:
        counts = self.rejected["reason"].value_counts().to_dict() if len(self.rejected) else {}
        return {"clean": len(self.clean), "rejected": len(self.rejected),
                "duplicates_dropped": self.duplicates_dropped, **counts}


def validate(df: pd.DataFrame) -> QualityReport:
    before = len(df)
    df = df.drop_duplicates(subset=["ticker", "trade_date"], keep="last").reset_index(drop=True)
    duplicates = before - len(df)

    reason = pd.Series(pd.NA, index=df.index, dtype="object")
    for name, is_bad in RULES.items():
        reason = reason.mask(reason.isna() & is_bad(df), name)  # first failing rule wins

    bad = reason.notna()
    rejected = df[bad].assign(reason=reason[bad]).reset_index(drop=True)
    clean = df[~bad].reset_index(drop=True)
    clean["volume"] = clean["volume"].fillna(0).astype("int64")
    return QualityReport(clean=clean, rejected=rejected, duplicates_dropped=duplicates)

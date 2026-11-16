import os
from dataclasses import dataclass, field

# Blue chips from the BET index, as listed on Yahoo Finance
DEFAULT_TICKERS = [
    "TLV.RO",   # Banca Transilvania
    "H2O.RO",   # Hidroelectrica
    "SNP.RO",   # OMV Petrom
    "SNG.RO",   # Romgaz
    "BRD.RO",   # BRD - Groupe Société Générale
    "SNN.RO",   # Nuclearelectrica
    "TGN.RO",   # Transgaz
    "DIGI.RO",  # Digi Communications
    "M.RO",     # MedLife
    "EL.RO",    # Electrica
]


@dataclass(frozen=True)
class Settings:
    database_url: str = field(
        default_factory=lambda: os.getenv(
            "BVB_DATABASE_URL", "postgresql+psycopg://bvb:bvb@localhost:5433/bvb"
        )
    )
    # Fail the run if more than this share of extracted rows is rejected by quality checks
    max_reject_ratio: float = float(os.getenv("BVB_MAX_REJECT_RATIO", "0.2"))
    # Calendar days of history re-read to compute rolling indicators (covers a 252-trading-day window)
    indicator_lookback_days: int = 400

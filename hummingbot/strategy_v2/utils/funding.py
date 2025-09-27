from __future__ import annotations

from decimal import Decimal
from typing import Dict, Optional

# Common fallbacks for funding interval seconds by connector name
FUNDING_INTERVAL_FALLBACKS: Dict[str, int] = {
    "bybit_perpetual": 60 * 60 * 8,
    "hyperliquid_perpetual": 60 * 60 * 1,
    "okx_perpetual": 60 * 60 * 8,
}


def normalized_funding_rate_in_seconds(
    funding_info,
    connector: str,
    fallbacks: Optional[Dict[str, int]] = None,
) -> Optional[Decimal]:
    """
    Convert funding_info.rate into a per-second rate using funding_info.funding_interval if present,
    otherwise fall back to a predefined interval per connector.

    Returns Decimal or None if unavailable.
    """
    try:
        rate = Decimal(str(funding_info.rate))
    except Exception:
        return None

    if rate.is_nan():
        return None

    interval = getattr(funding_info, "funding_interval", None)
    try:
        if interval and interval > 0:
            # funding_interval is in minutes; convert to seconds
            seconds = Decimal(str(interval)) * Decimal("60")
        else:
            fb = (fallbacks or FUNDING_INTERVAL_FALLBACKS)
            fallback_seconds = fb.get(connector, 60 * 60)
            seconds = Decimal(str(fallback_seconds))
    except Exception:
        seconds = Decimal("3600")

    if seconds <= 0:
        return None

    return rate / seconds


def minutes_to_next_funding(next_funding_timestamp: Optional[int], current_ts: float) -> Optional[Decimal]:
    """
    Compute minutes until the next funding event from current timestamp.
    """
    if next_funding_timestamp is None:
        return None
    try:
        delta = Decimal(str(next_funding_timestamp - current_ts))
        return delta / Decimal("60")
    except Exception:
        return None


def funding_diff_pct(entry_rate_per_sec: Decimal, hedge_rate_per_sec: Decimal, hours: int = 24) -> Optional[Decimal]:
    """
    Compute funding rate differential in percent (%) over a given horizon (hours).
    percent = (hedge_rate_sec - entry_rate_sec) * (hours * 3600) * 100

    Notes:
    - This is equivalent to funding_diff_bp(...) / 100.
    - Returned value is in human percentage units (e.g., 0.25 means 0.25%).
    """
    try:
        horizon_seconds = Decimal(str(hours)) * Decimal("3600")
        diff = (hedge_rate_per_sec - entry_rate_per_sec) * horizon_seconds
        return diff * Decimal("100")
    except Exception:
        return None

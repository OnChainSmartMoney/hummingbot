from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional, Union

from hummingbot.core.data_type.common import TradeType

if TYPE_CHECKING:
    from hummingbot.strategy_v2.executors.maker_hedge_single_executor.maker_hedge_single_executor import MakerHedgeSingleExecutor

from hummingbot.strategy_v2.utils.funding import (
    funding_diff_pct as util_funding_diff_pct,
    normalized_funding_rate_in_seconds as util_normalized_funding_rate_in_seconds,
)


class FundingHelper:
    def __init__(self, executor: 'MakerHedgeSingleExecutor'):
        self.exe = executor
        self._funding_below_start_ts: Optional[float] = None
        self._funding_last_diff_pct: Optional[Decimal] = None
        self._funding_exit_triggered: bool = False

    @property
    def funding_last_diff_pct(self):
        return self._funding_last_diff_pct

    def get_normalized_funding_rates(self) -> Optional[Dict[str, Union[Decimal, None]]]:
        try:
            mdp = getattr(self.exe._strategy, "market_data_provider", None)
            if mdp is None:
                return None
            entry_fi = mdp.get_funding_info(self.exe.maker_connector, self.exe.maker_pair)
            hedge_fi = mdp.get_funding_info(self.exe.hedge_connector, self.exe.hedge_pair)
            if entry_fi is None or hedge_fi is None:
                return None
            entry_sec = util_normalized_funding_rate_in_seconds(entry_fi, self.exe.maker_connector)
            hedge_sec = util_normalized_funding_rate_in_seconds(hedge_fi, self.exe.hedge_connector)
            return {"entry": entry_sec, "hedge": hedge_sec}
        except Exception:
            return None

    def _get_funding_diff_pct(self, funding_interval_hours: Optional[int] = None) -> Optional[Decimal]:
        try:
            rates = self.get_normalized_funding_rates()
            if rates is None:
                self.exe.logger().warning("[Funding monitoring] Rates unavailable")
                return None
            entry_sec = rates.get("entry")
            hedge_sec = rates.get("hedge")
            if entry_sec is None or hedge_sec is None:
                self.exe.logger().info("Funding rates unavailable")
                return None
            funding_profitability_interval_hours = self.exe.funding_profitability_interval_hours if funding_interval_hours is None else funding_interval_hours
            diff_pct = util_funding_diff_pct(entry_sec, hedge_sec, hours=funding_profitability_interval_hours)
            if diff_pct is None:
                return None
            return Decimal(str(diff_pct))
        except Exception:
            return None

    def get_oriented_funding_diff_pct(self, funding_interval_hours: Optional[int] = None) -> Optional[Decimal]:
        try:
            diff_pct_raw = self._get_funding_diff_pct(funding_interval_hours)
            if diff_pct_raw is None:
                self.exe.logger().info("Funding rates unavailable")
                return None
            try:
                side_factor = Decimal("1") if self.exe.side_maker == TradeType.BUY else Decimal("-1")
            except Exception:
                side_factor = Decimal("1")
            try:
                oriented_diff_pct = Decimal(str(diff_pct_raw)) * side_factor
            except Exception:
                oriented_diff_pct = diff_pct_raw
            return oriented_diff_pct
        except Exception:
            return None

    def monitor_and_maybe_trigger_exit(self, now_ts: float):
        try:
            oriented_diff_pct = self.get_oriented_funding_diff_pct()
            if oriented_diff_pct is None:
                self.exe.logger().info("Funding rates unavailable")
                return None
            self._funding_last_diff_pct = oriented_diff_pct
            self.exe.logger().info(
                f"[Funding monitoring] {self.exe.maker_pair} oriented_diff_pct(for position maker={'LONG' if self.exe.side_maker == TradeType.BUY else 'SHORT'}): {oriented_diff_pct}"
            )

            pct_threshold = self.exe.exit_funding_diff_pct_threshold
            hold_sec = self.exe.exit_hold_below_sec

            if oriented_diff_pct <= pct_threshold:
                if self._funding_below_start_ts is None:
                    self._funding_below_start_ts = now_ts
                    self.exe.logger().info(
                        f"[Funding monitoring] diff_pct={oriented_diff_pct:.6f}% <= {pct_threshold}% ; starting hold timer {hold_sec}s"
                    )
                else:
                    elapsed = now_ts - self._funding_below_start_ts
                    if elapsed >= float(hold_sec) and not self._funding_exit_triggered:
                        self._funding_exit_triggered = True
                        self.start_closing_due_to_funding(oriented_diff_pct, pct_threshold, hold_sec)
            else:
                if self._funding_below_start_ts is not None:
                    self.exe.logger().info(
                        f"[Funding monitoring] diff_pct back above threshold: {oriented_diff_pct:.6f}% > {pct_threshold}%; resetting timer"
                    )
                self._funding_below_start_ts = None
        except Exception as e:
            self.exe.logger().warning(f"[Funding monitoring] error: {e}")

    def start_closing_due_to_funding(self, diff_pct: Decimal, threshold: Decimal, hold_sec: int):
        self.exe.logger().info(
            f"[Funding exit] Sustained low funding diff_pct={diff_pct:.6f}% <= {threshold}% for {hold_sec}s; starting CLOSE sequence"
        )
        self.exe._closing = True
        self.exe._build_close_queue_if_needed()
        self._funding_below_start_ts = None

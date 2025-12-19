from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hummingbot.strategy_v2.executors.maker_hedge_single_executor.maker_hedge_single_executor import (
        MakerHedgeSingleExecutor,
    )

from hummingbot.core.data_type.common import PositionMode


class RiskHelper:
    def __init__(self, executor: "MakerHedgeSingleExecutor"):
        self.exe = executor

    def ensure_position_mode(self) -> bool:
        exe = self.exe
        try:
            hedge_market = exe._strategy.connectors[exe.hedge_connector]
            mode = getattr(hedge_market, "position_mode", None)
            desired_mode = getattr(
                exe, "_desired_hedge_position_mode", PositionMode.HEDGE
            )
            if desired_mode == PositionMode.HEDGE and mode != PositionMode.HEDGE:
                try:
                    exe._strategy.set_position_mode(
                        exe.hedge_connector, PositionMode.HEDGE
                    )
                except Exception:
                    pass
                exe.logger().warning(
                    f"[Position mode] {exe.hedge_connector} is not in HEDGE mode yet. Waiting before placing orders..."
                )
                return False
            return True
        except Exception:
            exe.logger().warning(
                f"[Position mode] Unable to verify for {exe.hedge_connector}. Waiting..."
            )
            return False

    def apply_leverage_once(self):
        exe = self.exe
        if exe._leverage_applied:
            return
        try:
            lev = int(exe.leverage) if getattr(exe, "leverage", None) is not None else 1
            if lev <= 0:
                lev = 1
            ok1 = ok2 = True
            try:
                exe._strategy.set_leverage(exe.maker_connector, exe.maker_pair, lev)
            except Exception as e:
                ok1 = False
                exe.logger().warning(
                    f"[Leverage] Failed to set {lev} on {exe.maker_connector}:{exe.maker_pair}: {e}"
                )
            try:
                exe._strategy.set_leverage(exe.hedge_connector, exe.hedge_pair, lev)
            except Exception as e:
                ok2 = False
                exe.logger().warning(
                    f"[Leverage] Failed to set {lev} on {exe.hedge_connector}:{exe.hedge_pair}: {e}"
                )
            exe._leverage_applied = ok1 and ok2
        except Exception as e:
            exe.logger().warning(f"[Leverage] Unexpected error applying leverage: {e}")

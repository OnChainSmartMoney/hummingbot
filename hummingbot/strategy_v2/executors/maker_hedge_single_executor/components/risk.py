from __future__ import annotations

from typing import TYPE_CHECKING

from hummingbot.core.utils.async_utils import safe_ensure_future

if TYPE_CHECKING:
    from hummingbot.strategy_v2.executors.maker_hedge_single_executor.maker_hedge_single_executor import (
        MakerHedgeSingleExecutor,
    )

from hummingbot.core.data_type.common import PositionMode


class RiskHelper:
    def __init__(self, executor: "MakerHedgeSingleExecutor"):
        self.exe = executor

    def _leverage_bounds_from_trading_rules(
        self, market: object, trading_pair: str
    ) -> tuple[int | None, int | None]:
        try:
            rules = getattr(market, "trading_rules", None)
            if not isinstance(rules, dict) or len(rules) == 0:
                return None, None

            rule = rules.get(trading_pair)
            if rule is None:
                return None, None

            min_lev = None
            max_lev = None
            try:
                raw_min = getattr(rule, "min_leverage", None)
                if raw_min is not None:
                    min_lev = int(raw_min)
            except Exception:
                min_lev = None
            try:
                raw_max = getattr(rule, "max_leverage", None)
                if raw_max is not None:
                    max_lev = int(raw_max)
            except Exception:
                max_lev = None

            if min_lev is None and max_lev is None:
                try:
                    bounds = getattr(market, "_leverage_bounds_by_trading_pair", None)
                    if isinstance(bounds, dict) and trading_pair in bounds:
                        b_min, b_max = bounds[trading_pair]
                        if b_min is not None:
                            min_lev = int(b_min)
                        if b_max is not None:
                            max_lev = int(b_max)
                except Exception:
                    pass

            return min_lev, max_lev
        except Exception:
            return None, None

    def _can_apply_leverage(
        self, market: object, trading_pair: str, leverage: int
    ) -> bool:
        if leverage <= 1:
            return True

        if not hasattr(market, "set_leverage"):
            return False

        min_lev, max_lev = self._leverage_bounds_from_trading_rules(
            market, trading_pair
        )

        if min_lev is not None and leverage < min_lev:
            return False
        if max_lev is not None and leverage > max_lev:
            return False
        return True

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

    def apply_leverage(self):
        exe = self.exe
        if exe._leverage_applied:
            return
        try:
            lev = int(exe.leverage) if getattr(exe, "leverage", None) is not None else 1
            if lev <= 0:
                lev = 1

            max_attempts = 5
            if bool(getattr(exe, "_leverage_apply_gave_up", False)):
                return

            attempts = int(getattr(exe, "_leverage_apply_attempts", 0) or 0) + 1
            setattr(exe, "_leverage_apply_attempts", attempts)

            def _give_up():
                setattr(exe, "_leverage_apply_gave_up", True)
                exe.logger().warning(
                    f"[Leverage] Failed to apply leverage after {attempts}/{max_attempts} attempts. Stopping executor."
                )
                try:
                    safe_ensure_future(
                        exe._publish_cant_apply_leverage_event(
                            reason="maximum attempts reached"
                        )
                    )
                finally:
                    exe.early_stop()

            maker_market = exe._strategy.connectors.get(exe.maker_connector)
            hedge_market = exe._strategy.connectors.get(exe.hedge_connector)
            if maker_market is None or hedge_market is None:
                exe.logger().warning(
                    f"[Leverage] Unable to access connector(s) {exe.maker_connector} or {exe.hedge_connector}. Waiting..."
                )
                if attempts >= max_attempts:
                    _give_up()
                return

            ok_maker = self._can_apply_leverage(maker_market, exe.maker_pair, lev)
            ok_hedge = self._can_apply_leverage(hedge_market, exe.hedge_pair, lev)
            if not ok_maker or not ok_hedge:
                exe.logger().warning(
                    f"[Leverage] Cannot apply {lev} on {exe.maker_connector}:{exe.maker_pair} or {exe.hedge_connector}:{exe.hedge_pair}. Waiting..."
                )
                if attempts >= max_attempts:
                    _give_up()
                return

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
            if ok1 and ok2:
                exe._leverage_applied = True
                setattr(exe, "_leverage_apply_attempts", 0)
                setattr(exe, "_leverage_apply_gave_up", False)
            else:
                if attempts >= max_attempts:
                    _give_up()
        except Exception as e:
            exe.logger().warning(f"[Leverage] Unexpected error applying leverage: {e}")

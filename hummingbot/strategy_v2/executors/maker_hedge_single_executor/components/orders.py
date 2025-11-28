from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Optional

from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

if TYPE_CHECKING:
    from hummingbot.strategy_v2.executors.maker_hedge_single_executor.maker_hedge_single_executor import (
        MakerHedgeSingleExecutor,
    )


class OrdersHelper:
    def __init__(self, executor: 'MakerHedgeSingleExecutor'):
        self.exe = executor

    def compute_limit_price(self, side: TradeType) -> Optional[Decimal]:
        price_type = PriceType.BestBid if side == TradeType.BUY else PriceType.BestAsk
        base_price = self.exe.get_price(self.exe.maker_connector, self.exe.maker_pair, price_type)
        self.exe.logger().info(f"[Order] Base price: {base_price}")
        if base_price.is_nan() or base_price <= 0:
            return None

        off = (self.exe.maker_price_offset_pct / Decimal("100"))
        if side == TradeType.BUY:
            self.exe.logger().info(f"[Order] Limit price: {base_price * (Decimal("1") - off)}")
            return base_price * (Decimal("1") - off)
        else:
            self.exe.logger().info(f"[Order] Limit price: {base_price * (Decimal("1") + off)}")
            return base_price * (Decimal("1") + off)

    async def validate_sufficient_balance(self):
        mid = self.exe.get_price(self.exe.maker_connector, self.exe.maker_pair, PriceType.MidPrice)
        if mid.is_nan() or mid <= 0:
            self.exe.logger().info("[Validation] Entry mid price unavailable; proceeding but orders may fail.")
            return
        base, quote = self.exe.maker_pair.split("-")

        self.exe.logger().info(f"[Config] pair_notional_usd_cap: {self.exe.pair_notional_usd_cap}, per_order_max_notional_usd: {self.exe.per_order_max_notional_usd}, leverage: {self.exe.leverage}")
        remaining_cap = self.exe.get_remaining_maker_cap()
        self.exe.logger().info(f"[Validation] Sufficient {quote} margin for next order; remaining cap ${remaining_cap:.2f}")
        if not self.exe.is_enough_maker_cap(remaining_cap):
            self.exe.logger().warning(f"[Validation] Not enough remaining maker cap to place new order, remaining cap: ${remaining_cap:.2f}, need at least: ${self.exe.per_order_min_notional_usd:.2f}. Completing executor.")
            return

        next_notional_usd = min(remaining_cap, Decimal(str(self.exe.per_order_max_notional_usd)))
        leverage = self.exe.leverage if getattr(self.exe, "leverage", None) is not None else Decimal("1")
        if leverage <= 0:
            leverage = Decimal("1")
        required_margin = Decimal(str(next_notional_usd)) / leverage
        self.exe.logger().info(f"[Validation] connector {self.exe.maker_connector} requires ~{required_margin:.6f} {quote}")
        avail_quote = self.exe.get_available_balance(self.exe.maker_connector, quote)
        if avail_quote < required_margin:
            self.exe.logger().warning(
                f"[Validation] Insufficient {quote} margin: need {required_margin:.6f} (next_notional {next_notional_usd:.6f}/lev {leverage}), have {avail_quote:.6f}."
            )

    async def place_next_part(self):
        exe = self.exe
        maker_side: TradeType = exe._get_maker_side_for_mode()

        if exe._closing:
            exe._build_close_queue_if_needed()
            if exe._closing_current is not None:
                return
            if not exe._close_queue:
                exe._finalize_hedge_tail()
                exe.logger().info("[Close] Queue is empty; finishing executor.")
                exe.close_type = CloseType.COMPLETED
                exe.stop()
                return
            item = exe._close_queue.pop(0)
            open_id = item["open_id"]
            raw_amount = item["amount"]
            mode_desc = "CLOSE"
        else:
            remaining_cap = exe.get_remaining_maker_cap()
            if remaining_cap <= 0:
                exe._finalize_hedge_tail()
                exe.logger().info("[Order] Pair notional USD cap fully consumed; completing executor.")
                exe.close_type = CloseType.COMPLETED
                exe.stop()
                return
            px_preview = self.compute_limit_price(maker_side)
            if px_preview is None or px_preview <= 0:
                return
            next_notional_usd = min(remaining_cap, Decimal(str(exe.per_order_max_notional_usd)))
            exe.logger().info(f"[Order] Next notional USD: ${next_notional_usd:.2f} at limit px {px_preview:.8f}")
            raw_amount = Decimal(str(next_notional_usd)) / px_preview
            maker_min_notional = exe.maker_min_notional_usd
            if maker_min_notional and next_notional_usd < maker_min_notional:
                exe.logger().info(f"[Order] Skip maker OPEN: below min notional ${maker_min_notional}")
                return
            mode_desc = "OPEN"

        try:
            entry_conn = exe._strategy.connectors[exe.maker_connector]
            amt_q_maker = entry_conn.quantize_order_amount(exe.maker_pair, raw_amount)
            hedge_conn = exe._strategy.connectors[exe.hedge_connector]
            amt_q = hedge_conn.quantize_order_amount(exe.hedge_pair, amt_q_maker)
        except Exception:
            amt_q = raw_amount
        amount = amt_q

        px = self.compute_limit_price(maker_side)
        if px is None or px <= 0:
            return
        if not exe._closing:
            planned_notional = amount * px
            rem_cap_now = exe.get_remaining_maker_cap()
            if planned_notional > rem_cap_now:
                try:
                    entry_conn = exe._strategy.connectors[exe.maker_connector]
                    adjusted = entry_conn.quantize_order_amount(exe.maker_pair, amount * Decimal("0.999"))
                    if adjusted > 0:
                        amount = adjusted
                except Exception:
                    pass
        if amount <= 0:
            exe.logger().warning("[Order] Computed amount <= 0; skipping")
            exe._opening_fully_completed = True
            return

        is_profitability_check_passed = exe._check_profitability_enter_condition(maker_side, px, amount)
        exe.logger().info(f"[Order] Placing maker {mode_desc} qty={amount} @ {px:.8f} (profitability check: {'pass' if is_profitability_check_passed else 'fail'})")

        now_ts = int(exe._strategy.current_timestamp)
        wait_sec = float(exe.non_profitable_wait_sec)

        if exe._profitability_helper.is_profitable_on_last_check is None:
            exe._profitability_helper.is_profitable_on_last_check = True
        if not exe._profitability_helper.last_profitable_ts:
            exe._profitability_helper.last_profitable_ts = now_ts

        if exe._closing:
            close_wait_sec = float(exe.closing_non_profitable_wait_sec)
            if exe._closing_no_wait:
                exe.logger().info("[Close] No-wait flag set; bypassing profitability wait due to risk trigger.")
                try:
                    exe._closing_wait_started_ts = None
                except Exception:
                    pass
            else:
                if not is_profitability_check_passed:
                    start_ts = exe._closing_wait_started_ts
                    if start_ts is None:
                        exe._closing_wait_started_ts = now_ts
                        exe.logger().info(f"[Close] Not profitable yet; starting up-to-{int(close_wait_sec)}s wait before forcing close.")
                        exe._next_order_ready_ts = now_ts + 1.0
                        return
                    elapsed_close = now_ts - start_ts
                    if elapsed_close < close_wait_sec:
                        remaining = max(0, int(close_wait_sec - elapsed_close))
                        exe.logger().info(f"[Close] Not profitable yet; waiting... remaining {remaining}s before forced close.")
                        exe._next_order_ready_ts = now_ts + 5.0
                        return
                    else:
                        exe.logger().info(f"[Close] Waited {int(elapsed_close)}s; forcing close without profitability condition.")
                else:
                    try:
                        exe._closing_wait_started_ts = None
                    except Exception:
                        pass
        else:
            if not is_profitability_check_passed:
                elapsed = now_ts - exe._profitability_helper.last_profitable_ts
                if not exe._profitability_helper.is_profitable_on_last_check:
                    if elapsed >= wait_sec:
                        if not exe.is_any_position_open():
                            exe.logger().info(f"[Profitability] {exe.maker_pair} wait time exceeded ({elapsed:.0f}s >= {wait_sec:.0f}s); no open orders, stopping executor.")
                            exe.early_stop()
                            return
                        exe._opening_fully_completed = True
                        exe.logger().info(f"[Profitability] {exe.maker_pair} wait time exceeded ({elapsed:.0f}s >= {wait_sec:.0f}s); entering funding monitoring.")
                    else:
                        exe.logger().info(f"[Profitability] {exe.maker_pair} still not profitable; waiting {wait_sec - elapsed:.0f}s more.")
                    return
                exe._profitability_helper.is_profitable_on_last_check = False
                exe.logger().info(f"[Profitability] {exe.maker_pair} not profitable; starting wait window {wait_sec:.0f}s.")
                return

        exe._profitability_helper.is_profitable_on_last_check = True
        exe._profitability_helper.last_profitable_ts = now_ts

        if (not exe._closing) and (not exe._profitability_helper.should_open_positions):
            exe.logger().info("[Config] Not opening new positions as per configuration")
            return

        order_id = exe.place_order(
            connector_name=exe.maker_connector,
            trading_pair=exe.maker_pair,
            order_type=OrderType.LIMIT_MAKER,
            side=maker_side,
            amount=amount,
            position_action=(PositionAction.CLOSE if exe._closing else PositionAction.OPEN),
            price=px,
        )
        new_tracked = TrackedOrder(order_id=order_id)

        if exe._closing:
            exe._closing_current = {
                "open_id": open_id,
                "close_order_id": order_id,
                "placed_amount": amount,
                "executed_base": Decimal("0"),
                "creation_ts": exe._strategy.current_timestamp,
            }
            exe.logger().info(f"[Close] Placed maker CLOSE (LIMIT marketable) for open_id={open_id} -> close_id={order_id} px={px} amt={amount}")
            try:
                exe._closing_no_wait = False
            except Exception:
                pass
        else:
            exe.add_order(new_tracked, is_maker=True)
            exe._maker_pending_ids.add(order_id)
            cooldown = float(exe.post_place_cooldown_sec)
            exe._next_order_ready_ts = exe._strategy.current_timestamp + cooldown
            exe.logger().info(f"[Order] Placed maker {mode_desc} id={order_id} type=LIMIT px={px} amt={amount}")

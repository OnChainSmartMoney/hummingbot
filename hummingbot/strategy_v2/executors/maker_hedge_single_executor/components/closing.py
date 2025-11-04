from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List

from hummingbot.core.data_type.common import PositionAction

if TYPE_CHECKING:
    from hummingbot.strategy_v2.executors.maker_hedge_single_executor.maker_hedge_single_executor import (
        MakerHedgeSingleExecutor,
    )


class ClosingHelper:
    def __init__(self, executor: 'MakerHedgeSingleExecutor'):
        self.exe = executor

    def build_close_queue_if_needed(self):
        exe = self.exe
        if exe._close_queue:
            return
        queue: List[Dict] = []

        def _fmt_tracked(o) -> Dict:
            ord_obj = getattr(o, "order", None)

            def _safe(attr, default=None):
                try:
                    return getattr(ord_obj, attr, default)
                except Exception:
                    return default
            pos = _safe("position")
            try:
                pos_str = pos.name if hasattr(pos, "name") else str(pos)
            except Exception:
                pos_str = str(pos)
            return {
                "order_id": getattr(o, "order_id", None),
                "exchange_order_id": _safe("exchange_order_id"),
                "trading_pair": _safe("trading_pair"),
                "position": pos_str,
                "is_open": _safe("is_open"),
                "amount": _safe("amount"),
                "executed_amount_base": getattr(o, "executed_amount_base", None),
                "price": _safe("price"),
                "average_executed_price": getattr(o, "average_executed_price", None),
                "creation_timestamp": _safe("creation_timestamp"),
            }

        try:
            summaries = [_fmt_tracked(o) for o in exe._maker_orders]
            self.exe.logger().info(f"[Close queue] Building FIFO close queue from existing OPEN maker orders: {summaries}")
        except Exception:
            self.exe.logger().info(f"[Close queue] Building FIFO close queue from {len(exe._maker_orders)} maker orders")

        for o in exe._maker_orders:
            order = o.order
            try:
                self.exe.logger().info(f"[Close queue] Candidate: {_fmt_tracked(o)}")
            except Exception:
                pass
            if order is None:
                continue
            try:
                if order.position == PositionAction.OPEN and (o.executed_amount_base or Decimal("0")) > 0:
                    queue.append({"open_id": o.order_id, "amount": o.executed_amount_base})
            except Exception:
                continue
        exe._close_queue = queue
        try:
            exe.logger().info(f"[Close queue] Built FIFO: {[{'open_id': it['open_id'], 'amount': str(it['amount'])} for it in exe._close_queue]}")
        except Exception:
            exe.logger().info(f"[Close queue] Built FIFO: {exe._close_queue}")

    def finalize_hedge_tail(self):
        if self.exe._hedge_helper:
            self.exe._hedge_helper.finalize_tail()

    def handle_close_ttl(self, now: float):
        exe = self.exe
        if not (exe._closing and exe._closing_current and (exe.config.maker_ttl_sec and exe.config.maker_ttl_sec > 0)):
            return
        close_creation_ts = exe._closing_current.get("creation_ts", 0) or 0
        if close_creation_ts > 0 and (now - close_creation_ts) >= float(exe.config.maker_ttl_sec):
            try:
                close_id = exe._closing_current.get("close_order_id")
                placed = exe._closing_current.get("placed_amount", Decimal("0"))
                executed = exe._closing_current.get("executed_base", Decimal("0"))
                remaining = placed - executed
                if remaining > 0 and close_id:
                    exe.logger().info(f"[TTL] CLOSE exceeded for {close_id}; cancel and re-quote remaining {remaining}.")
                    pair = exe.maker_pair
                    exe._strategy.cancel(exe.maker_connector, pair, close_id)
                    exe._close_queue.append({"open_id": exe._closing_current.get("open_id"), "amount": remaining})
                else:
                    exe.logger().info(f"[TTL] CLOSE exceeded for {close_id}; nothing remaining.")
            except Exception as e:
                exe.logger().warning(f"[TTL] Error during CLOSE handling: {e}")
            finally:
                exe._closing_current = None
                exe._next_order_ready_ts = now + 0.2

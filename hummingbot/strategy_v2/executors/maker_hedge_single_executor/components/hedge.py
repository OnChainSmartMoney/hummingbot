from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType
from hummingbot.strategy_v2.models.executors import TrackedOrder

if TYPE_CHECKING:
    from hummingbot.strategy_v2.executors.maker_hedge_single_executor.maker_hedge_single_executor import (
        MakerHedgeSingleExecutor,
    )


class HedgeHelper:
    def __init__(self, executor: 'MakerHedgeSingleExecutor'):
        self.exe = executor

    @property
    def accum_base(self) -> Decimal:
        return self.exe._hedge_accum_base

    def add_to_accumulator(self, amount: Decimal):
        if amount > 0:
            self.exe._hedge_accum_base += amount
            self.try_hedge_accumulated()

    def market_hedge(self, qty_base: Decimal):
        if qty_base <= 0:
            return
        if len(self.exe._hedge_inflight) >= self.exe._max_parallel_hedges:
            return
        order_id = self.exe.place_order(
            connector_name=self.exe.hedge_connector,
            trading_pair=self.exe.hedge_pair,
            order_type=OrderType.MARKET,
            side=(self.exe._opposite_side(self.exe.side_hedge) if self.exe._closing else self.exe.side_hedge),
            amount=qty_base,
            position_action=(PositionAction.CLOSE if self.exe._closing else PositionAction.OPEN),
        )
        self.exe.add_order(TrackedOrder(order_id=order_id), is_maker=False)
        self.exe._hedge_inflight.add(order_id)
        self.exe._hedge_inflight_amounts[order_id] = qty_base
        self.exe._last_hedge_order_id = order_id
        self.exe.logger().info(f"[Hedge] Market sent id={order_id} qty={qty_base} mode={'CLOSE' if self.exe._closing else 'OPEN'} inflight={len(self.exe._hedge_inflight)}")

    def try_hedge_accumulated(self):
        if self.exe._hedge_accum_base <= 0:
            return
        if len(self.exe._hedge_inflight) >= self.exe._max_parallel_hedges:
            return
        try:
            hedge_connector = self.exe._strategy.connectors[self.exe.hedge_connector]
            q_amt = hedge_connector.quantize_order_amount(self.exe.hedge_pair, self.exe._hedge_accum_base)
        except Exception:
            q_amt = self.exe._hedge_accum_base
        if q_amt is None or q_amt <= 0:
            return
        mid = self.exe.get_price(self.exe.hedge_connector, self.exe.hedge_pair, PriceType.MidPrice)
        if mid.is_nan() or mid <= 0:
            return
        notional = q_amt * mid
        min_notional = getattr(self.exe.config, "hedge_min_notional_usd", Decimal("0"))
        if notional < min_notional:
            return
        self.market_hedge(q_amt)
        self.exe._hedge_accum_base -= q_amt

    def finalize_tail(self):
        if self.exe._hedge_accum_base > 0:
            qty = self.exe._hedge_accum_base
            self.exe._hedge_accum_base = Decimal("0")
            self.exe.logger().info(f"[Hedge] Force-flush tail qty={qty}")
            self.market_hedge(qty)

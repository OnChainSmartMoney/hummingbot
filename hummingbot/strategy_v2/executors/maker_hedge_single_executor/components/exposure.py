from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional

from hummingbot.core.data_type.common import PositionAction, PriceType

if TYPE_CHECKING:
    from hummingbot.strategy_v2.executors.maker_hedge_single_executor.maker_hedge_single_executor import MakerHedgeSingleExecutor

from hummingbot.strategy_v2.models.executors import TrackedOrder


class ExposureHelper:
    def __init__(self, executor: 'MakerHedgeSingleExecutor'):
        self.exe = executor

    def maker_remaining_exposure_base(self) -> Decimal:
        open_base = Decimal("0")
        close_base = Decimal("0")
        for o in self.exe._maker_orders:
            order = o.order
            if order is None:
                continue
            try:
                if order.position == PositionAction.OPEN:
                    open_base += (o.executed_amount_base or Decimal("0"))
                elif order.position == PositionAction.CLOSE:
                    close_base += (o.executed_amount_base or Decimal("0"))
            except Exception:
                continue
        remaining = open_base - close_base
        if remaining < 0:
            remaining = Decimal("0")
        return remaining

    def hedge_remaining_exposure_base(self) -> Decimal:
        open_base = Decimal("0")
        close_base = Decimal("0")
        for o in self.exe._hedge_orders:
            order = o.order
            if order is None:
                continue
            try:
                if order.position == PositionAction.OPEN:
                    open_base += (o.executed_amount_base or Decimal("0"))
                elif order.position == PositionAction.CLOSE:
                    close_base += (o.executed_amount_base or Decimal("0"))
            except Exception:
                continue
        remaining = open_base - close_base
        if remaining < 0:
            remaining = Decimal("0")
        return remaining

    def get_full_position_base_amount(self, is_maker: bool = True) -> Decimal:
        orders = self.exe._maker_orders if is_maker else self.exe._hedge_orders
        position = Decimal("0")
        for order in orders:
            exec_base = order.executed_amount_base or Decimal("0")
            if exec_base > 0:
                position += exec_base
        return position

    def get_maker_order_usd_consumed(self) -> Decimal:
        consumed = Decimal("0")
        for order in self.exe._maker_orders:
            if order.order and order.executed_amount_base is not None and order.average_executed_price is not None:
                consumed += order.executed_amount_base * order.average_executed_price
        return consumed

    def get_maker_order_usd_inflight(self) -> Decimal:
        inflight = Decimal("0")
        for order in self.exe._maker_orders:
            o = order.order
            if o is None:
                continue
            try:
                if getattr(o, "is_open", False) and not order.is_filled:
                    total = Decimal(str(getattr(o, "amount", "0") or 0))
                    executed = order.executed_amount_base or Decimal("0")
                    remaining_base = total - executed
                    if remaining_base > 0:
                        px = Decimal(str(getattr(o, "price", "0") or 0))
                        if px > 0:
                            inflight += remaining_base * px
            except Exception:
                continue
        return inflight

    def get_remaining_maker_cap(self) -> Decimal:
        consumed = self.get_maker_order_usd_consumed()
        inflight = self.get_maker_order_usd_inflight()
        remaining_cap = Decimal(str(self.exe.config.pair_notional_usd_cap)) - consumed - inflight
        if remaining_cap < 0:
            remaining_cap = Decimal("0")
        return remaining_cap

    def is_enough_maker_cap(self, remaining_cap: Optional[Decimal]) -> bool:
        if remaining_cap is None:
            remaining_cap = self.get_remaining_maker_cap()
        return remaining_cap >= Decimal(str(self.exe.config.per_order_min_notional_usd))

    def add_fee_quote(self, trade_fee, price_mid: Decimal, pair: str):
        try:
            if trade_fee is None:
                return
            base, quote = pair.split("-")
            flat_fees = getattr(trade_fee, "flat_fees", None) or []
            for ff in flat_fees:
                amt = Decimal(str(getattr(ff, "amount", "0")))
                token = getattr(ff, "token", None) or getattr(ff, "currency", None)
                if amt <= 0:
                    continue
                if token == base and price_mid and price_mid > 0:
                    self.exe._cum_fees_quote += amt * price_mid
                elif token == quote:
                    self.exe._cum_fees_quote += amt
                else:
                    self.exe._cum_fees_quote += amt
        except Exception:
            pass

    def format_tracked_orders(self, orders: List[TrackedOrder]):
        formatted = []
        for o in orders:
            try:
                formatted.append({
                    "id": o.order_id,
                    "ex_id": getattr(o.order, 'exchange_order_id', None) if o.order else None,
                    "pair": getattr(o.order, 'trading_pair', None) if o.order else None,
                    "side": getattr(getattr(o.order, 'trade_type', None), 'name', None) if o.order else None,
                    "type": getattr(getattr(o.order, 'order_type', None), 'name', None) if o.order else None,
                    "px": str(getattr(o.order, 'price', None)) if o.order else None,
                    "amt": str(getattr(o.order, 'amount', None)) if o.order else None,
                    "exec_base": str(o.executed_amount_base),
                    "exec_quote": str(o.executed_amount_quote),
                    "state": getattr(getattr(o.order, 'current_state', None), 'name', None) if o.order else None,
                    "filled": o.is_filled,
                })
            except Exception as e:
                formatted.append({"id": o.order_id, "error": str(e)})
        return formatted

    def build_custom_info_positions(self, funding_stats: Dict) -> Dict:
        try:
            maker_pos_base = self.maker_remaining_exposure_base()
        except Exception:
            maker_pos_base = Decimal("0")
        try:
            hedge_pos_base = self.hedge_remaining_exposure_base()
        except Exception:
            hedge_pos_base = Decimal("0")

        entry_rate_sec = funding_stats.get("entry_rate_sec")
        hedge_rate_sec = funding_stats.get("hedge_rate_sec")
        abs_diff_pct = funding_stats.get("funding_diff_pct")
        oriented_diff_pct = funding_stats.get("funding_oriented_diff_pct")
        minutes_to_funding_entry = funding_stats.get("minutes_to_funding_entry")
        minutes_to_funding_hedge = funding_stats.get("minutes_to_funding_hedge")

        maker_open_orders = self.format_tracked_orders(self.exe._maker_orders)
        hedge_open_orders = self.format_tracked_orders(self.exe._hedge_orders)

        maker_mid = self.exe.get_price(self.exe.maker_connector, self.exe.maker_pair, PriceType.MidPrice)

        return {
            "maker_position_base": maker_pos_base,
            "maker_position_quote": (maker_pos_base * maker_mid),
            "hedge_position_base": hedge_pos_base,
            "funding_entry_rate_sec": entry_rate_sec,
            "funding_hedge_rate_sec": hedge_rate_sec,
            "funding_diff_pct": abs_diff_pct,
            "funding_oriented_diff_pct": oriented_diff_pct,
            "minutes_to_funding_entry": minutes_to_funding_entry,
            "minutes_to_funding_hedge": minutes_to_funding_hedge,
            "maker_open_orders": maker_open_orders,
            "hedge_open_orders": hedge_open_orders,
        }

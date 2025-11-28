from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Union

from hummingbot.core.data_type.common import PositionAction, PriceType
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

if TYPE_CHECKING:
    from hummingbot.strategy_v2.executors.maker_hedge_single_executor.maker_hedge_single_executor import (
        MakerHedgeSingleExecutor,
    )


class EventsHelper:
    def __init__(self, executor: 'MakerHedgeSingleExecutor'):
        self.exe = executor

    def process_order_created(self, event_tag: int, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        exe = self.exe
        if exe._closing_current and event.order_id == exe._closing_current.get("close_order_id"):
            updated = None
            try:
                updated = exe.get_in_flight_order(exe._market_name(market), event.order_id)
            except Exception:
                pass
            if updated:
                to = TrackedOrder(order_id=event.order_id)
                to.order = updated
                exe._maker_by_id[event.order_id] = to
                exe._closing_current["creation_ts"] = getattr(updated, "creation_timestamp", exe._strategy.current_timestamp)
            exe.logger().info(f"[Close] Maker CLOSE acknowledged id={event.order_id}")
            return

        to = exe._maker_by_id.get(event.order_id) or exe._hedge_by_id.get(event.order_id)
        if to:
            updated = None
            try:
                updated = exe.get_in_flight_order(exe._market_name(market), event.order_id)
            except Exception:
                pass
            if updated:
                to.order = updated
                exe.replace_order(to, is_maker=(to in exe._maker_orders))
        exe._maker_pending_ids.discard(event.order_id)

        if event.order_id in exe._hedge_inflight and exe._hedge_helper:
            exe._hedge_helper.try_hedge_accumulated()

    def process_order_filled(self, event_tag: int, market, event: OrderFilledEvent):
        exe = self.exe
        mid_maker = exe.get_price(exe.maker_connector, exe.maker_pair, PriceType.MidPrice)

        market_name = exe._market_name(market)
        if market_name == exe.maker_connector:
            exe._last_fill_ts = float(exe._strategy.current_timestamp)

        if exe._closing_current and event.order_id == exe._closing_current.get("close_order_id"):
            filled_base = Decimal(str(event.amount))
            exe._closing_current["executed_base"] = exe._closing_current.get("executed_base", Decimal("0")) + filled_base
            exe._add_fee_quote(event.trade_fee, mid_maker, exe.maker_pair)
            if exe._hedge_helper:
                exe._hedge_helper.add_to_accumulator(filled_base)
            return

        mo = exe._maker_by_id.get(event.order_id)
        if mo:
            try:
                updated = exe.get_in_flight_order(exe._market_name(market), event.order_id)
                if updated:
                    mo.order = updated
                    exe.replace_order(mo, is_maker=True)
            except Exception:
                pass
            filled_base = Decimal(str(event.amount))
            exe._add_fee_quote(event.trade_fee, mid_maker, exe.maker_pair)
            if exe._hedge_helper:
                exe._hedge_helper.add_to_accumulator(filled_base)
            return

        if event.order_id in exe._hedge_by_id:
            mid_hedge = exe.get_price(exe.hedge_connector, exe.hedge_pair, PriceType.MidPrice)
            exe._add_fee_quote(event.trade_fee, mid_hedge, exe.hedge_pair)

    def process_order_completed(self, event_tag: int, market, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        exe = self.exe
        if event.order_id in exe._hedge_inflight:
            exe._hedge_inflight.discard(event.order_id)
            exe._hedge_inflight_amounts.pop(event.order_id, None)
            if exe._hedge_helper:
                exe._hedge_helper.try_hedge_accumulated()

        exe._maker_pending_ids.discard(event.order_id)

        if exe._closing_current and event.order_id == exe._closing_current.get("close_order_id"):
            open_id = exe._closing_current.get("open_id")
            before = len(exe._maker_orders)
            exe._maker_orders = [o for o in exe._maker_orders if o.order_id != open_id]
            exe._maker_by_id.pop(open_id, None)
            after = len(exe._maker_orders)
            exe.logger().info(f"[Close] Closed open_id={open_id}; removed from maker list. Remaining tracked: {after} (was {before})")
            exe._closing_current = None
            if not exe._close_queue and not any((o.order and o.order.position == PositionAction.OPEN and (o.executed_amount_base or 0) > 0) for o in exe._maker_orders):
                exe._finalize_hedge_tail()
                exe.logger().info("[Close] Queue empty and no remaining maker opens; finishing executor.")
                exe._maker_orders.clear()
                exe._maker_by_id.clear()
                exe.close_type = CloseType.COMPLETED
                exe.stop()
            else:
                exe._next_order_ready_ts = exe._strategy.current_timestamp + float(exe.order_interval_sec)
            return

        if event.order_id in exe._maker_by_id:
            remaining_cap = exe.get_remaining_maker_cap()
            if remaining_cap <= 0:
                exe._finalize_hedge_tail()
                exe.close_type = CloseType.COMPLETED
                exe.stop()
            else:
                exe._next_order_ready_ts = exe._strategy.current_timestamp + float(exe.order_interval_sec)

    def process_order_canceled(self, event_tag: int, market, event: OrderCancelledEvent):
        exe = self.exe
        if exe._closing and exe._closing_current and event.order_id == exe._closing_current.get("close_order_id"):
            placed = exe._closing_current.get("placed_amount", Decimal("0"))
            executed = exe._closing_current.get("executed_base", Decimal("0"))
            remaining = placed - executed
            if remaining > 0:
                exe._close_queue.append({"open_id": exe._closing_current.get("open_id"), "amount": remaining})
                exe.logger().info(f"[Close] Order {event.order_id} canceled; re-queue remaining {remaining} for open_id={exe._closing_current.get('open_id')}")
            else:
                exe.logger().info(f"[Close] Order {event.order_id} canceled; nothing remaining to re-queue.")
            exe._closing_current = None
            exe._maker_pending_ids.discard(event.order_id)
            return

        if event.order_id in exe._maker_by_id:
            to = exe._maker_by_id.get(event.order_id)
            try:
                updated = exe.get_in_flight_order(exe._market_name(market), event.order_id)
                if updated:
                    to.order = updated
            except Exception:
                pass
            exec_base = to.executed_amount_base or Decimal("0")
            if exec_base <= 0:
                exe.remove_order(event.order_id, is_maker=True)
            else:
                exe.logger().info(f"[Cancel] Maker order {event.order_id} canceled with partial fill {exec_base}; keeping for exposure accounting.")
        elif event.order_id in exe._hedge_by_id:
            to = exe._hedge_by_id.get(event.order_id)
            try:
                updated = exe.get_in_flight_order(exe._market_name(market), event.order_id)
                if updated:
                    to.order = updated
            except Exception:
                pass
            exec_base = to.executed_amount_base or Decimal("0")
            if exec_base <= 0:
                exe.remove_order(event.order_id, is_maker=False)
            else:
                exe.logger().info(f"[Cancel] Hedge order {event.order_id} canceled with partial fill {exec_base}; keeping for exposure accounting.")

        exe._hedge_inflight.discard(event.order_id)
        exe._hedge_inflight_amounts.pop(event.order_id, None)
        exe._maker_pending_ids.discard(event.order_id)

    def process_order_failed(self, event_tag: int, market, event: MarketOrderFailureEvent):
        exe = self.exe
        order_id = event.order_id
        err_msg = event.error_message or "unknown error"

        exe._maker_pending_ids.discard(order_id)
        attempted_hedge = exe._hedge_inflight_amounts.pop(order_id, Decimal("0"))
        exe._hedge_inflight.discard(order_id)

        handled = False

        if exe._closing_current and order_id == exe._closing_current.get("close_order_id"):
            handled = True
            placed = exe._closing_current.get("placed_amount", Decimal("0"))
            executed = exe._closing_current.get("executed_base", Decimal("0"))
            remaining = placed - executed
            if remaining > 0:
                exe._close_queue.insert(0, {"open_id": exe._closing_current.get("open_id"), "amount": remaining})
                exe.logger().warning(f"[Failure] Maker CLOSE order {order_id} failed ({err_msg}); re-queued remaining {remaining}.")
            else:
                exe.logger().warning(f"[Failure] Maker CLOSE order {order_id} failed ({err_msg}); nothing remaining to re-queue.")
            exe._closing_current = None
            cooldown = float(exe.post_place_cooldown_sec)
            exe._next_order_ready_ts = exe._strategy.current_timestamp + cooldown

        maker_tracked = exe._maker_by_id.get(order_id)
        if maker_tracked is not None:
            handled = True
            exe.logger().warning(f"[Failure] Maker order {order_id} failed ({err_msg}); scheduling retry.")
            exe.remove_order(order_id, is_maker=True)
            cooldown = float(exe.post_place_cooldown_sec)
            exe._next_order_ready_ts = exe._strategy.current_timestamp + cooldown

        hedge_tracked = exe._hedge_by_id.get(order_id)
        if hedge_tracked is not None:
            handled = True
            hedge_amount = attempted_hedge
            if hedge_amount <= 0 and hedge_tracked.order is not None:
                try:
                    hedge_amount = Decimal(str(hedge_tracked.order.amount))
                except Exception:
                    hedge_amount = Decimal("0")
            exe.remove_order(order_id, is_maker=False)
            if hedge_amount > 0 and exe._hedge_helper:
                exe._hedge_helper.add_to_accumulator(hedge_amount)
            exe.logger().warning(f"[Failure] Hedge order {order_id} failed ({err_msg}); will attempt again.")
            if exe._hedge_helper:
                exe._hedge_helper.try_hedge_accumulated()
        elif attempted_hedge > 0:
            handled = True
            if exe._hedge_helper:
                exe._hedge_helper.add_to_accumulator(attempted_hedge)
            exe.logger().warning(f"[Failure] Recovered hedge accumulator {attempted_hedge} for failed order {order_id}; retrying.")
            if exe._hedge_helper:
                exe._hedge_helper.try_hedge_accumulated()

        if not handled:
            exe.logger().warning(f"[Failure] Order {order_id} failed ({err_msg}); no tracked state was updated.")

    def process_funding_payment(self, event_tag: int, market, event):
        exe = self.exe
        try:
            exe.logger().info(f"[Funding event] received: {event}")
            market_name = exe._market_name(market)
            event_pair = getattr(event, "trading_pair", None)
            if market_name == exe.maker_connector and event_pair == exe.maker_pair:
                leg = "maker"
            elif market_name == exe.hedge_connector and event_pair == exe.hedge_pair:
                leg = "hedge"
            else:
                return
            amount: Decimal = getattr(event, "amount", Decimal("0")) or Decimal("0")
            if leg == "maker":
                exe._cum_funding_maker_quote += Decimal(str(amount))
            else:
                exe._cum_funding_hedge_quote += Decimal(str(amount))
            exe._cum_funding_quote = exe._cum_funding_maker_quote + exe._cum_funding_hedge_quote
            exe.logger().info(
                f"[Funding] {leg} {market_name}:{event.trading_pair} amount={amount} cum_maker={exe._cum_funding_maker_quote} cum_hedge={exe._cum_funding_hedge_quote} net={exe._cum_funding_quote}"
            )
        except Exception as e:
            exe.logger().warning(f"[Funding event] Error processing: {e}")

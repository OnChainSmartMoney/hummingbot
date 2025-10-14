from __future__ import annotations

import json
import logging
from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PriceType, TradeType
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.maker_hedge_single_executor.data_types import MakerHedgeSingleExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder
from hummingbot.strategy_v2.utils.funding import (
    funding_diff_pct as util_funding_diff_pct,
    minutes_to_next_funding as util_minutes_to_next_funding,
    normalized_funding_rate_in_seconds as util_normalized_funding_rate_in_seconds,
)


class MakerHedgeSingleExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy, config: MakerHedgeSingleExecutorConfig, update_interval: float = 0.5, max_retries: int = 10):
        getcontext().prec = 28

        self.config = config
        self.maker_connector = config.maker_market.connector_name
        self.maker_pair = config.maker_market.trading_pair
        self.hedge_connector = config.hedge_market.connector_name
        self.hedge_pair = config.hedge_market.trading_pair
        self.side_maker = TradeType.BUY if str(config.side_maker).upper() == "BUY" else TradeType.SELL
        self.side_hedge = TradeType.SELL if self.side_maker == TradeType.BUY else TradeType.BUY
        self._max_retries = max_retries

        self._leverage_applied: bool = False

        self._maker_orders: List[TrackedOrder] = []
        self._hedge_orders: List[TrackedOrder] = []
        self._maker_by_id: Dict[str, TrackedOrder] = {}
        self._hedge_by_id: Dict[str, TrackedOrder] = {}

        self._hedge_inflight: set[str] = set()
        self._hedge_inflight_amounts: Dict[str, Decimal] = {}
        self._max_parallel_hedges: int = 2

        self._maker_pending_ids: set[str] = set()

        self._cum_fees_quote: Decimal = Decimal("0")
        self._net_pnl_quote: Decimal = Decimal("0")
        self._net_pnl_pct: Decimal = Decimal("0")
        self._cum_funding_maker_quote: Decimal = Decimal("0")
        self._cum_funding_hedge_quote: Decimal = Decimal("0")
        self._cum_funding_quote: Decimal = Decimal("0")

        self._next_order_ready_ts: float = 0.0

        self._hedge_accum_base = Decimal("0")

        self._closing: bool = False
        self._close_queue: List[Dict] = []
        self._closing_current: Optional[Dict] = None

        self._waiting_hedge_ack: bool = False
        self._last_hedge_order_id: Optional[str] = None

        self._opening_fully_completed: bool = False

        self._funding_below_start_ts: Optional[float] = None
        self._funding_last_diff_pct: Optional[Decimal] = None
        self._funding_exit_triggered: bool = False

        self._is_profitable_on_last_check: Optional[bool] = None
        self._last_profitable_ts: float = 0.0

        super().__init__(strategy=strategy, connectors=[self.maker_connector, self.hedge_connector], config=config, update_interval=update_interval)

    async def on_start(self):
        self.logger().info(
            f"Executor start: entry={self.maker_connector}:{self.maker_pair} hedge={self.hedge_connector}:{self.hedge_pair} side={self.side_maker.name}"
        )
        self._desired_hedge_position_mode: PositionMode = (
            PositionMode.ONEWAY if self.hedge_connector == "hyperliquid_perpetual" else PositionMode.HEDGE
        )
        try:
            if self._desired_hedge_position_mode == PositionMode.HEDGE:
                self._strategy.set_position_mode(self.hedge_connector, PositionMode.HEDGE)
                self.logger().info(f"Requested HEDGE position mode on {self.hedge_connector}")
            else:
                self.logger().info(f"Using ONEWAY position mode on {self.hedge_connector}")
        except Exception as e:
            self.logger().warning(f"Could not request {self._desired_hedge_position_mode.name} position mode on {self.hedge_connector}: {e}")
        await super().on_start()

    def add_order(self, order: TrackedOrder, is_maker: bool):
        if is_maker:
            self._maker_orders.append(order)
            self._maker_by_id[order.order_id] = order
        else:
            self._hedge_orders.append(order)
            self._hedge_by_id[order.order_id] = order

    def remove_order(self, order_id: str, is_maker: bool):
        if is_maker:
            self._maker_orders = [order for order in self._maker_orders if order.order_id != order_id]
            self._maker_by_id.pop(order_id, None)
            self._maker_pending_ids.discard(order_id)
        else:
            self._hedge_orders = [order for order in self._hedge_orders if order.order_id != order_id]
            self._hedge_by_id.pop(order_id, None)
            self._hedge_inflight_amounts.pop(order_id, None)

    def replace_order(self, order: TrackedOrder, is_maker: bool):
        if is_maker:
            for idx, existing_order in enumerate(self._maker_orders):
                if existing_order.order_id == order.order_id:
                    self._maker_orders[idx] = order
                    self._maker_by_id[order.order_id] = order
                    return
        else:
            for idx, existing_order in enumerate(self._hedge_orders):
                if existing_order.order_id == order.order_id:
                    self._hedge_orders[idx] = order
                    self._hedge_by_id[order.order_id] = order
                    return

    def cancel_order(self, order: TrackedOrder, is_maker: bool):
        exec_base = order.executed_amount_base or Decimal("0")
        conn = self.maker_connector if is_maker else self.hedge_connector
        pair = getattr(order.order, "trading_pair", None)
        if not pair:
            pair = self.maker_pair if is_maker else self.hedge_pair
            self.logger().warning(f"Cancel: missing trading_pair for {order.order_id}, using configured pair {pair}")
        try:
            self._strategy.cancel(conn, pair, order.order_id)
        except Exception as e:
            self.logger().warning(f"Cancel failed {order.order_id} on {conn}:{pair}: {e}")
        if exec_base <= 0:
            self.remove_order(order.order_id, is_maker=is_maker)

    def _format_tracked_orders(self, orders: List[TrackedOrder]):
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

    def get_last_active_unfilled_order(self, is_maker: bool) -> Optional[TrackedOrder]:
        orders = self._maker_orders if is_maker else self._hedge_orders
        if orders:
            for order in reversed(orders):
                if order.is_filled:
                    continue
                if order.order is not None:
                    return order
                if is_maker and order.order_id in self._maker_pending_ids:
                    return order
        return None

    def get_maker_order_usd_consumed(self) -> Decimal:
        consumed = Decimal("0")
        for order in self._maker_orders:
            if order.order and order.executed_amount_base is not None and order.average_executed_price is not None:
                consumed += order.executed_amount_base * order.average_executed_price
        return consumed

    def get_maker_order_usd_inflight(self) -> Decimal:
        inflight = Decimal("0")
        for order in self._maker_orders:
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
        remaining_cap = Decimal(str(self.config.pair_notional_usd_cap)) - consumed - inflight
        if remaining_cap < 0:
            remaining_cap = Decimal("0")
        return remaining_cap

    def is_enough_maker_cap(self, remaining_cap: Optional[Decimal]) -> bool:
        if remaining_cap is None:
            remaining_cap = self.get_remaining_maker_cap()
        return remaining_cap >= Decimal(str(self.config.per_order_min_notional_usd))

    def get_open_orders(self) -> List[TrackedOrder]:
        opened_orders = []
        for order in self._maker_orders + self._hedge_orders:
            try:
                if order.order and getattr(order.order, "is_open", False) and not order.is_filled:
                    opened_orders.append(order)
            except Exception:
                if order.order and not order.is_filled:
                    opened_orders.append(order)
        return opened_orders

    def cancel_all_orders(self):
        for order in self.get_open_orders():
            self.cancel_order(order, is_maker=(order in self._maker_orders))

    def get_full_position_base_amount(self, is_maker: bool = True) -> Decimal:
        orders = self._maker_orders if is_maker else self._hedge_orders
        position = Decimal("0")
        for order in orders:
            exec_base = order.executed_amount_base or Decimal("0")
            if exec_base > 0:
                position += exec_base
        return position

    def _add_fee_quote(self, trade_fee, price_mid: Decimal, pair: str):
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
                    self._cum_fees_quote += amt * price_mid
                elif token == quote:
                    self._cum_fees_quote += amt
                else:
                    self._cum_fees_quote += amt
        except Exception:
            pass

    async def validate_sufficient_balance(self):
        mid = self.get_price(self.maker_connector, self.maker_pair, PriceType.MidPrice)
        if mid.is_nan() or mid <= 0:
            self.logger().info("Entry mid price unavailable; proceeding but orders may fail.")
            return
        base, quote = self.maker_pair.split("-")

        self.logger().info(f"pair_notional_usd_cap: {self.config.pair_notional_usd_cap}, per_order_max_notional_usd: {self.config.per_order_max_notional_usd}, leverage: {self.config.leverage}")
        remaining_cap = self.get_remaining_maker_cap()
        self.logger().info(f"Validating sufficient {quote} margin for next order; remaining cap ${remaining_cap:.2f}")
        if not self.is_enough_maker_cap(remaining_cap):
            self.logger().warning(f"Not enough remaining maker cap to place new order, remaining cap: ${remaining_cap:.2f}, need at least: ${self.config.per_order_min_notional_usd:.2f}. Completing executor.")
            return

        next_notional_usd = min(remaining_cap, Decimal(str(self.config.per_order_max_notional_usd)))
        leverage = self.config.leverage if getattr(self.config, "leverage", None) is not None else Decimal("1")
        if leverage <= 0:
            leverage = Decimal("1")
        required_margin = Decimal(str(next_notional_usd)) / leverage
        self.logger().info(f"connector {self.maker_connector} requires ~{required_margin:.6f} {quote}")
        avail_quote = self.get_available_balance(self.maker_connector, quote)
        if avail_quote < required_margin:
            self.logger().info(
                f"Insufficient {quote} margin: need {required_margin:.6f} (next_notional {next_notional_usd:.6f}/lev {leverage}), have {avail_quote:.6f}."
            )

    def is_any_position_open(self) -> bool:
        pos_maker = self.get_full_position_base_amount(is_maker=True)
        pos_hedge = self.get_full_position_base_amount(is_maker=False)
        return (pos_maker > 0) or (pos_hedge > 0)

    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            try:
                hedge_market = self._strategy.connectors[self.hedge_connector]
                mode = getattr(hedge_market, "position_mode", None)
                desired_mode = getattr(self, "_desired_hedge_position_mode", PositionMode.HEDGE)
                if desired_mode == PositionMode.HEDGE and mode != PositionMode.HEDGE:
                    try:
                        self._strategy.set_position_mode(self.hedge_connector, PositionMode.HEDGE)
                    except Exception:
                        pass
                    self.logger().warning(f"{self.hedge_connector} is not in HEDGE mode yet. Waiting before placing orders...")
                    return
            except Exception:
                self.logger().warning(f"Unable to verify position mode for {self.hedge_connector}. Waiting...")
                return

            if not self._leverage_applied:
                try:
                    lev = int(self.config.leverage) if getattr(self.config, "leverage", None) is not None else 1
                    if lev <= 0:
                        lev = 1
                    ok1 = ok2 = True
                    try:
                        self._strategy.set_leverage(self.maker_connector, self.maker_pair, lev)
                    except Exception as e:
                        ok1 = False
                        self.logger().warning(f"Failed to set leverage {lev} on {self.maker_connector}:{self.maker_pair}: {e}")
                    try:
                        self._strategy.set_leverage(self.hedge_connector, self.hedge_pair, lev)
                    except Exception as e:
                        ok2 = False
                        self.logger().warning(f"Failed to set leverage {lev} on {self.hedge_connector}:{self.hedge_pair}: {e}")
                    self._leverage_applied = ok1 and ok2
                except Exception as e:
                    self.logger().warning(f"Unexpected error applying leverage: {e}")

            now = self._strategy.current_timestamp

            if self._opening_fully_completed and not self._closing:
                if not self.is_any_position_open():
                    self.early_stop()
                    return
                self._monitor_funding_and_maybe_trigger_exit(now)
                return

            if self._closing and self._closing_current and (self.config.maker_ttl_sec and self.config.maker_ttl_sec > 0):
                close_creation_ts = self._closing_current.get("creation_ts", 0) or 0
                if close_creation_ts > 0 and (now - close_creation_ts) >= float(self.config.maker_ttl_sec):
                    try:
                        close_id = self._closing_current.get("close_order_id")
                        placed = self._closing_current.get("placed_amount", Decimal("0"))
                        executed = self._closing_current.get("executed_base", Decimal("0"))
                        remaining = placed - executed
                        if remaining > 0 and close_id:
                            self.logger().info(f"CLOSE TTL exceeded for {close_id}; cancel and re-quote remaining {remaining}.")
                            pair = self.maker_pair
                            self._strategy.cancel(self.maker_connector, pair, close_id)
                            self._close_queue.append({"open_id": self._closing_current.get("open_id"), "amount": remaining})
                        else:
                            self.logger().info(f"CLOSE TTL exceeded for {close_id}; nothing remaining.")
                    except Exception as e:
                        self.logger().warning(f"Error during CLOSE TTL handling: {e}")
                    finally:
                        self._closing_current = None
                        self._next_order_ready_ts = now + 0.2

            last_unfilled_order = self.get_last_active_unfilled_order(is_maker=True)
            self.logger().debug(f"Last unfilled maker order: {last_unfilled_order.order_id if last_unfilled_order else None}")

            if last_unfilled_order is None and self._closing_current is None:
                if self._maker_pending_ids:
                    return

                if not self._closing:
                    remaining_cap = self.get_remaining_maker_cap()
                    if not self.is_enough_maker_cap(remaining_cap):
                        self.logger().warning(f"Not enough remaining maker cap to place new order, remaining cap: ${remaining_cap:.2f}, need at least: ${self.config.per_order_min_notional_usd:.2f}. Completing executor.")
                        self._opening_fully_completed = True
                        return
                    await self.validate_sufficient_balance()

                if now < self._next_order_ready_ts:
                    return

                await self._place_next_part()
            if last_unfilled_order and not self._opening_fully_completed:
                if not self._closing and self.config.maker_ttl_sec and self.config.maker_ttl_sec > 0:
                    unfilled_maker_creation_time = getattr(last_unfilled_order, "creation_timestamp", 0) or 0
                    if unfilled_maker_creation_time > 0 and (now - unfilled_maker_creation_time) >= float(self.config.maker_ttl_sec):
                        self.logger().info(f"Cancelling stale maker order {last_unfilled_order.order_id} due to TTL exceeded.")
                        self.cancel_order(last_unfilled_order, is_maker=True)

        elif self.status == RunnableStatus.SHUTTING_DOWN:
            pass

    def _get_mid(self) -> Decimal:
        px = self.get_price(self.maker_connector, self.maker_pair, PriceType.MidPrice)
        return Decimal("NaN") if px.is_nan() else px

    def _get_best_bid(self) -> Optional[Decimal]:
        try:
            bid = self.get_price(self.maker_connector, self.maker_pair, PriceType.BestBid)
            return None if bid.is_nan() else bid
        except Exception:
            return None

    def _get_best_ask(self) -> Optional[Decimal]:
        try:
            ask = self.get_price(self.maker_connector, self.maker_pair, PriceType.BestAsk)
            return None if ask.is_nan() else ask
        except Exception:
            return None

    def _get_normalized_funding_rates(self) -> Optional[Dict[str, Union[Decimal, None]]]:
        try:
            mdp = getattr(self._strategy, "market_data_provider", None)
            if mdp is None:
                return None
            entry_fi = mdp.get_funding_info(self.maker_connector, self.maker_pair)
            hedge_fi = mdp.get_funding_info(self.hedge_connector, self.hedge_pair)
            if entry_fi is None or hedge_fi is None:
                return None
            entry_sec = util_normalized_funding_rate_in_seconds(entry_fi, self.maker_connector)
            hedge_sec = util_normalized_funding_rate_in_seconds(hedge_fi, self.hedge_connector)
            return {
                "entry": entry_sec,
                "hedge": hedge_sec,
            }
        except Exception:
            return None

    def _get_funding_diff_pct(self, funding_interval_hours: Optional[int] = None) -> Optional[Decimal]:
        try:
            rates = self._get_normalized_funding_rates()
            if rates is None:
                self.logger().info("Funding rates unavailable")
                return None
            entry_sec = rates.get("entry")
            hedge_sec = rates.get("hedge")
            if entry_sec is None or hedge_sec is None:
                self.logger().info("Funding rates unavailable")
                return None
            funding_profitability_interval_hours = getattr(self.config, "funding_profitability_interval_hours", 24) if funding_interval_hours is None else funding_interval_hours
            diff_pct = util_funding_diff_pct(entry_sec, hedge_sec, hours=funding_profitability_interval_hours)
            if diff_pct is None:
                return None
            return Decimal(str(diff_pct))
        except Exception:
            return None

    def _get_oriented_funding_diff_pct(self, funding_interval_hours: Optional[int] = None) -> Optional[Decimal]:
        try:
            diff_pct_raw = self._get_funding_diff_pct(funding_interval_hours)
            if diff_pct_raw is None:
                self.logger().info("Funding rates unavailable")
                return None
            try:
                side_factor = Decimal("1") if self.side_maker == TradeType.BUY else Decimal("-1")
            except Exception:
                side_factor = Decimal("1")
            try:
                oriented_diff_pct = Decimal(str(diff_pct_raw)) * side_factor
            except Exception:
                oriented_diff_pct = diff_pct_raw
            return oriented_diff_pct
        except Exception:
            return None

    def _monitor_funding_and_maybe_trigger_exit(self, now_ts: float):
        try:
            oriented_diff_pct = self._get_oriented_funding_diff_pct()
            if oriented_diff_pct is None:
                self.logger().info("Funding rates unavailable")
                return None
            self._funding_last_diff_pct = oriented_diff_pct
            self.logger().info(
                f"[FundingMonitor] {self.maker_pair} oriented_diff_pct(for position maker={'LONG' if self.side_maker == TradeType.BUY else 'SHORT'}): {oriented_diff_pct}"
            )

            pct_threshold = getattr(self.config, "exit_funding_diff_pct_threshold", None)
            hold_sec = getattr(self.config, "exit_hold_below_sec", None)

            if oriented_diff_pct <= pct_threshold:
                if self._funding_below_start_ts is None:
                    self._funding_below_start_ts = now_ts
                    self.logger().info(
                        f"[FundingMonitor] diff_pct={oriented_diff_pct:.6f}% <= {pct_threshold}% ; starting hold timer {hold_sec}s"
                    )
                else:
                    elapsed = now_ts - self._funding_below_start_ts
                    if elapsed >= float(hold_sec) and not self._funding_exit_triggered:
                        self._funding_exit_triggered = True
                        self._start_closing_due_to_funding(oriented_diff_pct, pct_threshold, hold_sec)
            else:
                if self._funding_below_start_ts is not None:
                    self.logger().info(
                        f"[FundingMonitor] diff_pct back above threshold: {oriented_diff_pct:.6f}% > {pct_threshold}%; resetting timer"
                    )
                self._funding_below_start_ts = None
        except Exception as e:
            self.logger().warning(f"[FundingMonitor] error: {e}")

    def _start_closing_due_to_funding(self, diff_pct: Decimal, threshold: Decimal, hold_sec: int):
        self.logger().info(
            f"[FundingExit] Sustained low funding diff_pct={diff_pct:.6f}% <= {threshold}% for {hold_sec}s; starting CLOSE sequence"
        )
        self._closing = True
        self._build_close_queue_if_needed()
        self._funding_below_start_ts = None

    def _check_profitability_enter_condition(self, maker_side: TradeType, maker_price: Decimal, amount: Decimal) -> bool:
        if self._closing:
            return True

        mdp = getattr(self._strategy, "market_data_provider", None)

        hedge_side = self._get_hedge_side_for_mode()
        hedge_price = Decimal(mdp.get_price_for_quote_volume(
            connector_name=self.hedge_connector,
            trading_pair=self.hedge_pair,
            quote_volume=amount * maker_price,
            is_buy=hedge_side == TradeType.BUY,
        ).result_price)
        self.logger().info(f"Entry prices: maker {maker_price:.8f} exchange {self.maker_connector} side {maker_side} hedge {hedge_price:.8f} exchange {self.hedge_connector} side {hedge_side} for amount {amount:.8f}")

        maker_estimated_fees = self.connectors[self.maker_connector].get_fee(
            base_currency=self.maker_pair.split("-")[0],
            quote_currency=self.maker_pair.split("-")[1],
            order_type=OrderType.LIMIT_MAKER,
            order_side=maker_side,
            amount=amount,
            price=maker_price,
            is_maker=True,
            position_action=PositionAction.OPEN
        ).percent * 2
        hedge_estimated_fees = self.connectors[self.hedge_connector].get_fee(
            base_currency=self.hedge_pair.split("-")[0],
            quote_currency=self.hedge_pair.split("-")[1],
            order_type=OrderType.MARKET,
            order_side=hedge_side,
            amount=amount,
            price=hedge_price,
            is_maker=False,
            position_action=PositionAction.OPEN
        ).percent * 2
        self.logger().info(f"Estimated fees: maker {maker_estimated_fees:.6f} hedge {hedge_estimated_fees:.6f}")

        if maker_side == TradeType.BUY:
            estimated_trade_pnl_pct = (hedge_price - maker_price) / maker_price
        else:
            estimated_trade_pnl_pct = (maker_price - hedge_price) / maker_price

        self.logger().info(f"Estimated trade PnL%: {estimated_trade_pnl_pct:.6f}")

        total_estimated_fees = maker_estimated_fees + hedge_estimated_fees
        net_estimated_pnl_pct = estimated_trade_pnl_pct - total_estimated_fees

        if net_estimated_pnl_pct > Decimal("0"):
            self.logger().info(f"Net estimated PnL% after fees: {net_estimated_pnl_pct:.6f} (fees total {total_estimated_fees:.6f}) - PROFITABLE")
            return True

        oriented_funding_diff_pct = self._get_oriented_funding_diff_pct(funding_interval_hours=1)
        if oriented_funding_diff_pct is None:
            self.logger().info("Funding rates unavailable - skipping profitability-based entry condition")
            return False

        if (oriented_funding_diff_pct + net_estimated_pnl_pct) > Decimal("0"):
            self.logger().info(f"Net estimated PnL% after fees: {net_estimated_pnl_pct:.6f} + funding {oriented_funding_diff_pct:.6f} = {(net_estimated_pnl_pct + oriented_funding_diff_pct):.6f} - PROFITABLE with funding")
            return True
        else:
            self.logger().info(f"Net estimated PnL% after fees: {net_estimated_pnl_pct:.6f} + funding {oriented_funding_diff_pct:.6f} = {(net_estimated_pnl_pct + oriented_funding_diff_pct):.6f} - NOT PROFITABLE")
            return False

    def _compute_limit_price(self, side: TradeType, mid: Optional[Decimal] = None) -> Optional[Decimal]:
        if mid is None:
            mid = self._get_mid()
        if mid is None or mid.is_nan() or mid <= 0:
            return None
        off = (self.config.maker_price_offset_bp / Decimal("10000"))
        if side == TradeType.BUY:
            return mid * (Decimal("1") - off)
        else:
            return mid * (Decimal("1") + off)

    async def _place_next_part(self):
        mid = self._get_mid()
        if mid.is_nan() or mid <= 0:
            return

        maker_side: TradeType = self._get_maker_side_for_mode()

        if self._closing:
            self._build_close_queue_if_needed()
            if self._closing_current is not None:
                return
            if not self._close_queue:
                self._finalize_hedge_tail()
                self.logger().info("Close queue is empty; finishing executor.")
                self.close_type = CloseType.COMPLETED
                self.stop()
                return
            item = self._close_queue.pop(0)
            open_id = item["open_id"]
            raw_amount = item["amount"]
            mode_desc = "CLOSE"
        else:
            remaining_cap = self.get_remaining_maker_cap()
            if remaining_cap <= 0:
                self._finalize_hedge_tail()
                self.logger().info("Pair notional USD cap fully consumed; completing executor.")
                self.close_type = CloseType.COMPLETED
                self.stop()
                return
            px_preview = self._compute_limit_price(maker_side, mid)
            if px_preview is None or px_preview <= 0:
                return
            next_notional_usd = min(remaining_cap, Decimal(str(self.config.per_order_max_notional_usd)))
            self.logger().info(f"Next order notional USD: ${next_notional_usd:.2f} at limit px {px_preview:.8f}")
            raw_amount = Decimal(str(next_notional_usd)) / px_preview
            maker_min_notional = getattr(self.config, "maker_min_notional_usd", Decimal("0"))
            if maker_min_notional and next_notional_usd < maker_min_notional:
                self.logger().info(f"Skip maker OPEN: below min notional ${maker_min_notional}")
                return
            mode_desc = "OPEN"

        try:
            entry_conn = self._strategy.connectors[self.maker_connector]
            amt_q_maker = entry_conn.quantize_order_amount(self.maker_pair, raw_amount)
            hedge_conn = self._strategy.connectors[self.hedge_connector]
            amt_q = hedge_conn.quantize_order_amount(self.hedge_pair, amt_q_maker)
            self.logger().debug(f"Qty raw={raw_amount} -> maker_q={amt_q_maker} -> hedge_q={amt_q}")
        except Exception:
            amt_q = raw_amount
        amount = amt_q

        px = self._compute_limit_price(maker_side, mid)
        if px is None or px <= 0:
            return
        if not self._closing:
            planned_notional = amount * px
            rem_cap_now = self.get_remaining_maker_cap()
            if planned_notional > rem_cap_now:
                try:
                    entry_conn = self._strategy.connectors[self.maker_connector]
                    adjusted = entry_conn.quantize_order_amount(self.maker_pair, amount * Decimal("0.999"))
                    if adjusted > 0:
                        amount = adjusted
                except Exception:
                    pass
        if amount <= 0:
            self.logger().info("Computed order amount <= 0; skipping")
            self._opening_fully_completed = True
            return

        is_profitability_check_passed = self._check_profitability_enter_condition(maker_side, px, amount)
        self.logger().info(f"Placing maker {mode_desc} qty={amount} @ {px:.8f} (profitability check: {'pass' if is_profitability_check_passed else 'fail'})")

        now_ts = int(self._strategy.current_timestamp)
        wait_sec = float(getattr(self.config, "non_profitable_wait_sec", 60.0))

        if not hasattr(self, "_is_profitable_on_last_check"):
            self._is_profitable_on_last_check = True
        if not hasattr(self, "_last_profitable_ts") or not self._last_profitable_ts:
            self._last_profitable_ts = now_ts

        if not is_profitability_check_passed:
            elapsed = now_ts - self._last_profitable_ts

            if not self._is_profitable_on_last_check:
                if elapsed >= wait_sec:
                    if not self.is_any_position_open():
                        self.logger().info(
                            f"Pair {self.maker_pair} non-profitable wait time exceeded ({elapsed:.0f}s >= {wait_sec:.0f}s); no open orders, stopping executor."
                        )
                        self.early_stop()
                        return
                    self._opening_fully_completed = True
                    self.logger().info(
                        f"Pair {self.maker_pair} non-profitable wait time exceeded ({elapsed:.0f}s >= {wait_sec:.0f}s); entering funding monitoring."
                    )
                else:
                    self.logger().info(
                        f"Pair {self.maker_pair} still not profitable; waiting {wait_sec - elapsed:.0f}s more."
                    )
                return

            self._is_profitable_on_last_check = False
            self.logger().info(
                f"Pair {self.maker_pair} not profitable; starting wait window {wait_sec:.0f}s."
            )
            return

        self._is_profitable_on_last_check = True
        self._last_profitable_ts = now_ts

        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_pair,
            order_type=OrderType.LIMIT_MAKER,
            side=maker_side,
            amount=amount,
            position_action=(PositionAction.CLOSE if self._closing else PositionAction.OPEN),
            price=px,
        )
        new_tracked = TrackedOrder(order_id=order_id)

        if self._closing:
            self._closing_current = {
                "open_id": open_id,
                "close_order_id": order_id,
                "placed_amount": amount,
                "executed_base": Decimal("0"),
                "creation_ts": self._strategy.current_timestamp,
            }
            self.logger().info(f"Placed maker CLOSE (LIMIT marketable) for open_id={open_id} -> close_id={order_id} px={px} amt={amount}")
        else:
            self.add_order(new_tracked, is_maker=True)
            self._maker_pending_ids.add(order_id)
            cooldown = float(getattr(self.config, "post_place_cooldown_sec", 0.5))
            self._next_order_ready_ts = self._strategy.current_timestamp + cooldown
            self.logger().info(f"Placed maker {mode_desc} order id={order_id} type=LIMIT px={px} amt={amount}")

    def _hedge_market(self, qty_base: Decimal):
        if qty_base <= 0:
            return
        if len(self._hedge_inflight) >= self._max_parallel_hedges:
            return
        order_id = self.place_order(
            connector_name=self.hedge_connector,
            trading_pair=self.hedge_pair,
            order_type=OrderType.MARKET,
            side=(self._opposite_side(self.side_hedge) if self._closing else self.side_hedge),
            amount=qty_base,
            position_action=(PositionAction.CLOSE if self._closing else PositionAction.OPEN),
        )
        self.add_order(TrackedOrder(order_id=order_id), is_maker=False)
        self._hedge_inflight.add(order_id)
        self._hedge_inflight_amounts[order_id] = qty_base
        self._last_hedge_order_id = order_id  # legacy
        self.logger().info(f"Hedge market sent id={order_id} qty={qty_base} mode={'CLOSE' if self._closing else 'OPEN'} inflight={len(self._hedge_inflight)}")

    def _try_hedge_accumulated(self):
        if self._hedge_accum_base <= 0:
            return
        if len(self._hedge_inflight) >= self._max_parallel_hedges:
            return
        try:
            hedge_connector = self._strategy.connectors[self.hedge_connector]
            q_amt = hedge_connector.quantize_order_amount(self.hedge_pair, self._hedge_accum_base)
        except Exception:
            q_amt = self._hedge_accum_base
        if q_amt is None or q_amt <= 0:
            return
        mid = self.get_price(self.hedge_connector, self.hedge_pair, PriceType.MidPrice)
        if mid.is_nan() or mid <= 0:
            return
        notional = q_amt * mid
        min_notional = getattr(self.config, "hedge_min_notional_usd", Decimal("0"))
        if notional < min_notional:
            return
        self._hedge_market(q_amt)
        self._hedge_accum_base -= q_amt

    def _finalize_hedge_tail(self):
        if self._hedge_accum_base > 0:
            qty = self._hedge_accum_base
            self._hedge_accum_base = Decimal("0")
            self.logger().info(f"Force-flush hedge tail qty={qty}")
            self._hedge_market(qty)

    def _maker_remaining_exposure_base(self) -> Decimal:
        open_base = Decimal("0")
        close_base = Decimal("0")
        for o in self._maker_orders:
            if o.order is None:
                continue
            try:
                if o.order.position == PositionAction.OPEN:
                    open_base += (o.executed_amount_base or Decimal("0"))
                elif o.order.position == PositionAction.CLOSE:
                    close_base += (o.executed_amount_base or Decimal("0"))
            except Exception:
                pass
        remaining = open_base - close_base
        if remaining < 0:
            remaining = Decimal("0")
        return remaining

    def _hedge_remaining_exposure_base(self) -> Decimal:
        open_base = Decimal("0")
        close_base = Decimal("0")
        for o in self._hedge_orders:
            if o.order is None:
                continue
            try:
                if o.order.position == PositionAction.OPEN:
                    open_base += (o.executed_amount_base or Decimal("0"))
                elif o.order.position == PositionAction.CLOSE:
                    close_base += (o.executed_amount_base or Decimal("0"))
            except Exception:
                pass
        remaining = open_base - close_base
        if remaining < 0:
            remaining = Decimal("0")
        return remaining

    def _build_close_queue_if_needed(self):
        if self._close_queue:
            return
        queue: List[Dict] = []
        for o in self._maker_orders:
            if o.order is None:
                continue
            try:
                if o.order.position == PositionAction.OPEN and (o.executed_amount_base or Decimal("0")) > 0:
                    queue.append({"open_id": o.order_id, "amount": o.executed_amount_base})
            except Exception:
                continue
        self._close_queue = queue
        try:
            self.logger().info(f"Built close queue (FIFO): {json.dumps([{'open_id': it['open_id'], 'amount': str(it['amount'])} for it in self._close_queue])}")
        except Exception:
            self.logger().info(f"Built close queue (FIFO): {self._close_queue}")

    def _opposite_side(self, side: TradeType) -> TradeType:
        return TradeType.SELL if side == TradeType.BUY else TradeType.BUY

    def _get_maker_side_for_mode(self) -> TradeType:
        if self._closing:
            return self._opposite_side(self.side_maker)
        else:
            return self.side_maker

    def _get_hedge_side_for_mode(self) -> TradeType:
        if self._closing:
            return self._opposite_side(self.side_hedge)
        else:
            return self.side_hedge

    def _market_name(self, market: ConnectorBase) -> str:
        return getattr(market, "name", None) or getattr(market, "connector_name", None) or ""

    def process_order_created_event(self, event_tag: int, market: ConnectorBase, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        if self._closing_current and event.order_id == self._closing_current.get("close_order_id"):
            updated = None
            try:
                updated = self.get_in_flight_order(self._market_name(market), event.order_id)
            except Exception:
                pass
            if updated:
                to = TrackedOrder(order_id=event.order_id)
                to.order = updated
                self._maker_by_id[event.order_id] = to
                self._closing_current["creation_ts"] = getattr(updated, "creation_timestamp", self._strategy.current_timestamp)
            self.logger().info(f"Maker CLOSE acknowledged id={event.order_id}")
            return

        to = self._maker_by_id.get(event.order_id) or self._hedge_by_id.get(event.order_id)
        if to:
            updated = None
            try:
                updated = self.get_in_flight_order(self._market_name(market), event.order_id)
            except Exception:
                pass
            if updated:
                to.order = updated
                self.replace_order(to, is_maker=(to in self._maker_orders))
        self._maker_pending_ids.discard(event.order_id)

        if event.order_id in self._hedge_inflight:
            self._try_hedge_accumulated()

    def process_order_filled_event(self, event_tag: int, market: ConnectorBase, event: OrderFilledEvent):
        mid_maker = self.get_price(self.maker_connector, self.maker_pair, PriceType.MidPrice)
        if self._closing_current and event.order_id == self._closing_current.get("close_order_id"):
            filled_base = Decimal(str(event.amount))
            self._closing_current["executed_base"] = self._closing_current.get("executed_base", Decimal("0")) + filled_base
            self._add_fee_quote(event.trade_fee, mid_maker, self.maker_pair)
            self._hedge_accum_base += filled_base
            self._try_hedge_accumulated()
            return

        mo = self._maker_by_id.get(event.order_id)
        if mo:
            try:
                updated = self.get_in_flight_order(self._market_name(market), event.order_id)
                if updated:
                    mo.order = updated
                    self.replace_order(mo, is_maker=True)
            except Exception:
                pass
            filled_base = Decimal(str(event.amount))
            self._add_fee_quote(event.trade_fee, mid_maker, self.maker_pair)
            self._hedge_accum_base += filled_base
            self._try_hedge_accumulated()
            return

        if event.order_id in self._hedge_by_id:
            mid_hedge = self.get_price(self.hedge_connector, self.hedge_pair, PriceType.MidPrice)
            self._add_fee_quote(event.trade_fee, mid_hedge, self.hedge_pair)

    def process_order_completed_event(self, event_tag: int, market: ConnectorBase, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        if event.order_id in self._hedge_inflight:
            self._hedge_inflight.discard(event.order_id)
            self._hedge_inflight_amounts.pop(event.order_id, None)
            self._try_hedge_accumulated()

        self._maker_pending_ids.discard(event.order_id)

        if self._closing_current and event.order_id == self._closing_current.get("close_order_id"):
            open_id = self._closing_current.get("open_id")
            before = len(self._maker_orders)
            self._maker_orders = [o for o in self._maker_orders if o.order_id != open_id]
            self._maker_by_id.pop(open_id, None)
            after = len(self._maker_orders)
            self.logger().info(f"Closed open_id={open_id}; removed from maker list. Remaining tracked: {after} (was {before})")
            self._closing_current = None
            if not self._close_queue and not any((o.order and o.order.position == PositionAction.OPEN and (o.executed_amount_base or 0) > 0) for o in self._maker_orders):
                self._finalize_hedge_tail()
                self.logger().info("Close queue empty and no remaining maker opens; finishing executor.")
                self._maker_orders.clear()
                self._maker_by_id.clear()
                self.close_type = CloseType.COMPLETED
                self.stop()
            else:
                self._next_order_ready_ts = self._strategy.current_timestamp + float(self.config.order_interval_sec)
            return

        if event.order_id in self._maker_by_id:
            remaining_cap = self.get_remaining_maker_cap()
            if remaining_cap <= 0:
                self._finalize_hedge_tail()
                self.close_type = CloseType.COMPLETED
                self.stop()
            else:
                self._next_order_ready_ts = self._strategy.current_timestamp + float(self.config.order_interval_sec)

    def process_order_canceled_event(self, event_tag: int, market: ConnectorBase, event: OrderCancelledEvent):
        if self._closing and self._closing_current and event.order_id == self._closing_current.get("close_order_id"):
            placed = self._closing_current.get("placed_amount", Decimal("0"))
            executed = self._closing_current.get("executed_base", Decimal("0"))
            remaining = placed - executed
            if remaining > 0:
                self._close_queue.append({"open_id": self._closing_current.get("open_id"), "amount": remaining})
                self.logger().info(f"Close order {event.order_id} canceled; re-queue remaining {remaining} for open_id={self._closing_current.get('open_id')}")
            else:
                self.logger().info(f"Close order {event.order_id} canceled; nothing remaining to re-queue.")
            self._closing_current = None
            self._maker_pending_ids.discard(event.order_id)
            return

        if event.order_id in self._maker_by_id:
            to = self._maker_by_id.get(event.order_id)
            try:
                updated = self.get_in_flight_order(self._market_name(market), event.order_id)
                if updated:
                    to.order = updated
            except Exception:
                pass
            exec_base = to.executed_amount_base or Decimal("0")
            if exec_base <= 0:
                self.remove_order(event.order_id, is_maker=True)
            else:
                self.logger().info(f"Maker order {event.order_id} canceled with partial fill {exec_base}; keeping for exposure accounting.")
        elif event.order_id in self._hedge_by_id:
            to = self._hedge_by_id.get(event.order_id)
            try:
                updated = self.get_in_flight_order(self._market_name(market), event.order_id)
                if updated:
                    to.order = updated
            except Exception:
                pass
            exec_base = to.executed_amount_base or Decimal("0")
            if exec_base <= 0:
                self.remove_order(event.order_id, is_maker=False)
            else:
                self.logger().info(f"Hedge order {event.order_id} canceled with partial fill {exec_base}; keeping for exposure accounting.")

        self._hedge_inflight.discard(event.order_id)
        self._hedge_inflight_amounts.pop(event.order_id, None)
        self._maker_pending_ids.discard(event.order_id)

    def process_order_failed_event(self, event_tag: int, market: ConnectorBase, event: MarketOrderFailureEvent):
        order_id = event.order_id
        err_msg = event.error_message or "unknown error"

        self._maker_pending_ids.discard(order_id)
        attempted_hedge = self._hedge_inflight_amounts.pop(order_id, Decimal("0"))
        self._hedge_inflight.discard(order_id)

        handled = False

        if self._closing_current and order_id == self._closing_current.get("close_order_id"):
            handled = True
            placed = self._closing_current.get("placed_amount", Decimal("0"))
            executed = self._closing_current.get("executed_base", Decimal("0"))
            remaining = placed - executed
            if remaining > 0:
                self._close_queue.insert(0, {"open_id": self._closing_current.get("open_id"), "amount": remaining})
                self.logger().warning(
                    f"Maker CLOSE order {order_id} failed ({err_msg}); re-queued remaining {remaining}."
                )
            else:
                self.logger().warning(f"Maker CLOSE order {order_id} failed ({err_msg}); nothing remaining to re-queue.")
            self._closing_current = None
            cooldown = float(getattr(self.config, "post_place_cooldown_sec", 0.5))
            self._next_order_ready_ts = self._strategy.current_timestamp + cooldown

        maker_tracked = self._maker_by_id.get(order_id)
        if maker_tracked is not None:
            handled = True
            self.logger().warning(f"Maker order {order_id} failed ({err_msg}); scheduling retry.")
            self.remove_order(order_id, is_maker=True)
            cooldown = float(getattr(self.config, "post_place_cooldown_sec", 0.5))
            self._next_order_ready_ts = self._strategy.current_timestamp + cooldown

        hedge_tracked = self._hedge_by_id.get(order_id)
        if hedge_tracked is not None:
            handled = True
            hedge_amount = attempted_hedge
            if hedge_amount <= 0 and hedge_tracked.order is not None:
                try:
                    hedge_amount = Decimal(str(hedge_tracked.order.amount))
                except Exception:
                    hedge_amount = Decimal("0")
            self.remove_order(order_id, is_maker=False)
            if hedge_amount > 0:
                self._hedge_accum_base += hedge_amount
            self.logger().warning(f"Hedge order {order_id} failed ({err_msg}); will attempt again.")
            self._try_hedge_accumulated()
        elif attempted_hedge > 0:
            handled = True
            self._hedge_accum_base += attempted_hedge
            self.logger().warning(f"Recovered hedge accumulator {attempted_hedge} for failed order {order_id}; retrying.")
            self._try_hedge_accumulated()

        if not handled:
            self.logger().warning(f"Order {order_id} failed ({err_msg}); no tracked state was updated.")

    def get_cum_fees_quote(self) -> Decimal:
        return self._cum_fees_quote

    def get_net_pnl_quote(self) -> Decimal:
        return self._net_pnl_quote

    def get_net_pnl_pct(self) -> Decimal:
        return self._net_pnl_pct

    def get_custom_info(self) -> Dict:
        maker_pos_base: Decimal = Decimal("0")
        hedge_pos_base: Decimal = Decimal("0")
        oriented_diff_pct: Optional[Decimal] = None
        abs_diff_pct: Optional[Decimal] = None
        entry_rate_sec: Optional[Decimal] = None
        hedge_rate_sec: Optional[Decimal] = None
        minutes_to_funding_entry: Optional[Decimal] = None
        minutes_to_funding_hedge: Optional[Decimal] = None

        try:
            maker_pos_base = self._maker_remaining_exposure_base()
        except Exception:
            pass
        try:
            hedge_pos_base = self._hedge_remaining_exposure_base()
        except Exception:
            pass

        try:
            rates = self._get_normalized_funding_rates() or {}
            entry_rate_sec = rates.get("entry")
            hedge_rate_sec = rates.get("hedge")
            hours = getattr(self.config, "funding_profitability_interval_hours", None)
            abs_diff_pct = self._get_funding_diff_pct(funding_interval_hours=hours)
            oriented_diff_pct = self._get_oriented_funding_diff_pct(funding_interval_hours=hours)
        except Exception:
            pass

        try:
            mdp = getattr(self._strategy, "market_data_provider", None)
            if mdp is not None:
                entry_fi = mdp.get_funding_info(self.maker_connector, self.maker_pair)
                hedge_fi = mdp.get_funding_info(self.hedge_connector, self.hedge_pair)
                now_ts = self._strategy.current_timestamp
                if entry_fi is not None:
                    minutes_to_funding_entry = util_minutes_to_next_funding(entry_fi.next_funding_utc_timestamp, now_ts)
                if hedge_fi is not None:
                    minutes_to_funding_hedge = util_minutes_to_next_funding(hedge_fi.next_funding_utc_timestamp, now_ts)
        except Exception:
            pass

        try:
            maker_open_orders = self._format_tracked_orders(self._maker_orders)
        except Exception:
            maker_open_orders = []
        try:
            hedge_open_orders = self._format_tracked_orders(self._hedge_orders)
        except Exception:
            hedge_open_orders = []

        return {
            "maker_connector": self.maker_connector,
            "maker_pair": self.maker_pair,
            "hedge_connector": self.hedge_connector,
            "hedge_pair": self.hedge_pair,
            "side": self.side_maker.name,
            "maker_position_base": maker_pos_base,
            "maker_position_quote": (maker_pos_base * self.get_price(self.maker_connector, self.maker_pair, PriceType.MidPrice)),
            "hedge_position_base": hedge_pos_base,
            "net_pnl_quote": self.get_net_pnl_quote(),
            "net_pnl_pct": self.get_net_pnl_pct(),
            "funding_pnl_quote_maker": self._cum_funding_maker_quote,
            "funding_pnl_quote_hedge": self._cum_funding_hedge_quote,
            "funding_pnl_quote_net": (self._cum_funding_maker_quote + self._cum_funding_hedge_quote),
            "funding_pnl_quote": (self._cum_funding_maker_quote + self._cum_funding_hedge_quote),
            "funding_entry_rate_sec": entry_rate_sec,
            "funding_hedge_rate_sec": hedge_rate_sec,
            "funding_diff_pct": abs_diff_pct,
            "funding_oriented_diff_pct": oriented_diff_pct,
            "minutes_to_funding_entry": minutes_to_funding_entry,
            "minutes_to_funding_hedge": minutes_to_funding_hedge,
            "maker_open_orders": maker_open_orders,
            "hedge_open_orders": hedge_open_orders,
            "held_position_orders": [],
        }

    def process_funding_payment_event(self, event_tag: int, market: ConnectorBase, event):
        try:
            self.logger().info(f"Funding event received: {event}")
            market_name = self._market_name(market)
            event_pair = getattr(event, "trading_pair", None)
            if market_name == self.maker_connector and event_pair == self.maker_pair:
                leg = "maker"
            elif market_name == self.hedge_connector and event_pair == self.hedge_pair:
                leg = "hedge"
            else:
                return
            amount: Decimal = getattr(event, "amount", Decimal("0")) or Decimal("0")
            if leg == "maker":
                self._cum_funding_maker_quote += Decimal(str(amount))
            else:
                self._cum_funding_hedge_quote += Decimal(str(amount))
            self._cum_funding_quote = self._cum_funding_maker_quote + self._cum_funding_hedge_quote
            self.logger().info(
                f"[Funding] {leg} {market_name}:{event.trading_pair} amount={amount} cum_maker={self._cum_funding_maker_quote} cum_hedge={self._cum_funding_hedge_quote} net={self._cum_funding_quote}"
            )
        except Exception as e:
            self.logger().warning(f"Error processing funding event: {e}")

    def close_all_positions_by_market(self):
        maker_exposure = self._maker_remaining_exposure_base()
        hedge_exposure = self._hedge_remaining_exposure_base()
        self.logger().info(f"Closing maker position via market: {maker_exposure} {self.maker_pair}")
        self.logger().info(f"Closing hedge position via market: {hedge_exposure} {self.hedge_pair}")

        if maker_exposure > 0:
            self.place_order(
                connector_name=self.maker_connector,
                trading_pair=self.maker_pair,
                order_type=OrderType.MARKET,
                side=self._opposite_side(self.side_maker),
                amount=maker_exposure,
                position_action=PositionAction.CLOSE,
            )

        if hedge_exposure > 0:
            self.place_order(
                connector_name=self.hedge_connector,
                trading_pair=self.hedge_pair,
                order_type=OrderType.MARKET,
                side=self._opposite_side(self.side_hedge),
                amount=hedge_exposure,
                position_action=PositionAction.CLOSE,
            )

        self.stop()

    def early_stop(self, keep_position: bool = False):
        self.logger().info(f"Executor early stop: keep_position={keep_position}")
        self.cancel_all_orders()

        if keep_position:
            self.close_type = CloseType.POSITION_HOLD
            self.stop()
            return

        self.close_all_positions_by_market()
        self.close_type = CloseType.EARLY_STOP
        self.logger().info(
            f"Starting FIFO close with {len(self._close_queue)} items; maker orders to close: {[o['open_id'] for o in self._close_queue]}"
        )

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal, getcontext
from typing import Any, Dict, List, Optional, Union

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
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.remote_iface.mqtt import ETopicPublisher
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.maker_hedge_single_executor.data_types import MakerHedgeSingleExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder
from hummingbot.strategy_v2.utils.funding import minutes_to_next_funding as util_minutes_to_next_funding

from .components.closing import ClosingHelper
from .components.events import EventsHelper
from .components.exposure import ExposureHelper
from .components.funding import FundingHelper
from .components.hedge import HedgeHelper
from .components.liquidation import LiquidationHelper
from .components.logging_prefix import InstancePrefixLogger
from .components.orders import OrdersHelper
from .components.profitability import ProfitabilityHelper
from .components.risk import RiskHelper


class MakerHedgeSingleExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        strategy,
        config: MakerHedgeSingleExecutorConfig,
        update_interval: float = 0.5,
        max_retries: int = 10,
    ):
        getcontext().prec = 28

        self.config = config
        self.maker_connector = config.maker_market.connector_name
        self.maker_pair = config.maker_market.trading_pair
        self.hedge_connector = config.hedge_market.connector_name
        self.hedge_pair = config.hedge_market.trading_pair
        self.side_maker = (
            TradeType.BUY if str(config.side_maker).upper() == "BUY" else TradeType.SELL
        )
        self.side_hedge = (
            TradeType.SELL if self.side_maker == TradeType.BUY else TradeType.BUY
        )
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
        self._maker_cancel_pending: set[str] = set()

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
        self._closing_no_wait: bool = False

        self._closing_completed_event_published: bool = False

        self._waiting_hedge_ack: bool = False
        self._last_hedge_order_id: Optional[str] = None

        self._opening_fully_completed: bool = False

        self._start_ts: Optional[float] = None
        self._last_fill_ts: Optional[float] = None

        self._funding_helper: Optional[FundingHelper] = None
        self._profitability_helper: Optional[ProfitabilityHelper] = None
        self._hedge_helper: Optional[HedgeHelper] = None
        self._events_helper: Optional[EventsHelper] = None
        self._exposure_helper: Optional[ExposureHelper] = None
        self._orders_helper: Optional[OrdersHelper] = None
        self._closing_helper: Optional[ClosingHelper] = None
        self._risk_helper: Optional[RiskHelper] = None
        self._liquidation_helper: Optional[LiquidationHelper] = None

        self._last_liquidation_price_maker: Optional[Decimal] = None
        self._last_liquidation_price_hedge: Optional[Decimal] = None
        self._last_diff_pct_to_liquidation_maker: Optional[Decimal] = None
        self._last_diff_pct_to_liquidation_hedge: Optional[Decimal] = None
        self._maker_unrealized_pnl: Optional[Decimal] = None
        self._hedge_unrealized_pnl: Optional[Decimal] = None

        self.maker_price_offset_pct = config.maker_price_offset_pct
        self.pair_notional_usd_cap = config.pair_notional_usd_cap
        self.per_order_max_notional_usd = config.per_order_max_notional_usd
        self.per_order_min_notional_usd = config.per_order_min_notional_usd
        self.leverage = config.leverage
        self.non_profitable_wait_sec = getattr(config, "non_profitable_wait_sec", 60.0)
        self.closing_non_profitable_wait_sec = getattr(
            config, "closing_non_profitable_wait_sec", 3600.0
        )
        self.post_place_cooldown_sec = getattr(config, "post_place_cooldown_sec", 0.5)
        self.maker_min_notional_usd = getattr(
            config, "maker_min_notional_usd", Decimal("0")
        )
        self.maker_ttl_sec = config.maker_ttl_sec
        self.funding_profitability_interval_hours = getattr(
            config, "funding_profitability_interval_hours", 24
        )
        self.exit_funding_diff_pct_threshold = getattr(
            config, "exit_funding_diff_pct_threshold", None
        )
        self.exit_hold_below_sec = getattr(config, "exit_hold_below_sec", None)
        self.hedge_min_notional_usd = getattr(
            config, "hedge_min_notional_usd", Decimal("0")
        )
        self.liquidation_limit_close_pct = getattr(
            config, "liquidation_limit_close_pct", Decimal("5")
        )
        self.liquidation_market_close_pct = getattr(
            config, "liquidation_market_close_pct", Decimal("1")
        )
        self.fill_timeout_sec = float(getattr(config, "fill_timeout_sec", 0))

        self._closing_wait_started_ts: Optional[float] = None
        self._executor_events_pub: Optional[ETopicPublisher] = None

        super().__init__(
            strategy=strategy,
            connectors=[self.maker_connector, self.hedge_connector],
            config=config,
            update_interval=update_interval,
        )

        base_coin = (
            self.maker_pair.split("-")[0]
            if isinstance(self.maker_pair, str) and "-" in self.maker_pair
            else str(self.maker_pair)
        )
        self._prefixed_logger = InstancePrefixLogger.build_prefixed(base_coin)
        self.logger = lambda: self._prefixed_logger

        self._funding_helper = FundingHelper(self)
        self._profitability_helper = ProfitabilityHelper(self)
        self._hedge_helper = HedgeHelper(self)
        self._events_helper = EventsHelper(self)
        self._exposure_helper = ExposureHelper(self)
        self._orders_helper = OrdersHelper(self)
        self._closing_helper = ClosingHelper(self)
        self._risk_helper = RiskHelper(self)
        self._liquidation_helper = LiquidationHelper(self)

    async def on_start(self):
        self.logger().info(
            f"[Start] entry={self.maker_connector}:{self.maker_pair} hedge={self.hedge_connector}:{self.hedge_pair} side={self.side_maker.name}"
        )
        self._start_ts = float(getattr(self._strategy, "current_timestamp", 0))
        self._last_fill_ts = None
        self._desired_hedge_position_mode: PositionMode = (
            PositionMode.ONEWAY
            if self.hedge_connector == "hyperliquid_perpetual"
            else PositionMode.HEDGE
        )
        try:
            if self._desired_hedge_position_mode == PositionMode.HEDGE:
                try:
                    self._strategy.set_position_mode(
                        self.hedge_connector, PositionMode.HEDGE
                    )
                    self.logger().info(
                        f"[Position mode] Requested HEDGE on {self.hedge_connector}"
                    )
                except Exception as e:
                    self.logger().warning(
                        f"[Position mode] Could not set HEDGE on start: {e}"
                    )
            else:
                self.logger().info(
                    f"[Position mode] Using ONEWAY on {self.hedge_connector}"
                )
        except Exception as e:
            self.logger().warning(f"[Position mode] Setup error: {e}")
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
            self._maker_orders = [
                order for order in self._maker_orders if order.order_id != order_id
            ]
            self._maker_by_id.pop(order_id, None)
            self._maker_pending_ids.discard(order_id)
        else:
            self._hedge_orders = [
                order for order in self._hedge_orders if order.order_id != order_id
            ]
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
            self.logger().warning(
                f"Cancel: missing trading_pair for {order.order_id}, using configured pair {pair}"
            )
        try:
            self._strategy.cancel(conn, pair, order.order_id)
        except Exception as e:
            self.logger().warning(
                f"Cancel failed {order.order_id} on {conn}:{pair}: {e}"
            )
        if exec_base <= 0:
            self.remove_order(order.order_id, is_maker=is_maker)

    def _format_tracked_orders(self, orders: List[TrackedOrder]):
        return self._exposure_helper.format_tracked_orders(orders)

    def get_last_active_unfilled_order(self, is_maker: bool) -> Optional[TrackedOrder]:
        orders = self._maker_orders if is_maker else self._hedge_orders
        if orders:
            for order in reversed(orders):
                if order.is_filled:
                    continue
                if order.order is not None:
                    try:
                        if getattr(order.order, "is_open", False):
                            return order
                    except Exception:
                        pass
                if is_maker and order.order_id in self._maker_pending_ids:
                    return order
        return None

    def get_maker_order_usd_consumed(self) -> Decimal:
        return self._exposure_helper.get_maker_order_usd_consumed()

    def get_maker_order_usd_inflight(self) -> Decimal:
        return self._exposure_helper.get_maker_order_usd_inflight()

    def get_remaining_maker_cap(self) -> Decimal:
        return self._exposure_helper.get_remaining_maker_cap()

    def is_enough_maker_cap(self, remaining_cap: Optional[Decimal]) -> bool:
        return self._exposure_helper.is_enough_maker_cap(remaining_cap)

    def get_open_orders(self) -> List[TrackedOrder]:
        opened_orders = []
        for order in self._maker_orders + self._hedge_orders:
            try:
                if (
                    order.order
                    and getattr(order.order, "is_open", False)
                    and not order.is_filled
                ):
                    opened_orders.append(order)
            except Exception:
                if order.order and not order.is_filled:
                    opened_orders.append(order)
        return opened_orders

    def cancel_all_orders(self):
        for order in self.get_open_orders():
            self.cancel_order(order, is_maker=(order in self._maker_orders))

    def get_full_position_base_amount(self, is_maker: bool = True) -> Decimal:
        return self._exposure_helper.get_full_position_base_amount(is_maker=is_maker)

    def _add_fee_quote(self, trade_fee, price_mid: Decimal, pair: str):
        self._exposure_helper.add_fee_quote(trade_fee, price_mid, pair)

    async def validate_sufficient_balance(self):
        mid = self.get_price(self.maker_connector, self.maker_pair, PriceType.MidPrice)
        if mid.is_nan() or mid <= 0:
            self.logger().info(
                "[Validation] Entry mid price unavailable; proceeding but orders may fail."
            )
            return
        base, quote = self.maker_pair.split("-")

        self.logger().info(
            f"[Config] pair_notional_usd_cap: {self.pair_notional_usd_cap}, per_order_max_notional_usd: {self.per_order_max_notional_usd}, leverage: {self.leverage}"
        )
        remaining_cap = self.get_remaining_maker_cap()
        self.logger().info(
            f"[Validation] Sufficient {quote} margin for next order; remaining cap ${remaining_cap:.2f}"
        )
        if not self.is_enough_maker_cap(remaining_cap):
            self.logger().warning(
                f"[Validation] Not enough remaining maker cap to place new order, remaining cap: ${remaining_cap:.2f}, need at least: ${self.per_order_min_notional_usd:.2f}. Completing executor."
            )
            return

        next_notional_usd = min(
            remaining_cap, Decimal(str(self.per_order_max_notional_usd))
        )
        leverage = (
            self.leverage
            if getattr(self, "leverage", None) is not None
            else Decimal("1")
        )
        if leverage <= 0:
            leverage = Decimal("1")
        required_margin = Decimal(str(next_notional_usd)) / leverage
        self.logger().info(
            f"[Validation] connector {self.maker_connector} requires ~{required_margin:.6f} {quote}"
        )
        avail_quote = self.get_available_balance(self.maker_connector, quote)
        if avail_quote < required_margin:
            self.logger().warning(
                f"[Validation] Insufficient {quote} margin: need {required_margin:.6f} (next_notional {next_notional_usd:.6f}/lev {leverage}), have {avail_quote:.6f}."
            )

    def is_any_position_open(self) -> bool:
        pos_maker = self.get_full_position_base_amount(is_maker=True)
        pos_hedge = self.get_full_position_base_amount(is_maker=False)
        return (pos_maker > 0) or (pos_hedge > 0)

    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            if not self._ensure_setup():
                return

            now = self._strategy.current_timestamp

            if self._handle_monitoring(now):
                return

            self._closing_helper.handle_close_ttl(now)

            if self._check_timeouts(now):
                return

            last_unfilled_order = self.get_last_active_unfilled_order(is_maker=True)

            if last_unfilled_order is None and self._closing_current is None:
                await self._process_new_orders(now)
            elif last_unfilled_order and not self._opening_fully_completed:
                self._process_maker_ttl(now, last_unfilled_order)

        elif self.status == RunnableStatus.SHUTTING_DOWN:
            pass

    def _ensure_setup(self) -> bool:
        if not self._risk_helper.ensure_position_mode():
            return False

        if not self._leverage_applied:
            self._risk_helper.apply_leverage_once()
        return True

    def _handle_monitoring(self, now: float) -> bool:
        if self._opening_fully_completed and not self._closing:
            self._liquidation_helper.monitor(now)
            if not self.is_any_position_open():
                self.early_stop()
                return True

            self._funding_helper.monitor_and_maybe_trigger_exit(now)
            return True
        return False

    def _check_timeouts(self, now: float) -> bool:
        timeout = self.fill_timeout_sec
        if not self._closing:
            start_ts = self._start_ts or 0.0
            last_fill_ts = self._last_fill_ts or 0.0
            elapsed_since_start = (now - start_ts) if start_ts else 0.0
            elapsed_since_last_fill = (now - last_fill_ts) if last_fill_ts else None
            maker_pos_base = self.get_full_position_base_amount(is_maker=True)
            if maker_pos_base <= 0:
                if start_ts and elapsed_since_start >= timeout:
                    self.logger().info(
                        f"[Timeout] No fills for {elapsed_since_start:.0f}s >= {timeout:.0f}s; stopping executor."
                    )
                    self.early_stop()
                    return True
            else:
                if (
                    last_fill_ts
                    and elapsed_since_last_fill is not None
                    and elapsed_since_last_fill >= timeout
                ):
                    try:
                        cancelled_cnt = 0
                        for order in list(self._maker_orders):
                            try:
                                if (
                                    order.order
                                    and getattr(order.order, "is_open", False)
                                    and not order.is_filled
                                ):
                                    self.logger().info(
                                        f"[Timeout] Cancelling stale maker order {order.order_id} before funding monitoring."
                                    )
                                    self.cancel_order(order, is_maker=True)
                                    cancelled_cnt += 1
                            except Exception:
                                continue
                        if cancelled_cnt > 0:
                            self.logger().info(
                                f"[Timeout] Cancelled {cancelled_cnt} open maker order(s) before entering funding monitoring."
                            )
                    except Exception as e:
                        self.logger().warning(
                            f"[Timeout] Error cancelling open maker orders: {e}"
                        )
                    self._mark_opening_completed("fill_timeout_enter_monitoring")
                    self.logger().info(
                        f"[Timeout] Partial fills but inactive for {elapsed_since_last_fill:.0f}s >= {timeout:.0f}s; entering funding monitoring (no new opens)."
                    )
                    return True
        return False

    async def _process_new_orders(self, now: float):
        if self._opening_fully_completed and (not self._closing):
            return
        if self._maker_pending_ids:
            return

        if not self._closing:
            remaining_cap = self.get_remaining_maker_cap()
            if not self.is_enough_maker_cap(remaining_cap):
                self.logger().warning(
                    f"[Validation] Not enough remaining maker cap to place new order, remaining cap: ${remaining_cap:.2f}, need at least: ${self.per_order_min_notional_usd:.2f}. Completing executor."
                )
                self._mark_opening_completed("maker_cap_exhausted")
                return
            await self.validate_sufficient_balance()

        if now < self._next_order_ready_ts:
            return

        await self._place_next_part()

    def _process_maker_ttl(self, now: float, last_unfilled_order: TrackedOrder):
        if not self._closing and self.maker_ttl_sec and self.maker_ttl_sec > 0:
            unfilled_maker_creation_time = (
                getattr(last_unfilled_order, "creation_timestamp", 0) or 0
            )
            if unfilled_maker_creation_time > 0 and (
                now - unfilled_maker_creation_time
            ) >= float(self.maker_ttl_sec):
                is_open = False
                try:
                    is_open = bool(getattr(last_unfilled_order.order, "is_open", False))
                except Exception:
                    is_open = False
                if (
                    is_open
                    and last_unfilled_order.order_id not in self._maker_cancel_pending
                ):
                    self.logger().info(
                        f"[TTL] Cancelling stale maker order {last_unfilled_order.order_id} due to TTL exceeded."
                    )
                    self._maker_cancel_pending.add(last_unfilled_order.order_id)
                    self.cancel_order(last_unfilled_order, is_maker=True)

    def _get_mid(self) -> Decimal:
        px = self.get_price(self.maker_connector, self.maker_pair, PriceType.MidPrice)
        return Decimal("NaN") if px.is_nan() else px

    def _get_best_bid(self) -> Optional[Decimal]:
        try:
            bid = self.get_price(
                self.maker_connector, self.maker_pair, PriceType.BestBid
            )
            return None if bid.is_nan() else bid
        except Exception:
            return None

    def _get_best_ask(self) -> Optional[Decimal]:
        try:
            ask = self.get_price(
                self.maker_connector, self.maker_pair, PriceType.BestAsk
            )
            return None if ask.is_nan() else ask
        except Exception:
            return None

    def _check_profitability_enter_condition(
        self, maker_side: TradeType, maker_price: Decimal, amount: Decimal
    ) -> bool:
        return self._profitability_helper.check_enter_condition(
            maker_side, maker_price, amount
        )

    def _compute_limit_price(
        self, side: TradeType, mid: Optional[Decimal] = None
    ) -> Optional[Decimal]:
        return self._orders_helper.compute_limit_price(side, mid)

    async def _place_next_part(self):
        await self._orders_helper.place_next_part()

    def _finalize_hedge_tail(self):
        self._closing_helper.finalize_hedge_tail()

    def _maker_remaining_exposure_base(self) -> Decimal:
        return self._exposure_helper.maker_remaining_exposure_base()

    def _hedge_remaining_exposure_base(self) -> Decimal:
        return self._exposure_helper.hedge_remaining_exposure_base()

    def _build_close_queue_if_needed(self):
        self._closing_helper.build_close_queue_if_needed()

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
        return (
            getattr(market, "name", None)
            or getattr(market, "connector_name", None)
            or ""
        )

    def process_order_created_event(
        self,
        event_tag: int,
        market: ConnectorBase,
        event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent],
    ):
        return self._events_helper.process_order_created(event_tag, market, event)

    def process_order_filled_event(
        self, event_tag: int, market: ConnectorBase, event: OrderFilledEvent
    ):
        return self._events_helper.process_order_filled(event_tag, market, event)

    def process_order_completed_event(
        self,
        event_tag: int,
        market: ConnectorBase,
        event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent],
    ):
        return self._events_helper.process_order_completed(event_tag, market, event)

    def process_order_canceled_event(
        self, event_tag: int, market: ConnectorBase, event: OrderCancelledEvent
    ):
        result = self._events_helper.process_order_canceled(event_tag, market, event)
        try:
            oid = getattr(event, "order_id", None)
            if oid:
                self._maker_cancel_pending.discard(oid)
        except Exception:
            pass
        return result

    def process_order_failed_event(
        self, event_tag: int, market: ConnectorBase, event: MarketOrderFailureEvent
    ):
        return self._events_helper.process_order_failed(event_tag, market, event)

    def process_funding_payment_event(
        self, event_tag: int, market: ConnectorBase, event
    ):
        return self._events_helper.process_funding_payment(event_tag, market, event)

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
            rates = self._funding_helper.get_normalized_funding_rates() or {}
            entry_rate_sec = rates.get("entry")
            hedge_rate_sec = rates.get("hedge")
            hours = self.funding_profitability_interval_hours
            abs_diff_pct = self._funding_helper._get_funding_diff_pct(
                funding_interval_hours=hours
            )
            oriented_diff_pct = self._funding_helper.get_oriented_funding_diff_pct(
                funding_interval_hours=hours
            )
        except Exception:
            pass

        try:
            mdp = getattr(self._strategy, "market_data_provider", None)
            if mdp is not None:
                entry_fi = mdp.get_funding_info(self.maker_connector, self.maker_pair)
                hedge_fi = mdp.get_funding_info(self.hedge_connector, self.hedge_pair)
                now_ts = self._strategy.current_timestamp
                if entry_fi is not None:
                    minutes_to_funding_entry = util_minutes_to_next_funding(
                        entry_fi.next_funding_utc_timestamp, now_ts
                    )
                if hedge_fi is not None:
                    minutes_to_funding_hedge = util_minutes_to_next_funding(
                        hedge_fi.next_funding_utc_timestamp, now_ts
                    )
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

        custom_info: Dict[str, Any] = {
            "maker_connector": self.maker_connector,
            "maker_pair": self.maker_pair,
            "hedge_connector": self.hedge_connector,
            "hedge_pair": self.hedge_pair,
            "side": self.side_maker.name,
            "maker_position_base": maker_pos_base,
            "maker_position_quote": (
                maker_pos_base
                * self.get_price(
                    self.maker_connector, self.maker_pair, PriceType.MidPrice
                )
            ),
            "hedge_position_base": hedge_pos_base,
            "maker_unrealized_pnl": self._maker_unrealized_pnl,
            "hedge_unrealized_pnl": self._hedge_unrealized_pnl,
            "net_pnl_quote": self.get_net_pnl_quote(),
            "net_pnl_pct": self.get_net_pnl_pct(),
            "funding_pnl_quote_maker": self._cum_funding_maker_quote,
            "funding_pnl_quote_hedge": self._cum_funding_hedge_quote,
            "funding_pnl_quote_net": (
                self._cum_funding_maker_quote + self._cum_funding_hedge_quote
            ),
            "funding_pnl_quote": (
                self._cum_funding_maker_quote + self._cum_funding_hedge_quote
            ),
            "funding_entry_rate_sec": entry_rate_sec,
            "funding_hedge_rate_sec": hedge_rate_sec,
            "funding_diff_pct": abs_diff_pct,
            "funding_oriented_diff_pct": oriented_diff_pct,
            "minutes_to_funding_entry": minutes_to_funding_entry,
            "minutes_to_funding_hedge": minutes_to_funding_hedge,
            "maker_open_orders": maker_open_orders,
            "hedge_open_orders": hedge_open_orders,
            "held_position_orders": [],
            "last_diff_pct_to_liquidation_maker": self._last_diff_pct_to_liquidation_maker,
            "last_diff_pct_to_liquidation_hedge": self._last_diff_pct_to_liquidation_hedge,
            "last_liquidation_price_maker": self._last_liquidation_price_maker,
            "last_liquidation_price_hedge": self._last_liquidation_price_hedge,
        }

        safe_ensure_future(self._publish_custom_info_event(custom_info))

        return custom_info

    def close_all_positions_by_market(self):
        self._closing = True
        maker_exposure = self._maker_remaining_exposure_base()
        hedge_exposure = self._hedge_remaining_exposure_base()
        self.logger().info(
            f"[Close] Closing maker via market: {maker_exposure} {self.maker_pair}"
        )
        self.logger().info(
            f"[Close] Closing hedge via market: {hedge_exposure} {self.hedge_pair}"
        )

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
        self.logger().info(f"[Stop] Early stop: keep_position={keep_position}")
        self.cancel_all_orders()

        if keep_position:
            self.close_type = CloseType.POSITION_HOLD
            self.stop()
            return

        self._closing = True
        self.close_all_positions_by_market()
        self.close_type = CloseType.EARLY_STOP
        self.logger().info(
            f"[Close] Starting FIFO with {len(self._close_queue)} items; maker orders to close: {[o['open_id'] for o in self._close_queue]}"
        )

    def custom_command(
        self, custom_command: str, params: Optional[Dict[str, Any]] = None
    ):
        if custom_command == "early_stop":
            self._closing = True
            self.early_stop()
        elif custom_command == "close_by_limit":
            self.cancel_all_orders()
            self._mark_opening_completed("close_by_limit")
            self._closing = True
        else:
            self.logger().info(
                f"[Manual] Custom command triggered on executor {self.config.id}: {custom_command}, params={params}"
            )

    def _mark_opening_completed(self, reason: str):
        if self._opening_fully_completed:
            return
        self._opening_fully_completed = True
        self.logger().info(f"[Opening Completed] Marked as completed due to: {reason}")
        self._publish_opening_completed_event(reason)

    def _publish_opening_completed_event(self, reason: str):
        timestamp = float(
            getattr(self._strategy, "current_timestamp", time.time()) or time.time()
        )
        payload = {
            "event": "opening_completed",
            "reason": reason,
            "timestamp": timestamp,
        }

        self._publish_executor_topic(payload)

    async def _publish_closing_completed_event(self, reason: str):
        timestamp = float(
            getattr(self._strategy, "current_timestamp", time.time()) or time.time()
        )
        payload = {
            "event": "closing_completed",
            "reason": reason,
            "timestamp": timestamp,
        }

        self._publish_executor_topic(payload)
        await asyncio.sleep(0.1)

    async def _publish_custom_info_event(self, info: Dict[str, Any]):
        if self._closing_completed_event_published:
            return

        timestamp = float(
            getattr(self._strategy, "current_timestamp", time.time()) or time.time()
        )
        payload = {
            "event": "custom_info",
            "info": info,
            "timestamp": timestamp,
        }

        self._publish_executor_topic(payload)
        await asyncio.sleep(0.1)

    def on_stop(self):
        if self._closing_completed_event_published:
            return

        if self.close_type is None:
            return

        reason = (
            self.close_type.name
            if hasattr(self.close_type, "name")
            else str(self.close_type)
        )

        try:
            safe_ensure_future(self._publish_closing_completed_event(reason))
        except Exception as e:
            try:
                self.logger().debug(
                    f"[Close] Failed to schedule closing completion publish: {e}"
                )
            except Exception:
                pass
        finally:
            self._closing_completed_event_published = True
            super().on_stop()

    def _publish_executor_topic(self, payload: Dict[str, Any]):
        if self._executor_events_pub is None:
            try:
                self._executor_events_pub = ETopicPublisher(
                    "executors/events", use_bot_prefix=True
                )
            except Exception as e:
                self.logger().debug(
                    f"[MQTT] Unable to initialize executor event publisher: {e}"
                )
                self._executor_events_pub = None
                return
        if self._executor_events_pub is None:
            return

        try:
            self._executor_events_pub.send(payload)
        except Exception as e:
            self.logger().debug(f"[MQTT] Failed to publish executor event: {e}")

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel

from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.core.data_type.common import MarketDict, PriceType, TradeType
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.maker_hedge_single_executor.data_types import MakerHedgeSingleExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo
from hummingbot.strategy_v2.utils.funding import (
    funding_diff_pct as util_funding_diff_pct,
    minutes_to_next_funding as util_minutes_to_next_funding,
    normalized_funding_rate_in_seconds as util_normalized_funding_rate_in_seconds,
)


class PairConfig(BaseModel):
    base: str
    per_exchange_quote: dict
    total_notional_usd_per_pair: Decimal = Decimal("0")
    max_notional_per_part: Decimal = Decimal("0")
    min_notional_per_part: Decimal = Decimal("0")


class SignalConfig(BaseModel):
    min_funding_rate_profitability_pct: Decimal = Decimal("0")
    funding_profitability_interval_hours: int = 24


class MakerConfig(BaseModel):
    price_offset_bp: Decimal = Decimal("0.5")
    ttl_sec: int = 20
    refresh_on_stale: bool = True


class HedgeConfig(BaseModel):
    min_hedge_notional_usd: Decimal = Decimal("200")


class ExecutionConfig(BaseModel):
    total_notional_usd: Decimal = Decimal("2000")
    leverage: Decimal = Decimal("1")
    part_interval_sec: int = 8
    entry_cooldown_sec: int = 120
    maker: MakerConfig = MakerConfig()
    hedge: HedgeConfig = HedgeConfig()


class RiskConfig(BaseModel):
    max_groups_per_pair: int = 2


class ExitConfig(BaseModel):
    fr_spread_below_pct: Optional[Decimal] = None
    hold_below_sec: int = 60


class FundingRateArbControllerConfig(ControllerConfigBase):
    controller_name: str = "funding_rate_arb_controller"
    controller_type: str = "generic"
    connectors: List[str] = []
    pairs: List[PairConfig] = []
    signal: SignalConfig = SignalConfig()
    execution: ExecutionConfig = ExecutionConfig()
    risk: RiskConfig = RiskConfig()
    exit: ExitConfig = ExitConfig()

    def update_markets(self, markets: MarketDict) -> MarketDict:
        for p in self.pairs:
            for connector_name in self.connectors:
                quote = p.per_exchange_quote.get(connector_name)
                if not quote:
                    continue
                tp = f"{p.base}-{quote}"
                markets.add_or_update(connector_name, tp)
        return markets


class FundingRateArbController(ControllerBase):
    """
    Minimal working controller skeleton that proposes a Maker+Hedge single-executor
    when there is no active executor for a pair. Edge/liquidity checks are placeholders.
    """

    FUNDING_INTERVAL_FALLBACKS: Dict[str, int] = {
        "bybit_perpetual": 60 * 60 * 8,
        "hyperliquid_perpetual": 60 * 60 * 1,
        "okx_perpetual": 60 * 60 * 8,
    }

    DEFAULT_FUNDING_PROFITABILITY_INTERVAL_HOURS: int = 24

    def __init__(self, config: FundingRateArbControllerConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config: FundingRateArbControllerConfig
        self._last_entry_ts_by_pair = {}

    async def update_processed_data(self):
        pair_metrics: Dict[tuple, Dict] = {}
        current_ts = self.market_data_provider.time()

        for pair_config in self.config.pairs:
            connectors = self._get_available_connectors_for_pair(pair_config)
            if len(connectors) < 2:
                continue

            # If any active executor already uses this base across any connector combination, skip computing new metrics
            active_for_base = False
            for ei in self.executors_info:
                if not ei.is_active or ei.is_done:
                    continue
                cfg = getattr(ei, "config", None)
                maker_market = getattr(cfg, "maker_market", None)
                if maker_market is None:
                    continue
                trading_pair = getattr(maker_market, "trading_pair", "")
                if "-" not in trading_pair:
                    continue
                maker_base = trading_pair.split("-", 1)[0]
                if maker_base == pair_config.base:
                    active_for_base = True
                    break
            if active_for_base:
                continue

            # Warm mid prices for all connector/trading-pair combinations
            for connector_name in connectors:
                quote = pair_config.per_exchange_quote.get(connector_name)
                if not quote:
                    continue
                trading_pair = f"{pair_config.base}-{quote}"
                _ = self.market_data_provider.get_price_by_type(connector_name, trading_pair, PriceType.MidPrice)

            for entry_ex in connectors:
                entry_quote = pair_config.per_exchange_quote.get(entry_ex)
                if not entry_quote:
                    continue
                entry_tp = f"{pair_config.base}-{entry_quote}"

                for hedge_ex in connectors:
                    if hedge_ex == entry_ex:
                        continue
                    hedge_quote = pair_config.per_exchange_quote.get(hedge_ex)
                    if not hedge_quote:
                        continue
                    hedge_tp = f"{pair_config.base}-{hedge_quote}"

                    _ = self.market_data_provider.get_price_by_type(hedge_ex, hedge_tp, PriceType.MidPrice)

                    metrics = self._compute_pair_metrics(
                        pair_config=pair_config,
                        entry_connector=entry_ex,
                        entry_trading_pair=entry_tp,
                        hedge_connector=hedge_ex,
                        hedge_trading_pair=hedge_tp,
                        current_ts=current_ts,
                    )

                    pair_metrics[(pair_config.base, entry_ex, entry_tp, hedge_ex, hedge_tp)] = metrics

                    entry_rate_sec = metrics.get("entry_rate_sec")
                    hedge_rate_sec = metrics.get("hedge_rate_sec")
                    funding_rate_diff_pct = metrics.get("funding_rate_diff_pct")
                    funding_interval_hours = getattr(self.config.signal, "funding_profitability_interval_hours", self.DEFAULT_FUNDING_PROFITABILITY_INTERVAL_HOURS)
                    entry_rate_pct_for_funding_interval_hours = entry_rate_sec * 3600 * 100 * funding_interval_hours if entry_rate_sec is not None else None
                    hedge_rate_pct_for_funding_interval_hours = hedge_rate_sec * 3600 * 100 * funding_interval_hours if hedge_rate_sec is not None else None
                    if entry_rate_sec is not None and hedge_rate_sec is not None:
                        entry_rate_pct_for_funding_interval_hours_str = f"{entry_rate_pct_for_funding_interval_hours:.6f}"
                        hedge_rate_pct_for_funding_interval_hours_str = f"{hedge_rate_pct_for_funding_interval_hours:.6f}"
                        funding_diff_str = "n/a" if funding_rate_diff_pct is None else f"{funding_rate_diff_pct:.6f}"
                        trade_side = metrics.get("trade_side")
                        quote_volume = metrics.get("quote_volume")
                        self.logger().info(
                            "[FUNDING] %s entry=%s rate/interval=%s hedge=%s rate/interval=%s diff=%s side=%s quote_notional=%s",
                            pair_config.base,
                            f"{entry_ex}:{entry_tp}",
                            entry_rate_pct_for_funding_interval_hours_str,
                            f"{hedge_ex}:{hedge_tp}",
                            hedge_rate_pct_for_funding_interval_hours_str,
                            funding_diff_str,
                            getattr(trade_side, "name", "-"),
                            str(quote_volume) if quote_volume is not None else "-",
                        )

        self.processed_data["pair_metrics"] = pair_metrics

    def _get_available_connectors_for_pair(self, pair_config: PairConfig) -> List[str]:
        """
        Return list of connectors from global config that have a quote asset defined for this pair's base.
        Filters out connectors missing quotes or with empty strings.
        """
        connectors: List[str] = []
        for connector_name in self.config.connectors:
            quote = pair_config.per_exchange_quote.get(connector_name)
            if quote:
                connectors.append(connector_name)
        return connectors

    def _count_active_groups_for_base(self, base: str) -> int:
        """Counts active executors (maker legs) whose trading pair base matches given base symbol."""
        count = 0
        for ei in self.executors_info:
            if not ei.is_active or ei.is_done:
                continue
            cfg = getattr(ei, "config", None)
            maker_market = getattr(cfg, "maker_market", None)
            if maker_market is None:
                continue
            trading_pair = getattr(maker_market, "trading_pair", "")
            if "-" not in trading_pair:
                continue
            maker_base = trading_pair.split("-", 1)[0]
            if maker_base == base:
                count += 1
        return count

    def get_active_executors_for_trading_pair(self, trading_pair: str) -> List[ExecutorInfo]:
        active_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda e: (
                e.is_active and
                e.trading_pair == trading_pair
            )
        )
        return active_executors

    def determine_executor_actions(self) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        pair_metrics_map: Dict[tuple, Dict] = self.processed_data.get("pair_metrics", {})

        # Track active global allocation from running executors to not exceed total_notional_usd
        active_alloc_usd = Decimal("0")
        for ei in self.filter_executors(self.executors_info, lambda e: e.is_active and not e.is_done):
            cfg = ei.config
            cap = getattr(cfg, "pair_notional_usd_cap", None)
            if cap is not None:
                active_alloc_usd += Decimal(str(cap))

        for pair_config in self.config.pairs:
            connectors = self._get_available_connectors_for_pair(pair_config)
            if len(connectors) < 2:
                continue

            max_groups = getattr(self.config.risk, "max_groups_per_pair", None)
            if max_groups and max_groups > 0 and self._count_active_groups_for_base(pair_config.base) >= max_groups:
                continue

            combos: List[tuple] = []
            for (base, entry_ex, entry_tp, hedge_ex, hedge_tp), metrics in pair_metrics_map.items():
                if base != pair_config.base:
                    continue
                if entry_ex not in connectors or hedge_ex not in connectors:
                    continue
                combos.append(((entry_ex, entry_tp, hedge_ex, hedge_tp), metrics))

            if not combos:
                continue

            def _score(metric_dict: Dict) -> Decimal:
                value = metric_dict.get("funding_rate_diff_pct")
                if value is None:
                    return Decimal("-1")
                return Decimal(str(value))

            combos.sort(key=lambda item: _score(item[1]), reverse=True)

            for (entry_ex, entry_tp, hedge_ex, hedge_tp), metrics in combos:
                entry_mid = self.market_data_provider.get_price_by_type(entry_ex, entry_tp, PriceType.MidPrice)
                if entry_mid.is_nan() or entry_mid <= 0:
                    continue

                funding_rate_diff_pct = metrics.get("funding_rate_diff_pct")
                if funding_rate_diff_pct is None:
                    self.logger().info("[SKIP] %s %s->%s funding diff unavailable", entry_tp, entry_ex, hedge_ex)
                    continue

                # Compare in percent units (e.g., 0.02 means 0.02%)
                if funding_rate_diff_pct < self.config.signal.min_funding_rate_profitability_pct:
                    self.logger().info(
                        "[SKIP] %s %s->%s funding diff pct=%s below threshold pct=%s",
                        entry_tp,
                        entry_ex,
                        hedge_ex,
                        f"{funding_rate_diff_pct:.6f}",
                        f"{self.config.signal.min_funding_rate_profitability_pct:.6f}",
                    )
                    continue

                active_for_pair = self.get_active_executors_for_trading_pair(entry_tp)
                if any(not ei.is_done for ei in active_for_pair):
                    self.logger().info(f"[SKIP] {entry_tp} {entry_ex}->{hedge_ex} already has active executor")
                    continue

                now_ts = self.market_data_provider.time()
                cooldown_key = (entry_ex, entry_tp)
                last_ts = self._last_entry_ts_by_pair.get(cooldown_key, 0)
                if now_ts - last_ts < self.config.execution.entry_cooldown_sec:
                    self.logger().info(f"[SKIP] {entry_tp} {entry_ex}->{hedge_ex} in cooldown")
                    continue

                self.logger().info(f"cooldown check passed for {entry_tp} {entry_ex}->{hedge_ex}")
                pair_cap_usd: Decimal = pair_config.total_notional_usd_per_pair if pair_config.total_notional_usd_per_pair > 0 else self.config.execution.total_notional_usd
                if active_alloc_usd + pair_cap_usd > self.config.execution.total_notional_usd:
                    self.logger().info(f"[SKIP] {entry_tp} {entry_ex}->{hedge_ex} would exceed total allocation")
                    continue

                maker_side = metrics.get("trade_side", TradeType.BUY)

                exec_cfg = MakerHedgeSingleExecutorConfig(
                    timestamp=self.market_data_provider.time(),
                    maker_market=ConnectorPair(connector_name=entry_ex, trading_pair=entry_tp),
                    hedge_market=ConnectorPair(connector_name=hedge_ex, trading_pair=hedge_tp),
                    side_maker=maker_side.name,
                    leverage=self.config.execution.leverage,
                    pair_notional_usd_cap=pair_cap_usd,
                    per_order_max_notional_usd=pair_config.max_notional_per_part,
                    per_order_min_notional_usd=pair_config.min_notional_per_part,
                    order_interval_sec=self.config.execution.part_interval_sec,
                    maker_price_offset_bp=self.config.execution.maker.price_offset_bp,
                    maker_ttl_sec=self.config.execution.maker.ttl_sec,
                    maker_refresh_on_stale=self.config.execution.maker.refresh_on_stale,
                    hedge_min_notional_usd=self.config.execution.hedge.min_hedge_notional_usd,
                    exit_funding_diff_pct_threshold=self.config.exit.fr_spread_below_pct,
                    exit_hold_below_sec=self.config.exit.hold_below_sec,
                    funding_profitability_interval_hours=self.config.signal.funding_profitability_interval_hours,
                )

                actions.append(CreateExecutorAction(executor_config=exec_cfg, controller_id=self.config.id))
                self.logger().info(
                    "[ENTER] %s %s->%s exec side=%s funding diff pct=%s cap_usd=%s",
                    entry_tp,
                    entry_ex,
                    hedge_ex,
                    maker_side.name,
                    f"{funding_rate_diff_pct:.6f}" if funding_rate_diff_pct is not None else "n/a",
                    f"{pair_cap_usd:.2f}",
                )

                self._last_entry_ts_by_pair[cooldown_key] = now_ts
                active_alloc_usd += pair_cap_usd
                break  # create at most one executor per pair per cycle
        return actions

    def _select_quote_volume(self, pair_config: PairConfig) -> Optional[Decimal]:
        candidates = [
            pair_config.max_notional_per_part,
            pair_config.total_notional_usd_per_pair,
            self.config.execution.total_notional_usd,
        ]
        for candidate in candidates:
            candidate_dec = Decimal(str(candidate))
            if candidate_dec > 0:
                return candidate_dec
        return None

    def _normalized_funding_rate_in_seconds(self, funding_info, connector: str) -> Optional[Decimal]:
        trade_pair = funding_info.trading_pair
        rate = funding_info.rate
        interval = funding_info.funding_interval
        self.logger().info(f"Details - trading_pair: {trade_pair}, rate: {rate}, interval: {interval}")
        # Delegate to shared utility with controller-specific fallbacks
        return util_normalized_funding_rate_in_seconds(
            funding_info=funding_info,
            connector=connector,
            fallbacks=self.FUNDING_INTERVAL_FALLBACKS,
        )

    @staticmethod
    def _minutes_to_next_funding(next_funding_timestamp: Optional[int], current_ts: float) -> Optional[Decimal]:
        return util_minutes_to_next_funding(next_funding_timestamp, current_ts)

    def _compute_pair_metrics(
        self,
        pair_config: PairConfig,
        entry_connector: str,
        entry_trading_pair: str,
        hedge_connector: str,
        hedge_trading_pair: str,
        current_ts: float,
    ) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {
            "funding_rate_diff_pct": None,
            "trade_side": TradeType.BUY,
            "entry_rate_sec": None,
            "hedge_rate_sec": None,
            "entry_minutes_to_funding": None,
            "hedge_minutes_to_funding": None,
            "quote_volume": None,
        }

        entry_funding = self.market_data_provider.get_funding_info(entry_connector, entry_trading_pair)
        self.logger().debug(f"Entry funding info: {entry_funding}")
        hedge_funding = self.market_data_provider.get_funding_info(hedge_connector, hedge_trading_pair)
        self.logger().debug(f"Hedge funding info: {hedge_funding}")

        if entry_funding is None or hedge_funding is None:
            return metrics

        entry_rate_sec = self._normalized_funding_rate_in_seconds(entry_funding, entry_connector)
        self.logger().debug(f"Entry rate (sec): {entry_rate_sec}")
        hedge_rate_sec = self._normalized_funding_rate_in_seconds(hedge_funding, hedge_connector)
        self.logger().debug(f"Hedge rate (sec): {hedge_rate_sec}")

        metrics["entry_rate_sec"] = entry_rate_sec
        metrics["hedge_rate_sec"] = hedge_rate_sec

        if entry_rate_sec is None or hedge_rate_sec is None:
            return metrics

        interval_hours = self.config.signal.funding_profitability_interval_hours or self.DEFAULT_FUNDING_PROFITABILITY_INTERVAL_HOURS
        # Use shared util to compute human percent over the configured horizon
        diff_pct_signed = util_funding_diff_pct(entry_rate_sec, hedge_rate_sec, hours=int(interval_hours))
        funding_rate_diff_pct = abs(Decimal(str(diff_pct_signed))) if diff_pct_signed is not None else None

        metrics["funding_rate_diff_pct"] = funding_rate_diff_pct
        trade_side = TradeType.BUY if entry_rate_sec < hedge_rate_sec else TradeType.SELL
        metrics["trade_side"] = trade_side

        metrics["entry_minutes_to_funding"] = self._minutes_to_next_funding(entry_funding.next_funding_utc_timestamp, current_ts)
        metrics["hedge_minutes_to_funding"] = self._minutes_to_next_funding(hedge_funding.next_funding_utc_timestamp, current_ts)

        quote_volume = self._select_quote_volume(pair_config)
        metrics["quote_volume"] = quote_volume

        return metrics

    def on_stop(self):
        self.logger().info("FundingRateArbController stopping. Current open positions:")
        return super().on_stop()

    def to_format_status(self) -> List[str]:
        def _to_decimal(value) -> Decimal:
            if isinstance(value, Decimal):
                return value
            if value in (None, "", "NaN"):
                return Decimal("0")
            return Decimal(str(value))

        def _decimal_to_str(value: Decimal) -> str:
            if not isinstance(value, Decimal):
                value = Decimal(str(value))
            if value.is_nan():
                return "0"
            formatted = format(value, "f")
            if "." in formatted:
                formatted = formatted.rstrip("0").rstrip(".")
            return formatted or "0"

        def _format_price(price) -> str:
            numeric = Decimal(str(price))
            if numeric.is_nan():
                return "NaN"
            return f"{numeric:.6f}"

        summary_rows = []
        executor_rows = []
        order_rows = []
        pair_metrics_map: Dict[tuple, Dict] = self.processed_data.get("pair_metrics", {})

        for p in self.config.pairs:
            connectors = self._get_available_connectors_for_pair(p)
            if len(connectors) < 2:
                continue

            # Build rows for every metrics combination we computed for this base
            for (base, entry_ex, entry_tp, hedge_ex, hedge_tp), metrics in pair_metrics_map.items():
                if base != p.base:
                    continue
                if entry_ex not in connectors or hedge_ex not in connectors:
                    continue
                maker_mid_raw = self.market_data_provider.get_price_by_type(entry_ex, entry_tp, PriceType.MidPrice)
                hedge_mid_raw = self.market_data_provider.get_price_by_type(hedge_ex, hedge_tp, PriceType.MidPrice)

                funding_rate_diff_pct = metrics.get("funding_rate_diff_pct")
                trade_side_metric = metrics.get("trade_side")
                entry_minutes_to_funding = metrics.get("entry_minutes_to_funding")
                hedge_minutes_to_funding = metrics.get("hedge_minutes_to_funding")
                quote_volume = metrics.get("quote_volume")

                # Collect executors whose maker leg matches this entry pair+connector
                relevant_executors = []
                for ei in self.executors_info:
                    config = getattr(ei, "config", None)
                    maker_market = getattr(config, "maker_market", None)
                    hedge_market = getattr(config, "hedge_market", None)
                    if maker_market and hedge_market:
                        if maker_market.connector_name == entry_ex and maker_market.trading_pair == entry_tp and \
                                hedge_market.connector_name == hedge_ex and hedge_market.trading_pair == hedge_tp:
                            relevant_executors.append(ei)

                total_maker_pos = Decimal("0")
                total_hedge_pos = Decimal("0")
                total_open_orders = 0

                for ei in relevant_executors:
                    info = getattr(ei, "custom_info", {}) or {}
                    maker_pos = _to_decimal(info.get("maker_position_base", "0"))
                    hedge_pos = _to_decimal(info.get("hedge_position_base", "0"))
                    total_maker_pos += maker_pos
                    total_hedge_pos += hedge_pos

                    maker_orders = info.get("maker_open_orders", []) or []
                    hedge_orders = info.get("hedge_open_orders", []) or []
                    total_open_orders += len(maker_orders) + len(hedge_orders)

                    executor_rows.append({
                        "pair": p.base,
                        "executor": ei.id,
                        "status": getattr(ei.status, "name", str(ei.status)),
                        "side": info.get("side", "-"),
                        "maker_pos": _decimal_to_str(maker_pos),
                        "hedge_pos": _decimal_to_str(hedge_pos),
                        "orders": len(maker_orders) + len(hedge_orders),
                        "net_pnl_quote": _decimal_to_str(ei.net_pnl_quote),
                        "net_pnl_pct": _decimal_to_str(ei.net_pnl_pct),
                    })

                    for order in maker_orders:
                        order_rows.append({
                            "pair": p.base,
                            "executor": ei.id,
                            "leg": "maker",
                            "order_id": order.get("id"),
                            "side": order.get("side"),
                            "price": order.get("px"),
                            "amount": order.get("amt"),
                            "filled": order.get("exec_base"),
                            "state": order.get("state"),
                        })

                    for order in hedge_orders:
                        order_rows.append({
                            "pair": p.base,
                            "executor": ei.id,
                            "leg": "hedge",
                            "order_id": order.get("id"),
                            "side": order.get("side"),
                            "price": order.get("px"),
                            "amount": order.get("amt"),
                            "filled": order.get("exec_base"),
                            "state": order.get("state"),
                        })

                summary_rows.append({
                    "pair": f"{p.base}",
                    "entry": f"{entry_ex}:{entry_tp}",
                    "hedge": f"{hedge_ex}:{hedge_tp}",
                    "maker_mid": _format_price(maker_mid_raw),
                    "hedge_mid": _format_price(hedge_mid_raw),
                    "active_exec": sum(1 for ei in relevant_executors if ei.is_active),
                    "maker_pos": _decimal_to_str(total_maker_pos),
                    "hedge_pos": _decimal_to_str(total_hedge_pos),
                    "open_orders": total_open_orders,
                    "funding_diff_%": _decimal_to_str(funding_rate_diff_pct) if funding_rate_diff_pct is not None else "-",
                    "side": getattr(trade_side_metric, "name", "-"),
                    "quote_notional": _decimal_to_str(quote_volume) if quote_volume is not None else "-",
                    "min_to_funding_entry": _decimal_to_str(entry_minutes_to_funding) if entry_minutes_to_funding is not None else "-",
                    "min_to_funding_hedge": _decimal_to_str(hedge_minutes_to_funding) if hedge_minutes_to_funding is not None else "-",
                })

        if not summary_rows:
            return ["No pairs configured."]

        outputs: List[str] = []
        summary_df = pd.DataFrame(summary_rows)
        outputs.append("Active pairs summary:\n" + format_df_for_printout(summary_df, table_format="psql", index=False))

        if executor_rows:
            executor_df = pd.DataFrame(executor_rows)
            outputs.append("Active executors:\n" + format_df_for_printout(executor_df, table_format="psql", index=False))

        if order_rows:
            orders_df = pd.DataFrame(order_rows)
            outputs.append("Open orders:\n" + format_df_for_printout(orders_df, table_format="psql", index=False))

        return outputs

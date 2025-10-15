from __future__ import annotations

import logging
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
    price_offset_pct: Decimal = Decimal("0.5")
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
    non_profitable_wait_sec: int = 60
    fill_timeout_sec: int = 180


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
        self._base_stop_ts: Dict[str, float] = {}
        self._stop_cooldown_sec: int = 300

        try:
            prefix = "[Controller]"

            class _PrefixFilter(logging.Filter):
                def __init__(self, p: str):
                    super().__init__()
                    self._p = p

                def filter(self, record: logging.LogRecord) -> bool:
                    try:
                        msg = record.msg
                        if isinstance(msg, str):
                            if not msg.startswith(self._p):
                                record.msg = f"{self._p} {msg}"
                        else:
                            record.msg = f"{self._p} {msg}"
                    except Exception:
                        pass
                    return True

            instance_logger_name = f"{__name__}.{id(self)}"
            inst_logger = logging.getLogger(instance_logger_name)
            if not any(isinstance(flt, _PrefixFilter) for flt in getattr(inst_logger, 'filters', [])):
                inst_logger.addFilter(_PrefixFilter(prefix))
            self._prefixed_logger = inst_logger
            self.logger = lambda: self._prefixed_logger
        except Exception:
            self._prefixed_logger = logging.getLogger(__name__)
            self.logger = lambda: self._prefixed_logger

    async def update_processed_data(self):
        pair_metrics: Dict[tuple, Dict] = {}
        current_ts = self.market_data_provider.time()

        for pair_config in self.config.pairs:
            connectors = self._get_available_connectors_for_pair(pair_config)
            if len(connectors) < 2:
                continue

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
                        self.logger().info(
                            f"[Funding] base={pair_config.base} entry={entry_ex} rate_int={entry_rate_pct_for_funding_interval_hours_str} hedge={hedge_ex} rate_int={hedge_rate_pct_for_funding_interval_hours_str} diff={funding_diff_str} side={getattr(trade_side, 'name', '-')}"
                        )

        self.processed_data["pair_metrics"] = pair_metrics

    def _get_available_connectors_for_pair(self, pair_config: PairConfig) -> List[str]:
        connectors: List[str] = []
        for connector_name in self.config.connectors:
            quote = pair_config.per_exchange_quote.get(connector_name)
            if quote:
                connectors.append(connector_name)
        return connectors

    def _count_active_groups_for_base(self, base: str) -> int:
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

        now_ts_global = self.market_data_provider.time()

        for ei in self.executors_info:
            if ei.is_done and getattr(ei, "trading_pair", None):
                tp = ei.trading_pair
                if tp and "-" in tp:
                    base = tp.split("-", 1)[0]
                    self._base_stop_ts.setdefault(base, now_ts_global)

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

            stop_ts = self._base_stop_ts.get(pair_config.base)
            if stop_ts is not None and (now_ts_global - stop_ts) < self._stop_cooldown_sec:
                remain = int(self._stop_cooldown_sec - (now_ts_global - stop_ts))
                self.logger().info(f"[Skip] cooldown after stop base={pair_config.base} remain={remain}s")
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
                    self.logger().info(f"[Skip] funding diff unavailable {entry_tp} {entry_ex}->{hedge_ex}")
                    continue

                if funding_rate_diff_pct < self.config.signal.min_funding_rate_profitability_pct:
                    self.logger().info(
                        f"[Skip] below threshold {entry_tp} {entry_ex}->{hedge_ex} diff={funding_rate_diff_pct:.6f} threshold={self.config.signal.min_funding_rate_profitability_pct:.6f}"
                    )
                    continue

                active_for_pair = self.get_active_executors_for_trading_pair(entry_tp)
                if any(not ei.is_done for ei in active_for_pair):
                    self.logger().info(f"[Skip] already active {entry_tp} {entry_ex}->{hedge_ex}")
                    continue

                now_ts = self.market_data_provider.time()
                cooldown_key = (entry_ex, entry_tp)
                last_ts = self._last_entry_ts_by_pair.get(cooldown_key, 0)
                if now_ts - last_ts < self.config.execution.entry_cooldown_sec:
                    self.logger().info(f"[Skip] cooldown {entry_tp} {entry_ex}->{hedge_ex}")
                    continue

                self.logger().info(f"[Check] cooldown passed {entry_tp} {entry_ex}->{hedge_ex}")
                pair_cap_usd: Decimal = pair_config.total_notional_usd_per_pair if pair_config.total_notional_usd_per_pair > 0 else self.config.execution.total_notional_usd
                if active_alloc_usd + pair_cap_usd > self.config.execution.total_notional_usd:
                    self.logger().info(f"[Skip] allocation exceed {entry_tp} {entry_ex}->{hedge_ex}")
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
                    maker_price_offset_pct=self.config.execution.maker.price_offset_pct,
                    maker_ttl_sec=self.config.execution.maker.ttl_sec,
                    maker_refresh_on_stale=self.config.execution.maker.refresh_on_stale,
                    hedge_min_notional_usd=self.config.execution.hedge.min_hedge_notional_usd,
                    exit_funding_diff_pct_threshold=self.config.exit.fr_spread_below_pct,
                    exit_hold_below_sec=self.config.exit.hold_below_sec,
                    funding_profitability_interval_hours=self.config.signal.funding_profitability_interval_hours,
                    non_profitable_wait_sec=self.config.execution.non_profitable_wait_sec,
                    fill_timeout_sec=self.config.execution.fill_timeout_sec,
                )

                actions.append(CreateExecutorAction(executor_config=exec_cfg, controller_id=self.config.id))
                self.logger().info(
                    f"[Enter] {entry_tp} {entry_ex}->{hedge_ex} side={maker_side.name} diff={(f'{funding_rate_diff_pct:.6f}' if funding_rate_diff_pct is not None else 'n/a')} cap_usd={pair_cap_usd:.2f}"
                )

                self._last_entry_ts_by_pair[cooldown_key] = now_ts
                active_alloc_usd += pair_cap_usd
                break
        return actions

    def _normalized_funding_rate_in_seconds(self, funding_info, connector: str) -> Optional[Decimal]:
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
        }

        entry_funding = self.market_data_provider.get_funding_info(entry_connector, entry_trading_pair)
        if entry_funding is None:
            self.logger().info(f"[Funding] entry funding missing {entry_connector}:{entry_trading_pair}")
        hedge_funding = self.market_data_provider.get_funding_info(hedge_connector, hedge_trading_pair)
        if hedge_funding is None:
            self.logger().info(f"[Funding] hedge funding missing {hedge_connector}:{hedge_trading_pair}")

        if entry_funding is None or hedge_funding is None:
            return metrics

        entry_rate_sec = self._normalized_funding_rate_in_seconds(entry_funding, entry_connector)
        hedge_rate_sec = self._normalized_funding_rate_in_seconds(hedge_funding, hedge_connector)

        metrics["entry_rate_sec"] = entry_rate_sec
        metrics["hedge_rate_sec"] = hedge_rate_sec

        if entry_rate_sec is None or hedge_rate_sec is None:
            return metrics

        interval_hours = self.config.signal.funding_profitability_interval_hours or self.DEFAULT_FUNDING_PROFITABILITY_INTERVAL_HOURS
        diff_pct_signed = util_funding_diff_pct(entry_rate_sec, hedge_rate_sec, hours=int(interval_hours))
        funding_rate_diff_pct = abs(Decimal(str(diff_pct_signed))) if diff_pct_signed is not None else None

        metrics["funding_rate_diff_pct"] = funding_rate_diff_pct
        trade_side = TradeType.BUY if entry_rate_sec < hedge_rate_sec else TradeType.SELL
        metrics["trade_side"] = trade_side

        metrics["entry_minutes_to_funding"] = self._minutes_to_next_funding(entry_funding.next_funding_utc_timestamp, current_ts)
        metrics["hedge_minutes_to_funding"] = self._minutes_to_next_funding(hedge_funding.next_funding_utc_timestamp, current_ts)

        return metrics

    def on_stop(self):
        self.logger().info("[Stop] FundingRateArbController stopping. Current open positions:")
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

        def _to_int(value) -> int:
            if value is None:
                return 0
            return int(value)

        outputs: List[str] = []
        exec_rows: List[Dict[str, Any]] = []
        order_rows: List[Dict[str, Any]] = []

        for ei in self.executors_info:
            if not ei.is_active:
                continue
            cfg = getattr(ei, "config", None)
            info = getattr(ei, "custom_info", {}) or {}

            maker_market = getattr(cfg, "maker_market", None)
            hedge_market = getattr(cfg, "hedge_market", None)
            if not maker_market or not hedge_market:
                self.logger().warning(f"[Exec] missing market info id={ei.id}")
                continue

            entry_ex = maker_market.connector_name
            entry_tp = maker_market.trading_pair
            hedge_ex = hedge_market.connector_name
            hedge_tp = hedge_market.trading_pair

            maker_pos = _to_decimal(info.get("maker_position_base", "0"))
            maker_pos_quote = _to_decimal(info.get("maker_position_quote", "0"))
            hedge_pos = _to_decimal(info.get("hedge_position_base", "0"))
            net_pnl_quote = _to_decimal(info.get("net_pnl_quote", ei.net_pnl_quote))
            net_pnl_pct = _to_decimal(info.get("net_pnl_pct", ei.net_pnl_pct))
            funding_pnl_maker = _to_decimal(info.get("funding_pnl_quote_maker", info.get("funding_pnl_quote", 0)))
            funding_pnl_hedge = _to_decimal(info.get("funding_pnl_quote_hedge", 0))
            funding_pnl_net = _to_decimal(info.get("funding_pnl_quote_net", funding_pnl_maker + funding_pnl_hedge))

            oriented_diff = info.get("funding_oriented_diff_pct")

            min_to_funding_entry = info.get("minutes_to_funding_entry")
            min_to_funding_hedge = info.get("minutes_to_funding_hedge")

            exec_rows.append({
                "Entry": f"{entry_ex}:{entry_tp}",
                "Hedge": f"{hedge_ex}:{hedge_tp}",
                "Status": getattr(ei.status, "name", str(ei.status)),
                "Side": info.get("side", "-"),
                "Maker pos": _decimal_to_str(maker_pos),
                "Hedge pos": _decimal_to_str(hedge_pos),
                "Maker pos $": _decimal_to_str(maker_pos_quote),
                "Net pnl quote": _decimal_to_str(net_pnl_quote),
                "Net pnl %": _decimal_to_str(net_pnl_pct),
                "Funding pnl maker": _decimal_to_str(funding_pnl_maker),
                "Funding pnl hedge": _decimal_to_str(funding_pnl_hedge),
                "Funding pnl net": _decimal_to_str(funding_pnl_net),
                "Funding diff %": _decimal_to_str(oriented_diff) if oriented_diff is not None else "-",
                "Min entry fund": _to_int(min_to_funding_entry) if min_to_funding_entry is not None else "-",
                "Min hedge fund": _to_int(min_to_funding_hedge) if min_to_funding_hedge is not None else "-",
            })

            maker_orders = info.get("maker_open_orders", []) or []
            hedge_orders = info.get("hedge_open_orders", []) or []
            for order in maker_orders:
                order_rows.append({
                    "Connector": entry_ex,
                    "Trading Pair": entry_tp,
                    "Side": order.get("side"),
                    "Price": order.get("px"),
                    "Amount": order.get("amt"),
                    "Filled": order.get("exec_base"),
                    "State": order.get("state"),
                })
            for order in hedge_orders:
                order_rows.append({
                    "Connector": hedge_ex,
                    "Trading Pair": hedge_tp,
                    "Side": order.get("side"),
                    "Price": order.get("px"),
                    "Amount": order.get("amt"),
                    "Filled": order.get("exec_base"),
                    "State": order.get("state"),
                })

        if not exec_rows:
            return ["No active executors."]

        exec_df = pd.DataFrame(exec_rows)
        outputs.append("Active executors:\n" + format_df_for_printout(exec_df, table_format="psql", index=False))

        if order_rows:
            orders_df = pd.DataFrame(order_rows)
            outputs.append("Open orders:\n" + format_df_for_printout(orders_df, table_format="psql", index=False))

        return outputs

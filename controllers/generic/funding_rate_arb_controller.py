from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, Field, field_validator, model_validator

from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.core.data_type.common import MarketDict, PriceType, TradeType
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.maker_hedge_single_executor.data_types import MakerHedgeSingleExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


class LegConfig(BaseModel):
    role: str
    connector: str
    quote_asset: str
    side: str

    @field_validator("role", mode="before")
    @classmethod
    def _validate_role(cls, value: str) -> str:
        if not isinstance(value, str):
            raise ValueError("Each leg role must be either 'maker' or 'hedge'.")
        if value not in {"maker", "hedge"}:
            raise ValueError("Each leg role must be either 'maker' or 'hedge'.")
        return value

    @field_validator("connector", mode="before")
    @classmethod
    def _validate_connector(cls, value: str) -> str:
        if value is None:
            raise ValueError("Each leg must define a connector.")
        connector = str(value).strip()
        if connector == "":
            raise ValueError("Each leg must define a connector.")
        is_perpetual = "perpetual" in connector.lower()
        if not is_perpetual:
            raise ValueError(
                "Each leg connector must be a perpetual futures connector."
            )
        return connector

    @field_validator("quote_asset", mode="before")
    @classmethod
    def _validate_quote(cls, value: str) -> str:
        if value is None:
            raise ValueError("Each leg must define a quote_asset.")
        quote = str(value).strip()
        if quote == "":
            raise ValueError("Each leg must define a quote_asset.")
        return quote

    @field_validator("side", mode="before")
    @classmethod
    def _validate_side(cls, value: str) -> str:
        if not isinstance(value, str):
            raise ValueError("Each leg side must be either 'BUY' or 'SELL'.")
        if value not in {"BUY", "SELL"}:
            raise ValueError("Each leg side must be either 'BUY' or 'SELL'.")
        return value


class PairConfig(BaseModel):
    base_asset: str
    total_notional_usd: Decimal
    max_notional_per_part: Decimal
    min_notional_per_part: Decimal

    @field_validator("base_asset", mode="before")
    @classmethod
    def _validate_base(cls, value: str) -> str:
        if value is None:
            raise ValueError("pair.base_asset is required.")
        base = str(value).strip()
        if base == "":
            raise ValueError("pair.base_asset is required.")
        return base


class MakerConfig(BaseModel):
    price_offset_pct: Decimal
    ttl_sec: int


class HedgeConfig(BaseModel):
    min_hedge_notional_usd: Decimal


class ExecutionConfig(BaseModel):
    leverage: Decimal
    maker: MakerConfig
    hedge: HedgeConfig
    non_profitable_wait_sec: int
    fill_timeout_sec: int


class ExitConfig(BaseModel):
    funding_profitability_interval_hours: int
    fr_spread_below_pct: Optional[Decimal]
    hold_below_sec: int
    closing_non_profitable_wait_sec: int
    liquidation_limit_close_pct: Decimal
    liquidation_market_close_pct: Decimal


class FundingRateArbControllerConfig(ControllerConfigBase):
    controller_name: str = "funding_rate_arb_controller"
    controller_type: str = "generic"
    connectors: List[str] = Field(default_factory=list)
    legs: List[LegConfig] = Field(default_factory=list)
    pair: PairConfig
    execution: ExecutionConfig
    exit: ExitConfig

    @model_validator(mode="after")
    def _validate_legs_and_pair(self):
        legs: List[LegConfig] = getattr(self, "legs", [])
        pair: Optional[PairConfig] = getattr(self, "pair", None)

        if pair is None:
            raise ValueError(
                "A single 'pair' section must be provided in the controller config."
            )

        if len(legs) != 2:
            raise ValueError("Exactly two legs (maker and hedge) must be defined.")

        roles = [leg.role for leg in legs]
        if sorted(roles) != ["hedge", "maker"]:
            raise ValueError("Leg roles must include one 'maker' and one 'hedge'.")

        connectors = [leg.connector for leg in legs]
        if len(set(connectors)) != 2:
            raise ValueError("Leg connectors must be unique.")

        object.__setattr__(self, "connectors", connectors)
        return self

    @property
    def maker_leg(self) -> LegConfig:
        return next(leg for leg in self.legs if leg.role == "maker")

    @property
    def hedge_leg(self) -> LegConfig:
        return next(leg for leg in self.legs if leg.role == "hedge")

    def leg_trading_pair(self, leg: LegConfig) -> str:
        return f"{self.pair.base_asset}-{leg.quote_asset}"

    def update_markets(self, markets: MarketDict) -> MarketDict:
        maker_leg = self.maker_leg
        hedge_leg = self.hedge_leg
        markets.add_or_update(maker_leg.connector, self.leg_trading_pair(maker_leg))
        markets.add_or_update(hedge_leg.connector, self.leg_trading_pair(hedge_leg))
        return markets


class FundingRateArbController(ControllerBase):
    FUNDING_INTERVAL_FALLBACKS: Dict[str, int] = {
        "bybit_perpetual": 60 * 60 * 8,
        "hyperliquid_perpetual": 60 * 60 * 1,
        "okx_perpetual": 60 * 60 * 8,
    }

    def __init__(self, config: FundingRateArbControllerConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config: FundingRateArbControllerConfig
        self.firs_open_executed: bool = False

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
            if not any(
                isinstance(flt, _PrefixFilter)
                for flt in getattr(inst_logger, "filters", [])
            ):
                inst_logger.addFilter(_PrefixFilter(prefix))
            self._prefixed_logger = inst_logger
            self.logger = lambda: self._prefixed_logger
        except Exception:
            self._prefixed_logger = logging.getLogger(__name__)
            self.logger = lambda: self._prefixed_logger

    async def update_processed_data(self):
        pass

    def determine_executor_actions(self) -> List[ExecutorAction]:
        if self.firs_open_executed:
            return []

        actions: List[ExecutorAction] = []

        pair_config = self.config.pair
        maker_leg = self.config.maker_leg
        hedge_leg = self.config.hedge_leg
        maker_trading_pair = self.config.leg_trading_pair(maker_leg)
        hedge_trading_pair = self.config.leg_trading_pair(hedge_leg)
        maker_side = TradeType.BUY if maker_leg.side == "BUY" else TradeType.SELL
        pair_cap_usd = pair_config.total_notional_usd

        mid_price = self.market_data_provider.get_price_by_type(
            maker_leg.connector, maker_trading_pair, PriceType.MidPrice
        )
        if mid_price.is_nan() or mid_price <= 0:
            self.logger().info(
                f"[Skip] invalid mid price for {maker_trading_pair} on {maker_leg.connector}"
            )
            return actions

        exec_cfg = MakerHedgeSingleExecutorConfig(
            timestamp=self.market_data_provider.time(),
            maker_market=ConnectorPair(
                connector_name=maker_leg.connector, trading_pair=maker_trading_pair
            ),
            hedge_market=ConnectorPair(
                connector_name=hedge_leg.connector, trading_pair=hedge_trading_pair
            ),
            side_maker=maker_side.name,
            controller_id=self.config.id,
            leverage=self.config.execution.leverage,
            pair_notional_usd_cap=pair_cap_usd,
            per_order_max_notional_usd=pair_config.max_notional_per_part,
            per_order_min_notional_usd=pair_config.min_notional_per_part,
            maker_price_offset_pct=self.config.execution.maker.price_offset_pct,
            maker_ttl_sec=self.config.execution.maker.ttl_sec,
            hedge_min_notional_usd=self.config.execution.hedge.min_hedge_notional_usd,
            exit_funding_diff_pct_threshold=self.config.exit.fr_spread_below_pct,
            exit_hold_below_sec=self.config.exit.hold_below_sec,
            funding_profitability_interval_hours=self.config.exit.funding_profitability_interval_hours,
            non_profitable_wait_sec=self.config.execution.non_profitable_wait_sec,
            fill_timeout_sec=self.config.execution.fill_timeout_sec,
            closing_non_profitable_wait_sec=self.config.exit.closing_non_profitable_wait_sec,
            liquidation_limit_close_pct=self.config.exit.liquidation_limit_close_pct,
            liquidation_market_close_pct=self.config.exit.liquidation_market_close_pct,
        )

        actions.append(
            CreateExecutorAction(
                executor_config=exec_cfg,
                controller_id=self.config.id,
            )
        )
        self.logger().info(
            f"[Enter] {maker_trading_pair} {maker_leg.connector}->{hedge_leg.connector} "
            f"side={maker_side.name} "
        )

        self.firs_open_executed = True

        return actions

    def on_stop(self):
        self.logger().info(
            "[Stop] FundingRateArbController stopping. Current open positions:"
        )
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
        info_rows: List[Dict[str, Any]] = []

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
            maker_unrealized_pnl = _to_decimal(info.get("maker_unrealized_pnl"))
            hedge_unrealized_pnl = _to_decimal(info.get("hedge_unrealized_pnl"))
            funding_pnl_maker = _to_decimal(
                info.get("funding_pnl_quote_maker", info.get("funding_pnl_quote", 0))
            )
            funding_pnl_hedge = _to_decimal(info.get("funding_pnl_quote_hedge", 0))
            funding_pnl_net = _to_decimal(
                info.get("funding_pnl_quote_net", funding_pnl_maker + funding_pnl_hedge)
            )

            oriented_diff = info.get("funding_oriented_diff_pct")

            min_to_funding_entry = info.get("minutes_to_funding_entry")
            min_to_funding_hedge = info.get("minutes_to_funding_hedge")

            last_diff_pct_to_liquidation_maker = info.get(
                "last_diff_pct_to_liquidation_maker"
            )
            last_diff_pct_to_liquidation_hedge = info.get(
                "last_diff_pct_to_liquidation_hedge"
            )
            last_liquidation_price_maker = info.get("last_liquidation_price_maker")
            last_liquidation_price_hedge = info.get("last_liquidation_price_hedge")

            exec_rows.append(
                {
                    "Entry": f"{entry_ex}:{entry_tp}",
                    "Hedge": f"{hedge_ex}:{hedge_tp}",
                    "Status": getattr(ei.status, "name", str(ei.status)),
                    "Side": info.get("side", "-"),
                    "Maker pos": _decimal_to_str(maker_pos),
                    "Hedge pos": _decimal_to_str(hedge_pos),
                    "Maker pos $": _decimal_to_str(maker_pos_quote),
                    "Maker unrealized PnL": _decimal_to_str(maker_unrealized_pnl),
                    "Hedge unrealized PnL": _decimal_to_str(hedge_unrealized_pnl),
                    "Funding pnl maker": _decimal_to_str(funding_pnl_maker),
                    "Funding pnl hedge": _decimal_to_str(funding_pnl_hedge),
                    "Funding pnl net": _decimal_to_str(funding_pnl_net),
                    "Funding diff %": _decimal_to_str(oriented_diff)
                    if oriented_diff is not None
                    else "-",
                }
            )

            info_rows.append(
                {
                    "Entry": f"{entry_ex}:{entry_tp}",
                    "Hedge": f"{hedge_ex}:{hedge_tp}",
                    "Min entry fund": _to_int(min_to_funding_entry)
                    if min_to_funding_entry is not None
                    else "-",
                    "Min hedge fund": _to_int(min_to_funding_hedge)
                    if min_to_funding_hedge is not None
                    else "-",
                    "Diff to liq maker %": _decimal_to_str(
                        last_diff_pct_to_liquidation_maker
                    )
                    if last_diff_pct_to_liquidation_maker is not None
                    else "-",
                    "Diff to liq hedge %": _decimal_to_str(
                        last_diff_pct_to_liquidation_hedge
                    )
                    if last_diff_pct_to_liquidation_hedge is not None
                    else "-",
                    "Liq price maker": _decimal_to_str(last_liquidation_price_maker)
                    if last_liquidation_price_maker is not None
                    else "-",
                    "Liq price hedge": _decimal_to_str(last_liquidation_price_hedge)
                    if last_liquidation_price_hedge is not None
                    else "-",
                }
            )

            maker_orders = info.get("maker_open_orders", []) or []
            hedge_orders = info.get("hedge_open_orders", []) or []
            for order in maker_orders:
                order_rows.append(
                    {
                        "Connector": entry_ex,
                        "Trading Pair": entry_tp,
                        "Side": order.get("side"),
                        "Price": order.get("px"),
                        "Amount": order.get("amt"),
                        "Filled": order.get("exec_base"),
                        "State": order.get("state"),
                    }
                )
            for order in hedge_orders:
                order_rows.append(
                    {
                        "Connector": hedge_ex,
                        "Trading Pair": hedge_tp,
                        "Side": order.get("side"),
                        "Price": order.get("px"),
                        "Amount": order.get("amt"),
                        "Filled": order.get("exec_base"),
                        "State": order.get("state"),
                    }
                )

        if not exec_rows:
            return ["No active executors."]

        exec_df = pd.DataFrame(exec_rows)
        outputs.append(
            "Active executors:\n"
            + format_df_for_printout(exec_df, table_format="psql", index=False)
        )

        info_df = pd.DataFrame(info_rows)
        outputs.append(
            "Executor info summary:\n"
            + format_df_for_printout(info_df, table_format="psql", index=False)
        )

        if order_rows:
            orders_df = pd.DataFrame(order_rows)
            outputs.append(
                "Open orders:\n"
                + format_df_for_printout(orders_df, table_format="psql", index=False)
            )

        return outputs

    def custom_command(
        self, custom_command: str, params: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        self.logger().info(
            f"[Manual] Custom command triggered: custom_command={custom_command}, params={params}"
        )
        self.logger().info("hum controller 1")
        try:
            active_executor_ids = []
            self.logger().info(
                f"[Manual] Checking {len(self.executors_info)} executors info..."
            )
            for ei in self.executors_info:
                self.logger().info(
                    f"[Manual] Checking executor {ei.id}: active={ei.is_active}, done={ei.is_done}"
                )
                if ei.is_active and not ei.is_done:
                    active_executor_ids.append(ei.id)

            self.logger().info("hum controller 2")
            self.logger().info(
                f"[Manual] Identified {len(active_executor_ids)} active executors for custom command: {active_executor_ids}"
            )
            return active_executor_ids
        except Exception as e:
            self.logger().error(f"[Manual] Error in custom_command: {e}", exc_info=True)
            return []

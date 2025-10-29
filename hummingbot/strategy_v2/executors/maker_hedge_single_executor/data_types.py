from decimal import Decimal
from typing import Literal

from pydantic import Field, field_validator

from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class MakerHedgeSingleExecutorConfig(ExecutorConfigBase):
    type: Literal["maker_hedge_single_executor"] = "maker_hedge_single_executor"
    maker_market: ConnectorPair
    hedge_market: ConnectorPair
    side_maker: Literal["BUY", "SELL"] | str = Field(default="BUY")
    leverage: Decimal = Decimal("1")
    pair_notional_usd_cap: Decimal
    per_order_max_notional_usd: Decimal
    per_order_min_notional_usd: Decimal = Decimal("0")
    order_interval_sec: int = 8

    maker_price_offset_pct: Decimal = Decimal("0.5")
    maker_ttl_sec: int = 20
    maker_refresh_on_stale: bool = True

    hedge_min_notional_usd: Decimal = Decimal("200")

    exit_funding_diff_pct_threshold: Decimal | None = None
    exit_hold_below_sec: int = 60
    funding_profitability_interval_hours: int = 24

    non_profitable_wait_sec: int = 60
    fill_timeout_sec: int = 180

    @field_validator("controller_id", mode="before")
    @classmethod
    def _ensure_controller_id(cls, v):
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return "main"
        return v

    @property
    def trading_pair(self) -> str:
        return self.maker_market.trading_pair

    @property
    def connector_name(self) -> str:
        return self.maker_market.connector_name

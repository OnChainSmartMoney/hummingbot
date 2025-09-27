from decimal import Decimal
from typing import Literal

from pydantic import Field

from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class MakerHedgeSingleExecutorConfig(ExecutorConfigBase):
    type: Literal["maker_hedge_single_executor"] = "maker_hedge_single_executor"
    maker_market: ConnectorPair
    hedge_market: ConnectorPair
    side_maker: Literal["BUY", "SELL"] | str = Field(default="BUY")
    leverage: Decimal = Decimal("1")
    # Budgeting and pacing
    pair_notional_usd_cap: Decimal
    per_order_max_notional_usd: Decimal
    per_order_min_notional_usd: Decimal = Decimal("0")
    order_interval_sec: int = 8

    # maker leg
    maker_price_offset_bp: Decimal = Decimal("0.5")
    maker_ttl_sec: int = 20
    maker_refresh_on_stale: bool = True

    hedge_min_notional_usd: Decimal = Decimal("200")

    # exit conditions
    exit_funding_diff_pct_threshold: Decimal | None = None
    exit_hold_below_sec: int = 60
    funding_profitability_interval_hours: int = 24

    @property
    def trading_pair(self) -> str:
        """Primary trading pair for this executor (maker leg)."""
        return self.maker_market.trading_pair

    @property
    def connector_name(self) -> str:
        """Primary connector name for this executor (maker leg)."""
        return self.maker_market.connector_name

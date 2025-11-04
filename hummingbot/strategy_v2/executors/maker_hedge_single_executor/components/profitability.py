from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Optional

from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType

if TYPE_CHECKING:
    from hummingbot.strategy_v2.executors.maker_hedge_single_executor.maker_hedge_single_executor import (
        MakerHedgeSingleExecutor,
    )


class ProfitabilityHelper:
    def __init__(self, executor: 'MakerHedgeSingleExecutor'):
        self.exe = executor
        self.is_profitable_on_last_check: Optional[bool] = None
        self.last_profitable_ts: float = 0.0
        self.should_open_positions: bool = True
        self.profitability_should_be_negative: bool = False
        self.profitability_always_positive: bool = False

    def check_enter_condition(self, maker_side: TradeType, maker_price: Decimal, amount: Decimal) -> bool:
        if self.profitability_always_positive:
            return True

        if self.profitability_should_be_negative:
            return False

        mdp = getattr(self.exe._strategy, "market_data_provider", None)
        hedge_side = self.exe._get_hedge_side_for_mode()
        hedge_price = Decimal(mdp.get_price_for_quote_volume(
            connector_name=self.exe.hedge_connector,
            trading_pair=self.exe.hedge_pair,
            quote_volume=amount * maker_price,
            is_buy=hedge_side == TradeType.BUY,
        ).result_price)
        self.exe.logger().info(f"[Profitability] Entry prices: maker {maker_price:.8f} exchange {self.exe.maker_connector} side {maker_side} hedge {hedge_price:.8f} exchange {self.exe.hedge_connector} side {hedge_side} for amount {amount:.8f}")

        maker_estimated_fees = self.exe.connectors[self.exe.maker_connector].get_fee(
            base_currency=self.exe.maker_pair.split("-")[0],
            quote_currency=self.exe.maker_pair.split("-")[1],
            order_type=OrderType.LIMIT_MAKER,
            order_side=maker_side,
            amount=amount,
            price=maker_price,
            is_maker=True,
            position_action=PositionAction.OPEN
        ).percent * 2
        hedge_estimated_fees = self.exe.connectors[self.exe.hedge_connector].get_fee(
            base_currency=self.exe.hedge_pair.split("-")[0],
            quote_currency=self.exe.hedge_pair.split("-")[1],
            order_type=OrderType.MARKET,
            order_side=hedge_side,
            amount=amount,
            price=hedge_price,
            is_maker=False,
            position_action=PositionAction.OPEN
        ).percent * 2
        self.exe.logger().info(f"[Profitability] Estimated fees: maker {maker_estimated_fees:.6f} hedge {hedge_estimated_fees:.6f}")

        if maker_side == TradeType.BUY:
            estimated_trade_pnl_pct = (hedge_price - maker_price) / maker_price
        else:
            estimated_trade_pnl_pct = (maker_price - hedge_price) / maker_price

        self.exe.logger().info(f"[Profitability] Estimated trade PnL%: {estimated_trade_pnl_pct:.6f}")

        total_estimated_fees = maker_estimated_fees + hedge_estimated_fees
        net_estimated_pnl_pct = estimated_trade_pnl_pct - total_estimated_fees

        if net_estimated_pnl_pct > Decimal("0"):
            self.exe.logger().info(f"[Profitability] Net estimated PnL% after fees: {net_estimated_pnl_pct:.6f} (fees total {total_estimated_fees:.6f}) - PROFITABLE")
            return True

        oriented_funding_diff_pct = self.exe._funding_helper.get_oriented_funding_diff_pct(funding_interval_hours=1)
        if oriented_funding_diff_pct is None:
            self.exe.logger().warning("[Profitability] Funding rates unavailable - skipping profitability-based entry condition")
            return False

        if (oriented_funding_diff_pct + net_estimated_pnl_pct) > Decimal("0"):
            self.exe.logger().info(f"[Profitability] Net after fees {net_estimated_pnl_pct:.6f} + funding {oriented_funding_diff_pct:.6f} = {(net_estimated_pnl_pct + oriented_funding_diff_pct):.6f} - PROFITABLE with funding")
            return True
        else:
            self.exe.logger().info(f"[Profitability] Net after fees {net_estimated_pnl_pct:.6f} + funding {oriented_funding_diff_pct:.6f} = {(net_estimated_pnl_pct + oriented_funding_diff_pct):.6f} - NOT PROFITABLE")
            return False

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.strategy_v2.models.executors import CloseType

if TYPE_CHECKING:
    from hummingbot.strategy_v2.executors.maker_hedge_single_executor.maker_hedge_single_executor import (
        MakerHedgeSingleExecutor,
    )


class LiquidationHelper:
    def __init__(self, executor: "MakerHedgeSingleExecutor"):
        self.exe = executor

    def monitor(self, now_ts: float):
        exe = self.exe
        limit_trig = Decimal(str(getattr(exe.config, "liquidation_limit_close_pct", Decimal("5"))))
        mkt_trig = Decimal(str(getattr(exe.config, "liquidation_market_close_pct", Decimal("1"))))
        if limit_trig <= 0 and mkt_trig <= 0:
            return

        def side_is_long(is_maker: bool) -> bool:
            side = exe.side_maker if is_maker else exe.side_hedge
            return side == TradeType.BUY

        def compute_liq(is_maker: bool) -> Optional[Dict[str, Decimal]]:
            pos_base = exe.get_full_position_base_amount(is_maker=is_maker)
            if pos_base <= 0:
                return None
            avg_entry = exe._exposure_helper.get_avg_entry_price(is_maker=is_maker)
            if avg_entry is None or avg_entry <= 0:
                return None
            long = side_is_long(is_maker)

            liq_px, upnl = self._get_connector_liq_and_pnl(is_maker=is_maker)
            if upnl is not None:
                if is_maker:
                    exe._maker_unrealized_pnl = upnl
                else:
                    exe._hedge_unrealized_pnl = upnl

            if liq_px is None:
                try:
                    lev = Decimal(str(getattr(exe.config, "leverage", Decimal("1"))))
                except Exception:
                    lev = Decimal("1")
                if lev is None or lev <= 0:
                    lev = Decimal("1")
                safety_pct = Decimal("0.10")
                effective_mult = Decimal("1") - safety_pct
                adverse_move = (Decimal("1") / lev) * effective_mult
                try:
                    if long:
                        liq_px = avg_entry * (Decimal("1") - adverse_move)
                    else:
                        liq_px = avg_entry * (Decimal("1") + adverse_move)
                    exe.logger().warning(
                        f"[Liq] Missing connector liq price for {'maker' if is_maker else 'hedge'}; using fallback with 10% safety: liq={liq_px:.8f} (entry={avg_entry:.8f}, lev={lev}, move={(adverse_move * Decimal('100')):.4f}%)"
                    )
                except Exception:
                    exe.logger().error(
                        f"[Liq] Could not compute fallback liquidation price for {'maker' if is_maker else 'hedge'}; skipping risk calc for this leg."
                    )
                    return None

            mid = exe.get_price(
                exe.maker_connector if is_maker else exe.hedge_connector,
                exe.maker_pair if is_maker else exe.hedge_pair,
                PriceType.MidPrice,
            )
            if mid.is_nan() or mid <= 0:
                return None
            cur_px = mid
            if long:
                dist = (cur_px - liq_px) / cur_px * Decimal("100")
            else:
                dist = (liq_px - cur_px) / cur_px * Decimal("100")
            if dist < 0:
                dist = Decimal("0")
            if is_maker:
                exe._last_liquidation_price_maker = liq_px
                exe._last_diff_pct_to_liquidation_maker = dist
            else:
                exe._last_liquidation_price_hedge = liq_px
                exe._last_diff_pct_to_liquidation_hedge = dist
            return {"entry": avg_entry, "liq": liq_px, "cur": cur_px, "dist_pct": dist}

        mk = compute_liq(True)
        hg = compute_liq(False)

        def log_liq(label: str, info: Optional[Dict[str, Decimal]]):
            if info is None:
                return
            exe.logger().info(
                f"[Liq] {label}: entry={info['entry']:.8f} liq={info['liq']:.8f} cur={info['cur']:.8f} dist={info['dist_pct']:.4f}% (limit={limit_trig}%, market={mkt_trig}%)"
            )

        log_liq("Maker", mk)
        log_liq("Hedge", hg)

        critical_dist = None
        if mk is not None:
            critical_dist = mk["dist_pct"] if critical_dist is None else min(critical_dist, mk["dist_pct"])
        if hg is not None:
            critical_dist = hg["dist_pct"] if critical_dist is None else min(critical_dist, hg["dist_pct"])
        if critical_dist is None:
            return

        if mkt_trig > 0 and critical_dist <= mkt_trig:
            exe.logger().info(
                f"[Liq] Distance {critical_dist:.4f}% <= market trigger {mkt_trig}% -> closing by MARKET now"
            )
            exe.close_all_positions_by_market()
            exe.close_type = CloseType.EARLY_STOP
            return

        if not exe._closing and limit_trig > 0 and critical_dist <= limit_trig:
            exe.logger().info(
                f"[Liq] Distance {critical_dist:.4f}% <= limit trigger {limit_trig}% -> starting CLOSE via LIMIT (no wait)"
            )
            exe._closing = True
            exe._closing_no_wait = True
            exe._build_close_queue_if_needed()

    def _get_connector_liq_and_pnl(self, is_maker: bool) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        exe = self.exe
        try:
            conn_name = exe.maker_connector if is_maker else exe.hedge_connector
            pair = exe.maker_pair if is_maker else exe.hedge_pair
            conn = self._get_connector_obj(conn_name)
            if conn is None or not pair:
                return None, None

            pos_obj = None
            attr = "account_positions"
            if hasattr(conn, attr):
                data = getattr(conn, attr)
                exe.logger().info(f"[Liq] Trying connector attribute '{data}' for positions")
                if isinstance(data, dict):
                    for it in data.values():
                        sym = None
                        try:
                            sym = getattr(it, "trading_pair", None) or getattr(it, "symbol", None)
                        except Exception:
                            pass
                        if sym is None and isinstance(it, dict):
                            sym = it.get("trading_pair") or it.get("symbol")
                        if sym == pair:
                            pos_obj = it
                            break
                elif isinstance(data, (list, tuple)):
                    for it in data:
                        sym = None
                        try:
                            sym = getattr(it, "trading_pair", None) or getattr(it, "symbol", None)
                        except Exception:
                            pass
                        if sym is None and isinstance(it, dict):
                            sym = it.get("trading_pair") or it.get("symbol")
                        if sym == pair:
                            pos_obj = it
                            break

            if pos_obj is None:
                return None, None

            liq_px: Optional[Decimal] = None
            liquid_param = "liquidation_price"
            try:
                v = None
                if isinstance(pos_obj, dict):
                    v = pos_obj.get(liquid_param)
                else:
                    v = getattr(pos_obj, liquid_param, None)
                if v is not None:
                    v_str = str(v)
                    if v_str not in ("", "0", "None", "nan", "NaN"):
                        liq_px = Decimal(v_str)
            except Exception:
                pass

            upnl: Optional[Decimal] = None
            upnl_param = "unrealized_pnl"
            try:
                v = None
                if isinstance(pos_obj, dict):
                    v = pos_obj.get(upnl_param)
                else:
                    v = getattr(pos_obj, upnl_param, None)
                if v is not None:
                    v_str = str(v)
                    if v_str not in ("", "None", "nan", "NaN"):
                        upnl = Decimal(v_str)
            except Exception:
                pass

            return liq_px, upnl
        except Exception:
            return None, None

    def _get_connector_obj(self, connector_name: str):
        try:
            return getattr(self.exe._strategy, "connectors", {}).get(connector_name)
        except Exception:
            return None

# engine/risk_manager.py
# Mirrors: calcPositionSize, calcSLTP, all SL/TP ATR logic from GAS

from __future__ import annotations
from typing import Dict, Optional
from config import CONFIG
from engine.indicator_engine import IndicatorEngine

IE = IndicatorEngine()

def calc_position_size(balance: float, price: float, atr: float,
                       risk_pct: float = None) -> float:
    """Risk-based position size in USDT. Mirrors calcPositionSize() in GAS."""
    rp = risk_pct or CONFIG.RISK_PERCENT / 100
    risk_usdt = balance * rp
    risk_per_unit = atr * CONFIG.ATR_MULT_SL
    if risk_per_unit <= 0 or price <= 0: return 0.0
    units = risk_usdt / risk_per_unit
    size_usdt = units * price
    max_size = balance * CONFIG.MAX_SIZE_PCT
    return round(min(size_usdt, max_size), 2)

def calc_sltp(price: float, atr: float, direction: str,
              trade_type: str = "SCALP") -> Dict:
    """
    Compute SL + 3 TPs. Mirrors calcSLTP() in GAS.
    Swing trades get wider multipliers.
    """
    is_long = direction == "LONG"
    is_swing = "Swing" in trade_type

    sl_mult  = CONFIG.SWING_ATR_MULT_SL  if is_swing else CONFIG.ATR_MULT_SL
    tp1_mult = CONFIG.SWING_ATR_MULT_TP1 if is_swing else CONFIG.ATR_MULT_TP1
    tp2_mult = CONFIG.SWING_ATR_MULT_TP2 if is_swing else CONFIG.ATR_MULT_TP2
    tp3_mult = CONFIG.SWING_ATR_MULT_TP3 if is_swing else CONFIG.ATR_MULT_TP3

    if is_long:
        sl  = price - atr * sl_mult
        tp1 = price + atr * tp1_mult
        tp2 = price + atr * tp2_mult
        tp3 = price + atr * tp3_mult
    else:
        sl  = price + atr * sl_mult
        tp1 = price - atr * tp1_mult
        tp2 = price - atr * tp2_mult
        tp3 = price - atr * tp3_mult

    risk   = abs(price - sl)
    reward = abs(tp2 - price)
    rr     = round(reward / risk, 2) if risk > 0 else 0.0
    return {
        "sl": round(sl, 6), "tp1": round(tp1, 6),
        "tp2": round(tp2, 6), "tp3": round(tp3, 6), "rr": rr,
    }

def get_trade_type(c5m, c1h, c4h, direction: str, regime: str) -> str:
    """Mirrors getTradeType() in GAS."""
    if direction == "SHORT" or CONFIG.SHORT_SCALP_ONLY:
        return "Scalp 5m"
    ie = IndicatorEngine()
    trend4h = ie.get_trend(c4h) if c4h and len(c4h) >= 55 else "MIXED"
    trend1h = ie.get_trend_1h(c1h) if c1h and len(c1h) >= 30 else "MIXED"
    regime_ok = regime in ("TRENDING", "MIXED", "UNKNOWN")
    if trend4h == "BULL" and trend1h == "BULL" and regime_ok: return "Swing 4H+1H"
    if trend4h == "BULL" and regime == "TRENDING":             return "Swing 4H+1H"
    if trend4h == "MIXED" and trend1h == "BULL" and regime == "TRENDING": return "Swing 1H"
    return "Scalp 5m"

def atr_pct_check(price: float, atr: float) -> bool:
    """Block if ATR% > MAX_ATR_PCT (avoids hyper-volatile entries)."""
    if price <= 0: return False
    return (atr / price) <= CONFIG.MAX_ATR_PCT

def check_price_drift(entry: float, current: float) -> bool:
    """Blocks if price drifted too far from computed entry."""
    if entry <= 0: return False
    return abs(current - entry) / entry <= CONFIG.PRICE_DRIFT_MAX

def portfolio_risk_check(open_trades: list, symbol: str, direction: str,
                         balance: float = 0) -> Dict:
    """Mirrors portfolioRiskCheck() in GAS."""
    total = len(open_trades)
    if total >= CONFIG.MAX_SIGNALS_PER_RUN:
        return {"pass": False, "reason": f"Max trades {total}/{CONFIG.MAX_SIGNALS_PER_RUN}"}
    already_open = any(
        t.get("symbol") == symbol and t.get("direction") == direction
        for t in open_trades
    )
    if already_open:
        return {"pass": False, "reason": f"Already open: {symbol} {direction}"}
    if total == 0:
        return {"pass": True, "reason": ""}
    longs  = sum(1 for t in open_trades if t.get("direction") == "LONG")
    shorts = total - longs
    next_dir_count = (longs + 1) if direction == "LONG" else (shorts + 1)
    next_dir_pct   = next_dir_count / (total + 1)
    if next_dir_pct > 0.70:
        return {"pass": False, "reason": f"Direction exposure {next_dir_pct:.0%} > 70%"}
    return {"pass": True, "reason": ""}

def get_obi_signal(book_imbalance: Optional[Dict], direction: str) -> Dict:
    """Order Book Imbalance boost. Mirrors getOBISignal() in GAS."""
    if not book_imbalance:
        return {"pass": True, "boost": 0, "reason": ""}
    ratio = book_imbalance.get("ratio", 0.5)
    boost = 0.0
    reason = ""
    if direction == "LONG":
        if ratio > 0.68:   boost = 0.5;  reason = "OBI strong bull"
        elif ratio > 0.60: boost = 0.2
        elif ratio < 0.32: boost = -0.2
    else:
        if ratio < 0.32:   boost = 0.5;  reason = "OBI strong bear"
        elif ratio < 0.40: boost = 0.2
        elif ratio > 0.68: boost = -0.2
    return {"pass": True, "boost": boost, "ratio": ratio, "reason": reason}


"""
engine/session_filter.py — Ramos 360 Ai 🎖️
Session Filter + ATR Dynamic SL
فلتر الجلسات: لندن + نيويورك فقط للـ Scalp
ATR Dynamic SL: يحسب Stop Loss بناءً على تذبذب السوق الفعلي
"""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional, Tuple
import math
from loguru import logger


# ═══════════════════════════════════════════════════════════════
# SESSION WINDOWS (UTC)
# ═══════════════════════════════════════════════════════════════

SESSIONS = {
    "LONDON":     (7,  12),   # 07:00–12:00 UTC (أفضل: 08:00–11:00)
    "NEW_YORK":   (13, 17),   # 13:00–17:00 UTC (أفضل: 13:00–16:00)
    "OVERLAP":    (13, 16),   # تقاطع لندن + نيويورك (الأقوى)
    "ASIA":       (0,  7),    # ممنوع للـ Scalp
    "DEAD_ZONE":  (17, 23),   # ممنوع للـ Scalp
}

# جلسات مسموح بها لكل نوع تداول
SESSION_RULES = {
    "Scalp":      ["LONDON", "NEW_YORK", "OVERLAP"],
    "QuickScalp": ["LONDON", "OVERLAP"],            # أكثر صرامة
    "Swing":      ["LONDON", "NEW_YORK", "ASIA"],   # Swing مرن أكثر
    "SuperSwing": None,                             # بلا قيود زمنية
}


def get_current_session(dt: Optional[datetime] = None) -> str:
    """Returns the current trading session name."""
    if dt is None:
        dt = datetime.now(timezone.utc)
    h = dt.hour

    if SESSIONS["OVERLAP"][0] <= h < SESSIONS["OVERLAP"][1]:
        return "OVERLAP"
    if SESSIONS["LONDON"][0] <= h < SESSIONS["LONDON"][1]:
        return "LONDON"
    if SESSIONS["NEW_YORK"][0] <= h < SESSIONS["NEW_YORK"][1]:
        return "NEW_YORK"
    if SESSIONS["ASIA"][0] <= h < SESSIONS["ASIA"][1]:
        return "ASIA"
    return "DEAD_ZONE"


def is_session_allowed(trade_type: str,
                        dt: Optional[datetime] = None) -> Tuple[bool, str]:
    """
    Returns (allowed: bool, reason: str).
    Scalp only during London / NY / Overlap.
    Swing always allowed.
    """
    if dt is None:
        dt = datetime.now(timezone.utc)

    allowed_sessions = SESSION_RULES.get(trade_type)
    if allowed_sessions is None:
        return True, "No session restriction for this trade type"

    session = get_current_session(dt)
    if session in allowed_sessions:
        return True, f"Session OK: {session}"

    return False, (
        f"Session blocked: {session} — {trade_type} only allowed during "
        f"{', '.join(allowed_sessions)}"
    )


def session_quality_score(dt: Optional[datetime] = None) -> float:
    """
    Returns a quality multiplier (0.5–1.5) for the current session.
    Overlap = 1.5 (highest quality)
    London  = 1.2
    NY      = 1.1
    Asia    = 0.5 (poor for crypto scalp)
    Dead    = 0.3
    """
    session = get_current_session(dt)
    return {
        "OVERLAP":   1.5,
        "LONDON":    1.2,
        "NEW_YORK":  1.1,
        "ASIA":      0.5,
        "DEAD_ZONE": 0.3,
    }.get(session, 1.0)


# ═══════════════════════════════════════════════════════════════
# ATR DYNAMIC SL + TP
# ═══════════════════════════════════════════════════════════════

def _calc_atr(candles: list, period: int = 14) -> float:
    """Calculate ATR from raw candle list (newest-first format)."""
    if not candles or len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(min(period + 1, len(candles) - 1)):
        h   = float(candles[i][2])
        l   = float(candles[i][3])
        pc  = float(candles[i + 1][4])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else 0.0


def calc_dynamic_sl_tp(
    price:     float,
    direction: str,
    candles_4h: list,
    trade_type: str = "Scalp",
) -> dict:
    """
    Calculate dynamic SL and TP levels using ATR.

    Scalp:     SL = ATR × 1.2,  TP1 = ATR × 1.0, TP2 = ATR × 2.0, TP3 = ATR × 3.0
    Swing:     SL = ATR × 2.0,  TP1 = ATR × 2.5, TP2 = ATR × 5.0, TP3 = ATR × 8.0
    SuperSwing:SL = ATR × 2.5,  TP1 = ATR × 4.0, TP2 = ATR × 8.0, TP3 = ATR × 12.0
    """
    atr = _calc_atr(candles_4h, 14)

    # Fallback: use % of price if ATR unavailable
    if atr <= 0:
        atr = price * 0.015  # 1.5% default

    # Multipliers per trade type
    mults = {
        "Scalp":      {"sl": 1.2, "tp1": 1.0, "tp2": 2.0, "tp3": 3.0},
        "QuickScalp": {"sl": 0.8, "tp1": 0.7, "tp2": 1.4, "tp3": 2.0},
        "Swing":      {"sl": 2.0, "tp1": 2.5, "tp2": 5.0, "tp3": 8.0},
        "SuperSwing": {"sl": 2.5, "tp1": 4.0, "tp2": 8.0, "tp3": 12.0},
    }
    m = mults.get(trade_type, mults["Scalp"])

    if direction == "LONG":
        sl  = round(price - atr * m["sl"],  4)
        tp1 = round(price + atr * m["tp1"], 4)
        tp2 = round(price + atr * m["tp2"], 4)
        tp3 = round(price + atr * m["tp3"], 4)
    else:
        sl  = round(price + atr * m["sl"],  4)
        tp1 = round(price - atr * m["tp1"], 4)
        tp2 = round(price - atr * m["tp2"], 4)
        tp3 = round(price - atr * m["tp3"], 4)

    rr = round(abs(tp1 - price) / max(abs(sl - price), 1e-10), 2)

    logger.debug(
        f"[ATR SL] {direction} price={price:.4f} atr={atr:.4f} "
        f"sl={sl:.4f} tp1={tp1:.4f} rr={rr:.2f}"
    )

    return {
        "sl":  sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "atr": round(atr, 6),
        "rr":  rr,
    }


def validate_levels(price: float, direction: str,
                     sl: float, tp1: float,
                     min_rr: float = 0.8) -> Tuple[bool, str]:
    """Validate that SL/TP levels make sense."""
    if direction == "LONG":
        if sl >= price:
            return False, f"SL {sl} >= price {price}"
        if tp1 <= price:
            return False, f"TP1 {tp1} <= price {price}"
    else:
        if sl <= price:
            return False, f"SL {sl} <= price {price}"
        if tp1 >= price:
            return False, f"TP1 {tp1} >= price {price}"

    rr = abs(tp1 - price) / max(abs(sl - price), 1e-10)
    if rr < min_rr:
        return False, f"RR {rr:.2f} < min {min_rr}"

    return True, "OK"

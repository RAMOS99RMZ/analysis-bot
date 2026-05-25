# engine/market_state.py — Global Market State (shared between all experts)
# يخزن نتائج E10 و E11 ليقرأها جميع الخبراء
from __future__ import annotations
import time
from typing import Dict, Optional

_STATE: Dict = {
    "e10": None,          # نتيجة E10 لـ BTC + ETH
    "e11": None,          # نتيجة E11 لـ USDT.D + BTC.D
    "gemini": None,       # رأي Gemini اليومي
    "updated_at": 0,      # timestamp آخر تحديث
    "bias": "NEUTRAL",    # الاتجاه العام: LONG / SHORT / NEUTRAL
    "trade_type": "SCALP+SWING",
}

_GEMINI_UPDATED: float = 0   # timestamp آخر استدعاء Gemini


def update_e10(result: Dict) -> None:
    _STATE["e10"] = result
    _STATE["updated_at"] = time.time()
    _recalc_bias()


def update_e11(result: Dict) -> None:
    _STATE["e11"] = result
    _STATE["updated_at"] = time.time()
    _recalc_bias()


def update_gemini(opinion: str) -> None:
    global _GEMINI_UPDATED
    _STATE["gemini"] = opinion
    _GEMINI_UPDATED = time.time()


def get_state() -> Dict:
    return _STATE.copy()


def get_bias() -> str:
    return _STATE.get("bias", "NEUTRAL")


def get_trade_type() -> str:
    return _STATE.get("trade_type", "SCALP+SWING")


def is_fresh(max_age_seconds: int = 14400) -> bool:
    """النتيجة طازجة إذا تحدثت خلال آخر 4 ساعات (4H candle)."""
    return (time.time() - _STATE["updated_at"]) < max_age_seconds


def gemini_needs_update(interval_seconds: int = 86400) -> bool:
    """Gemini يُستدعى مرة يومياً فقط."""
    return (time.time() - _GEMINI_UPDATED) > interval_seconds


def _recalc_bias() -> None:
    """يحسب الاتجاه العام من E10 + E11."""
    e10 = _STATE.get("e10") or {}
    e11 = _STATE.get("e11") or {}

    e10_long  = float(e10.get("long",  0))
    e10_short = float(e10.get("short", 0))
    e11_long  = float(e11.get("long",  0))
    e11_short = float(e11.get("short", 0))

    # وزن E10 أعلى (60%) لأنه يحلل الشارت مباشرة
    combined_long  = e10_long  * 0.60 + e11_long  * 0.40
    combined_short = e10_short * 0.60 + e11_short * 0.40

    if combined_long > combined_short + 0.10:
        _STATE["bias"] = "LONG"
    elif combined_short > combined_long + 0.10:
        _STATE["bias"] = "SHORT"
    else:
        _STATE["bias"] = "NEUTRAL"

    # trade_type من E10
    _STATE["trade_type"] = e10.get("trade_type", "SCALP+SWING")

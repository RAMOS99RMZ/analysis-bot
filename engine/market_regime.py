# engine/market_regime.py — 6 أنظمة سوقية دقيقة
from __future__ import annotations
from typing import Dict, List
from engine.indicator_engine import IndicatorEngine

IE = IndicatorEngine()

REGIME_CONFIG = {
    "STRONG_BULL":  {"scalp_long": True,  "scalp_short": False, "swing_long": True,  "swing_short": False, "size_mult": 1.2},
    "WEAK_BULL":    {"scalp_long": True,  "scalp_short": False, "swing_long": True,  "swing_short": False, "size_mult": 0.8},
    "RANGING":      {"scalp_long": True,  "scalp_short": True,  "swing_long": False, "swing_short": False, "size_mult": 0.6},
    "WEAK_BEAR":    {"scalp_long": False, "scalp_short": True,  "swing_long": False, "swing_short": True,  "size_mult": 0.8},
    "STRONG_BEAR":  {"scalp_long": False, "scalp_short": True,  "swing_long": False, "swing_short": True,  "size_mult": 1.2},
    "CHAOS":        {"scalp_long": False, "scalp_short": False, "swing_long": False, "swing_short": False, "size_mult": 0.0},
}

REGIME_AR = {
    "STRONG_BULL": "🟢 صعود قوي",
    "WEAK_BULL":   "🔵 صعود ضعيف",
    "RANGING":     "🟡 سوق جانبي",
    "WEAK_BEAR":   "🟠 هبوط ضعيف",
    "STRONG_BEAR": "🔴 هبوط قوي",
    "CHAOS":       "⚫ تذبذب متطرف",
}


def detect_regime(c4h: List, c1h: List, c1d: List = None) -> Dict:
    """
    يُحدد النظام السوقي من بين 6 أنظمة بدقة.
    يستخدم: ATR% + EMA alignment + RSI + Trend
    """
    try:
        if not c4h or len(c4h) < 20:
            return _build(regime="RANGING", reason="بيانات غير كافية")

        price   = float(c4h[0][4])
        atr_val = IE.atr(c4h, 14)
        atr_pct = (atr_val / price * 100) if price > 0 else 2.0
        rsi4h   = IE.rsi(c4h, 14)
        trend4h = IE.get_trend(c4h)
        trend1h = IE.get_trend_1h(c1h) if c1h and len(c1h) >= 20 else "MIXED"

        ema20  = IE.ema(c4h, 20)
        ema50  = IE.ema(c4h, 50)
        ema200 = IE.ema(c4h, 200) if len(c4h) >= 200 else ema50

        bull_align = price > ema20 > ema50 > ema200
        bear_align = price < ema20 < ema50 < ema200

        # ── CHAOS: تذبذب مرتفع جداً ──────────────────────────────
        if atr_pct > 7.0:
            return _build("CHAOS", f"ATR={atr_pct:.1f}% مرتفع جداً")

        # ── STRONG BULL ───────────────────────────────────────────
        if (bull_align and trend4h == "BULL" and trend1h == "BULL"
                and rsi4h > 55 and atr_pct > 1.0):
            return _build("STRONG_BULL",
                          f"EMA مصطفة↑ | RSI={rsi4h:.0f} | ATR={atr_pct:.1f}%")

        # ── STRONG BEAR ───────────────────────────────────────────
        if (bear_align and trend4h == "BEAR" and trend1h == "BEAR"
                and rsi4h < 45 and atr_pct > 1.0):
            return _build("STRONG_BEAR",
                          f"EMA مصطفة↓ | RSI={rsi4h:.0f} | ATR={atr_pct:.1f}%")

        # ── WEAK BULL ─────────────────────────────────────────────
        if trend4h == "BULL" and (bull_align or trend1h == "BULL"):
            return _build("WEAK_BULL",
                          f"4H صاعد | 1H={trend1h} | RSI={rsi4h:.0f}")

        # ── WEAK BEAR ─────────────────────────────────────────────
        if trend4h == "BEAR" and (bear_align or trend1h == "BEAR"):
            return _build("WEAK_BEAR",
                          f"4H هابط | 1H={trend1h} | RSI={rsi4h:.0f}")

        # ── RANGING ───────────────────────────────────────────────
        return _build("RANGING",
                      f"لا اتجاه واضح | RSI={rsi4h:.0f} | ATR={atr_pct:.1f}%")

    except Exception as e:
        return _build("RANGING", f"خطأ: {e}")


def _build(regime: str, reason: str) -> Dict:
    cfg = REGIME_CONFIG.get(regime, REGIME_CONFIG["RANGING"])
    return {
        "regime":       regime,
        "regime_ar":    REGIME_AR.get(regime, regime),
        "reason":       reason,
        "scalp_long":   cfg["scalp_long"],
        "scalp_short":  cfg["scalp_short"],
        "swing_long":   cfg["swing_long"],
        "swing_short":  cfg["swing_short"],
        "size_mult":    cfg["size_mult"],
        "tradeable":    cfg["size_mult"] > 0,
    }


def is_direction_allowed(regime_data: Dict, direction: str, is_scalp: bool) -> bool:
    """يتحقق هل الاتجاه مسموح في هذا النظام."""
    if not regime_data.get("tradeable", True):
        return False
    if is_scalp:
        key = "scalp_long" if direction == "LONG" else "scalp_short"
    else:
        key = "swing_long" if direction == "LONG" else "swing_short"
    return regime_data.get(key, True)

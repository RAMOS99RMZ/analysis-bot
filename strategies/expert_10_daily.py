# strategies/expert_11_usdt.py — E11: USDT Dominance Analyst
# ═══════════════════════════════════════════════════════════════════
# وظيفة E11: تحليل شارت USDT.D فقط
#   → USDT.D صاعد = أموال تهرب من كريبتو → SHORT
#   → USDT.D هابط = أموال تدخل كريبتو   → LONG
#
# يتتبع تاريخ USDT.D في الذاكرة لحساب الاتجاه الحقيقي
# ═══════════════════════════════════════════════════════════════════
from __future__ import annotations
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

from engine.indicator_engine import IndicatorEngine

IE = IndicatorEngine()

# ── ذاكرة تاريخية لـ USDT.D (تُملأ مع كل استدعاء) ──────────────────
_USDT_D_HISTORY: Deque[float] = deque(maxlen=12)  # آخر 12 قراءة


# ─────────────────────────────────────────────────────────────────
# تحليل اتجاه USDT.D من السجل التاريخي
# ─────────────────────────────────────────────────────────────────
def _analyze_usdt_trend(history: Deque[float]) -> Tuple[str, float, str]:
    """
    يحلل السجل التاريخي لـ USDT.D ويُرجع:
    (trend: RISING/FALLING/FLAT, slope: معدل التغيير, label: نص)
    """
    if len(history) < 3:
        return "UNKNOWN", 0.0, "بيانات غير كافية"

    vals = list(history)   # الأحدث في النهاية (deque order)

    # حساب الانحدار الخطي البسيط (slope)
    n    = len(vals)
    xs   = list(range(n))
    xm   = sum(xs) / n
    ym   = sum(vals) / n
    num  = sum((xs[i] - xm) * (vals[i] - ym) for i in range(n))
    den  = sum((xs[i] - xm) ** 2 for i in range(n))
    slope = num / den if den != 0 else 0.0

    # مقارنة آخر 3 قراءات مع أول 3 قراءات
    recent_avg = sum(vals[-3:]) / 3
    old_avg    = sum(vals[:3])  / 3
    change_pct = ((recent_avg - old_avg) / old_avg * 100) if old_avg > 0 else 0.0

    if slope > 0.02 or change_pct > 0.3:
        trend = "RISING"
        label = f"USDT.D في صعود ↑ (slope={slope:.3f})"
    elif slope < -0.02 or change_pct < -0.3:
        trend = "FALLING"
        label = f"USDT.D في هبوط ↓ (slope={slope:.3f})"
    else:
        trend = "FLAT"
        label = f"USDT.D مستقر ↔ (slope={slope:.3f})"

    return trend, round(slope, 4), label


# ─────────────────────────────────────────────────────────────────
# الدالة الرئيسية لـ E11
# ─────────────────────────────────────────────────────────────────
def analyze(data: Dict) -> Optional[Dict]:
    """
    يحلل شارت USDT.D فقط ويُرجع اتجاه السوق.
    يُرجع: {name, long, short, why, usdt_level, usdt_trend}
    """
    global _USDT_D_HISTORY

    try:
        usdt_d = data.get("usdt_dominance", {})
        c4h    = data.get("c4h", [])   # فقط كـ fallback
        long_s  = 0.0
        short_s = 0.0
        why: Dict = {}

        # ── استخراج بيانات USDT.D ────────────────────────────────────
        value   = float(usdt_d.get("usdt_d", usdt_d.get("value", 5.0)))
        stables = float(usdt_d.get("stables", value))
        btc_d   = float(usdt_d.get("btc_d",  50.0))

        # ── تحديث السجل التاريخي ─────────────────────────────────────
        if value > 0:
            _USDT_D_HISTORY.append(value)

        # ── تحليل الاتجاه من السجل ───────────────────────────────────
        hist_trend, slope, hist_label = _analyze_usdt_trend(_USDT_D_HISTORY)
        why["usdt_trend"] = hist_label

        # ── 1. اتجاه USDT.D الحالي ───────────────────────────────────
        # قراءة rising/falling من البيانات المُمرَّرة
        passed_trend = usdt_d.get("trend", "NEUTRAL")
        rising  = usdt_d.get("rising",  hist_trend == "RISING")
        falling = usdt_d.get("falling", hist_trend == "FALLING")

        if falling or hist_trend == "FALLING":
            long_s  += 0.45
            why["usdt_direction"] = f"USDT.D هابط ↓ {value:.3f}% — دخول كريبتو 🟢"
        elif rising or hist_trend == "RISING":
            short_s += 0.45
            why["usdt_direction"] = f"USDT.D صاعد ↑ {value:.3f}% — خروج من كريبتو 🔴"
        elif passed_trend in ("BEAR", "BEAR_WEAK"):
            long_s  += 0.25
            why["usdt_direction"] = f"USDT.D اتجاه هبوطي {value:.3f}%"
        elif passed_trend in ("BULL", "BULL_WEAK"):
            short_s += 0.25
            why["usdt_direction"] = f"USDT.D اتجاه صعودي {value:.3f}%"

        # ── 2. المستويات المتطرفة ─────────────────────────────────────
        if value < 4.0:
            long_s  += 0.20
            why["usdt_extreme"] = f"USDT.D منخفض جداً {value:.3f}% — ذروة دخول كريبتو 🚀"
        elif value > 9.0:
            short_s += 0.20
            why["usdt_extreme"] = f"USDT.D مرتفع جداً {value:.3f}% — ذروة خروف من كريبتو ⛔"
        elif value > 8.0:
            short_s += 0.15
            why["usdt_extreme"] = f"USDT.D مرتفع {value:.3f}% — تحذير ⚠️"
        elif value < 5.0:
            long_s  += 0.10
            why["usdt_extreme"] = f"USDT.D منخفض {value:.3f}% — إيجابي 🟢"

        # ── 3. مجموع Stablecoins (USDT + USDC) ───────────────────────
        if stables > 15.0:
            # سيولة ضخمة في stablecoins → قد تنتقل للكريبتو قريباً
            long_s  += 0.10
            why["stables"] = f"Stables {stables:.1f}% — سيولة ضخمة تنتظر 💰"
        elif stables < 8.0:
            # معظم الأموال في كريبتو → تشبع → تحذير انعكاس
            short_s += 0.05
            why["stables"] = f"Stables {stables:.1f}% — أموال مستثمرة بالكامل ⚠️"

        # ── 4. BTC Dominance كمؤشر نوعية السوق ──────────────────────
        if btc_d > 60:
            # BTC يسيطر بقوة → altcoins ضعيفة → تحذير للـ alts
            why["btc_dom"] = f"BTC.D مرتفع {btc_d:.1f}% — تجنب الـ Alts"
        elif btc_d < 40:
            # موسم altcoins
            long_s  += 0.05
            why["btc_dom"] = f"BTC.D منخفض {btc_d:.1f}% — موسم Alts 🌟"

        # ── 5. Fallback — اذا لم تتوفر بيانات USDT.D ─────────────────
        if not usdt_d and c4h and len(c4h) >= 20:
            trend4 = IE.get_trend(c4h)
            if trend4 == "BULL":
                long_s  += 0.20; why["proxy"] = "BTC 4H proxy BULL (لا بيانات USDT.D)"
            elif trend4 == "BEAR":
                short_s += 0.20; why["proxy"] = "BTC 4H proxy BEAR (لا بيانات USDT.D)"

        return {
            "name":        "USDT",
            "long":        round(min(long_s,  1.0), 4),
            "short":       round(min(short_s, 1.0), 4),
            "why":         why,
            "usdt_level":  round(value, 3),
            "usdt_trend":  hist_trend,
        }

    except Exception as e:
        return {
            "name": "USDT", "long": 0.0, "short": 0.0,
            "why": {"err": str(e)}, "usdt_level": 0.0, "usdt_trend": "ERROR",
        }

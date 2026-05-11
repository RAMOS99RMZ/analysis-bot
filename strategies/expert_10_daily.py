
# strategies/expert_10_daily.py — E10: Daily Director
# ═══════════════════════════════════════════════════════════════════
# وظيفة E10: تحليل شارت BTC/ETH على 1D/4H/1H
#   → تحديد اتجاه السوق (LONG/SHORT)
#   → تحديد نوع التداول (Scalp / Swing / كلاهما)
#   → تخزين النتيجة لباقي الخبراء
#
# الإصلاحات المطبّقة:
#   1. CME Gap — weekday() حقيقي بدل فهرس الشمعة
#   2. ATR Filter — مضاعف الثقة حسب بيئة التذبذب
#   3. Level Proximity — تعزيز النقاط عند قرب السعر من S/R
# ═══════════════════════════════════════════════════════════════════
from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from engine.indicator_engine import IndicatorEngine
from utils.helpers import calc_volume_profile, utc_weekday

IE = IndicatorEngine()


# ─────────────────────────────────────────────────────────────────
# 1. CME Gap — يبحث عن الجمعة الحقيقية والاثنين الحقيقي
# ─────────────────────────────────────────────────────────────────
def _detect_cme_gap(c1d: List) -> Dict:
    """
    يكتشف CME Gap بين إغلاق الجمعة وفتح الاثنين.
    يستخدم timestamp الحقيقي لكل شمعة لتحديد اليوم.
    c1d[i] = [timestamp_ms, open, high, low, close, volume]
    """
    try:
        if not c1d or len(c1d) < 5:
            return {"has_gap": False}

        friday_close: Optional[float] = None
        monday_open:  Optional[float] = None

        for candle in c1d[:14]:                    # آخر 14 يوم
            ts = int(candle[0]) / 1000             # ms → seconds
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            wd = dt.weekday()                      # 0=Mon, 4=Fri

            if wd == 4 and friday_close is None:   # جمعة
                friday_close = float(candle[4])
            elif wd == 0 and monday_open is None:  # اثنين
                monday_open  = float(candle[1])

            if friday_close is not None and monday_open is not None:
                break

        if friday_close is None or monday_open is None:
            return {"has_gap": False}

        gap_pct = (monday_open - friday_close) / friday_close

        if abs(gap_pct) > 0.005:                   # فجوة > 0.5%
            return {
                "has_gap":  True,
                "gap_pct":  round(gap_pct, 4),
                "gap_fill": friday_close,
                "bullish":  gap_pct < 0,           # فجوة أسفل = ستُملأ صعوداً
            }
        return {"has_gap": False}

    except Exception:
        return {"has_gap": False}


# ─────────────────────────────────────────────────────────────────
# 2. ATR Filter — تصنيف بيئة التذبذب وتعديل المضاعف
# ─────────────────────────────────────────────────────────────────
def _atr_filter(c4h: List, price: float) -> Tuple[float, str]:
    """
    يحسب ATR% ويُرجع (مضاعف النقاط, نص الحالة).
    DEAD    < 0.5%  → 0.0  (لا تداول)
    LOW     < 1.0%  → 0.6
    NORMAL  1-4%    → 1.0  (مثالي)
    HIGH    4-7%    → 0.7
    EXTREME > 7%    → 0.0  (لا تداول)
    """
    try:
        atr_val = IE.atr(c4h, 14)
        atr_pct = (atr_val / price) * 100 if price > 0 else 2.0

        if atr_pct < 0.5:   return 0.0, f"راكد جداً 💤 ({atr_pct:.2f}%)"
        elif atr_pct < 1.0: return 0.6, f"تذبذب منخفض ⚠️ ({atr_pct:.2f}%)"
        elif atr_pct <= 4.0:return 1.0, f"بيئة مثالية ✅ ({atr_pct:.2f}%)"
        elif atr_pct <= 7.0:return 0.7, f"تذبذب مرتفع ⚠️ ({atr_pct:.2f}%)"
        else:                return 0.0, f"خطر — تذبذب متطرف 🚨 ({atr_pct:.2f}%)"
    except Exception:
        return 1.0, "ATR غير محسوب"


# ─────────────────────────────────────────────────────────────────
# 3. Level Proximity — تعزيز النقاط عند قرب السعر من S/R
# ─────────────────────────────────────────────────────────────────
def _proximity_bonus(c4h: List, c1d: List, price: float) -> Tuple[float, str]:
    """
    يفحص هل السعر قريب من مستوى مهم (Pivot/Fib/EMA/POC).
    القرب = ضمن 1.5% من المستوى.
    يُرجع (نقطة إضافية 0→0.15, سبب).
    """
    try:
        zone = price * 0.015   # 1.5% من السعر

        levels: List[Tuple[str, float]] = []

        # Pivot Points من الشمعة اليومية السابقة
        if c1d and len(c1d) >= 2:
            prev = c1d[1]
            H = float(prev[2]); L = float(prev[3]); C = float(prev[4])
            PP = (H + L + C) / 3
            levels += [
                ("PP",    PP),
                ("R1",    2*PP - L),
                ("S1",    2*PP - H),
            ]

        # EMA المهمة
        ema20  = IE.ema(c4h, 20)  if c4h else None
        ema50  = IE.ema(c4h, 50)  if c4h else None
        ema200 = IE.ema(c4h, 200) if c4h and len(c4h) >= 200 else None
        for name, val in [("EMA20", ema20), ("EMA50", ema50), ("EMA200", ema200)]:
            if val:
                levels.append((name, val))

        # Fibonacci
        lookback = (c4h or [])[:60]
        if len(lookback) >= 20:
            hi  = max(float(c[2]) for c in lookback)
            lo  = min(float(c[3]) for c in lookback)
            rng = hi - lo
            for ratio, name in [(0.382,"F38"),(0.500,"F50"),(0.618,"F62")]:
                levels.append((name, hi - rng * ratio))

        # POC
        vp  = calc_volume_profile(c4h[:40] if c4h else [], 10)
        poc = vp.get("poc", 0)
        if poc > 0:
            levels.append(("POC", poc))

        hits = [n for n, v in levels if v > 0 and abs(price - v) <= zone]

        if len(hits) >= 3:
            return 0.15, f"تقاطع قوي: {', '.join(hits[:3])}"
        elif len(hits) == 2:
            return 0.10, f"تقاطع: {', '.join(hits)}"
        elif len(hits) == 1:
            return 0.05, f"قرب من {hits[0]}"
        else:
            return 0.0, "بعيد عن المستويات الرئيسية"

    except Exception:
        return 0.0, "—"


# ─────────────────────────────────────────────────────────────────
# الدالة الرئيسية لـ E10
# ─────────────────────────────────────────────────────────────────
def analyze(data: Dict) -> Optional[Dict]:
    """
    يحلل شارت BTC/ETH على 1D/4H/1H.
    يُرجع: {name, long, short, why, trade_type, atr_label}
    """
    try:
        c1d = data.get("c1d", [])
        c1w = data.get("c1w", [])
        c4h = data.get("c4h", [])
        c1h = data.get("c1h", [])

        if not c4h or len(c4h) < 20:
            return None

        price   = float(c4h[0][4])
        long_s  = 0.0
        short_s = 0.0
        why: Dict = {}

        # ── ATR Filter (يُطبَّق في النهاية كمضاعف) ─────────────────
        atr_mult, atr_label = _atr_filter(c4h, price)
        why["atr"] = atr_label

        # إذا كانت البيئة غير قابلة للتداول — أرجع صفر مباشرة
        if atr_mult == 0.0:
            return {
                "name": "Daily", "long": 0.0, "short": 0.0,
                "why": why, "trade_type": "NONE", "atr_label": atr_label,
            }

        # ── 1. Weekly Trend ──────────────────────────────────────────
        if c1w and len(c1w) >= 4:
            wk_trend = IE.get_trend(c1w)
            if wk_trend == "BULL":
                long_s  += 0.30; why["weekly"] = "Weekly BULL ↑"
            elif wk_trend == "BEAR":
                short_s += 0.30; why["weekly"] = "Weekly BEAR ↓"

        # ── 2. Daily Trend ───────────────────────────────────────────
        if c1d and len(c1d) >= 20:
            d_trend = IE.get_trend(c1d)
            if d_trend == "BULL":
                long_s  += 0.25; why["daily"] = "Daily BULL"
            elif d_trend == "BEAR":
                short_s += 0.25; why["daily"] = "Daily BEAR"

        # ── 3. Daily POC ─────────────────────────────────────────────
        if c1d and len(c1d) >= 10:
            vp  = calc_volume_profile(c1d, 30)
            poc = vp.get("poc", 0)
            if poc > 0:
                if price > poc * 1.002:
                    long_s  += 0.20; why["poc"] = f"فوق POC اليومي ${poc:,.0f}"
                elif price < poc * 0.998:
                    short_s += 0.20; why["poc"] = f"تحت POC اليومي ${poc:,.0f}"

        # ── 4. CME Gap (weekday حقيقي) ───────────────────────────────
        gap = _detect_cme_gap(c1d)
        if gap["has_gap"]:
            gap_pct_str = f"{abs(gap['gap_pct']):.2%}"
            if gap["bullish"]:
                long_s  += 0.25
                why["cme"] = f"CME Gap صاعد {gap_pct_str} — ستُملأ صعوداً"
            else:
                short_s += 0.25
                why["cme"] = f"CME Gap هابط {gap_pct_str} — ستُملأ هبوطاً"

        # ── 5. 4H Regime ─────────────────────────────────────────────
        regime = IE.get_market_regime(c4h, c1h)
        if regime.get("bull_align"):
            long_s  += 0.20; why["regime"] = "4H EMA مصطفة صعوداً"
        elif regime.get("bear_align"):
            short_s += 0.20; why["regime"] = "4H EMA مصطفة هبوطاً"

        # ── 6. Weekday Bonus (ثلاثاء/أربعاء/خميس أقوى إحصائياً) ─────
        wd = utc_weekday()
        if wd in (1, 2, 3):
            long_s  += 0.05; why["weekday"] = "أيام قوة إحصائية (ث/ر/خ)"

        # ── 7. Level Proximity Bonus ─────────────────────────────────
        prox_bonus, prox_why = _proximity_bonus(c4h, c1d, price)
        if prox_bonus > 0:
            # نُضيف المكافأة للاتجاه الأقوى حالياً
            if long_s >= short_s:
                long_s  += prox_bonus
            else:
                short_s += prox_bonus
            why["proximity"] = prox_why

        # ── تطبيق ATR كمضاعف ─────────────────────────────────────────
        long_s  = round(min(long_s  * atr_mult, 1.0), 4)
        short_s = round(min(short_s * atr_mult, 1.0), 4)

        # ── تحديد نوع التداول بناءً على ATR ─────────────────────────
        try:
            atr_val = IE.atr(c4h, 14)
            atr_pct = (atr_val / price) * 100
            if atr_pct < 1.5:
                trade_type = "SCALP"           # تذبذب منخفض → scalp فقط
            elif atr_pct > 4.0:
                trade_type = "SWING"           # تذبذب مرتفع → swing فقط
            else:
                trade_type = "SCALP+SWING"     # بيئة مثالية → كلاهما
        except Exception:
            trade_type = "SCALP+SWING"

        return {
            "name":       "Daily",
            "long":       long_s,
            "short":      short_s,
            "why":        why,
            "trade_type": trade_type,
            "atr_label":  atr_label,
        }

    except Exception as e:
        return {
            "name": "Daily", "long": 0.0, "short": 0.0,
            "why": {"err": str(e)}, "trade_type": "UNKNOWN", "atr_label": "—",
        }

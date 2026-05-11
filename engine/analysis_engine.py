
# engine/analysis_engine.py
# ═══════════════════════════════════════════════════════════════════════
# E10 + E11 — محرك التحليل اليومي الكامل  (النسخة المُحسَّنة)
#
# الإصلاحات المطبَّقة:
#   1. CME Gap  — يعتمد weekday() الحقيقي لا فهرس الشمعة
#   2. أوزان متوازنة — كل خبير مُقيَّس على [-1, +1] قبل الجمع
#   3. فلتر تقارب المستويات — يُضاعف الثقة عند قرب السعر من S/R
#   4. فلتر ATR للتذبذب — يُحذّر أو يُعطّل في السوق الراكد/المتطرف
#   5. فحص ارتباط BTC — لا يُحلل ETH إذا انفصل عن BTC
# ═══════════════════════════════════════════════════════════════════════

from __future__ import annotations
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import httpx
from loguru import logger

from engine.indicator_engine import IndicatorEngine
from utils.helpers import calc_volume_profile

IE = IndicatorEngine()

# ══════════════════════════════════════════════════════════════════════
# الأوزان الرسمية لكل خبير  (يجب أن يجمع إلى 1.0)
# ClassicTA وSMC أعلى وزناً لأنهما الأكثر موثوقية في الكريبتو
# Gann/Lunar أقل وزناً لأنها استكشافية
# ══════════════════════════════════════════════════════════════════════
EXPERT_WEIGHTS: Dict[str, float] = {
    "ClassicTA": 0.25,   # RSI + MACD + SuperTrend — الأعلى موثوقية
    "SMC":       0.20,   # Order Blocks + BOS — مؤسسي
    "Wyckoff":   0.15,   # مراحل السوق
    "OBV":       0.15,   # حجم التداول — مؤشر رائد
    "Ichimoku":  0.12,   # اتجاه + سحابة
    "Harmonic":  0.08,   # أنماط هندسية
    "Gann":      0.05,   # قمري/زمني — استكشافي
}
# نطاق النقاط الخام لكل خبير (للتقييس)
EXPERT_RAW_MAX: Dict[str, float] = {
    "ClassicTA": 5.0,   # يمكن أن يصل ±5
    "SMC":       1.0,
    "Wyckoff":   1.0,
    "OBV":       1.0,
    "Ichimoku":  1.0,
    "Harmonic":  1.0,
    "Gann":      1.0,
}


# ══════════════════════════════════════════════════════════════════════
# 1. CME Gap — بأيام الأسبوع الحقيقية
# ══════════════════════════════════════════════════════════════════════

def _detect_cme_gap(c1d: List) -> Dict:
    """
    يكتشف CME Gap بين إغلاق الجمعة وفتح الاثنين.
    يبحث في c1d عن الجمعة والاثنين الفعليين باستخدام timestamp.
    c1d[i] = [timestamp_ms, open, high, low, close, volume]
    """
    try:
        if not c1d or len(c1d) < 5:
            return {"has_gap": False}

        friday_close: Optional[float] = None
        monday_open:  Optional[float] = None

        for candle in c1d[:14]:           # نبحث في آخر 14 يوم فقط
            ts  = int(candle[0]) / 1000   # تحويل milliseconds → seconds
            dt  = datetime.fromtimestamp(ts, tz=timezone.utc)
            wd  = dt.weekday()            # 0=Mon … 4=Fri … 6=Sun

            if wd == 4 and friday_close is None:   # الجمعة
                friday_close = float(candle[4])
            elif wd == 0 and monday_open is None:  # الاثنين
                monday_open  = float(candle[1])

            if friday_close is not None and monday_open is not None:
                break

        if friday_close is None or monday_open is None:
            return {"has_gap": False}

        gap_pct = (monday_open - friday_close) / friday_close

        if abs(gap_pct) > 0.005:          # فجوة > 0.5%
            return {
                "has_gap":  True,
                "gap_pct":  round(gap_pct, 4),
                "gap_fill": friday_close,
                "bullish":  gap_pct < 0,  # الاثنين أقل = فجوة صاعدة (ستُملأ صعوداً)
            }
        return {"has_gap": False}

    except Exception as e:
        logger.debug(f"[CME Gap] {e}")
        return {"has_gap": False}


# ══════════════════════════════════════════════════════════════════════
# 2. فلتر ATR — تصنيف بيئة التذبذب
# ══════════════════════════════════════════════════════════════════════

def _atr_environment(c4h: List, price: float) -> Dict:
    """
    يحسب ATR% ويُصنّف البيئة:
      - DEAD  : ATR% < 0.5%  → سوق راكد — لا تداول
      - LOW   : ATR% < 1.0%  → تذبذب منخفض — تقليل الحجم
      - NORMAL: 1% ≤ ATR% ≤ 4% → بيئة مثالية
      - HIGH  : ATR% > 4%   → تذبذب مرتفع — تقليل الحجم
      - EXTREME: ATR% > 7%  → خطر — لا تداول
    """
    try:
        atr_val = IE.atr(c4h, 14)
        atr_pct = (atr_val / price) * 100 if price > 0 else 2.0

        if atr_pct < 0.5:
            env = "DEAD";    label = "راكد جداً 💤";     trade = False; score_mult = 0.0
        elif atr_pct < 1.0:
            env = "LOW";     label = "تذبذب منخفض ⚠️";  trade = True;  score_mult = 0.6
        elif atr_pct <= 4.0:
            env = "NORMAL";  label = "بيئة مثالية ✅";   trade = True;  score_mult = 1.0
        elif atr_pct <= 7.0:
            env = "HIGH";    label = "تذبذب مرتفع ⚠️";  trade = True;  score_mult = 0.7
        else:
            env = "EXTREME"; label = "خطر — تذبذب متطرف 🚨"; trade = False; score_mult = 0.0

        return {
            "atr_val":    round(atr_val, 2),
            "atr_pct":    round(atr_pct, 2),
            "env":        env,
            "label":      label,
            "tradeable":  trade,
            "score_mult": score_mult,    # مضاعف الثقة النهائية
        }
    except Exception as e:
        logger.debug(f"[ATR] {e}")
        return {"atr_val": 0, "atr_pct": 2.0, "env": "NORMAL",
                "label": "غير محسوب", "tradeable": True, "score_mult": 1.0}


# ══════════════════════════════════════════════════════════════════════
# 3. فلتر تقارب المستويات
# ══════════════════════════════════════════════════════════════════════

def _proximity_multiplier(price: float, levels: Dict, atr_val: float) -> Tuple[float, str]:
    """
    يفحص هل السعر قريب من مستوى S/R مهم (Fib / Pivot / EMA / POC).
    القرب = ضمن 1.5 × ATR من المستوى.
    يُرجع: (مضاعف 0.5→1.5, نص السبب)
    """
    try:
        zone = atr_val * 1.5 if atr_val > 0 else price * 0.015

        key_levels: List[Tuple[str, float]] = [
            ("Fib 38.2%",  levels.get("fib", {}).get("38.2", 0)),
            ("Fib 50.0%",  levels.get("fib", {}).get("50.0", 0)),
            ("Fib 61.8%",  levels.get("fib", {}).get("61.8", 0)),
            ("Pivot PP",   levels.get("pp", 0)),
            ("EMA 20",     levels.get("ema20", 0)),
            ("EMA 50",     levels.get("ema50", 0)),
            ("EMA 200",    levels.get("ema200", 0)),
            ("POC",        levels.get("poc", 0)),
        ]

        hits: List[str] = []
        for name, lvl in key_levels:
            if lvl > 0 and abs(price - lvl) <= zone:
                hits.append(name)

        if len(hits) >= 3:
            return 1.5, f"تقاطع قوي: {', '.join(hits[:3])}"
        elif len(hits) == 2:
            return 1.25, f"تقاطع: {', '.join(hits)}"
        elif len(hits) == 1:
            return 1.10, f"قرب من {hits[0]}"
        else:
            return 0.85, "بعيد عن المستويات الرئيسية"

    except Exception as e:
        logger.debug(f"[Proximity] {e}")
        return 1.0, "—"


# ══════════════════════════════════════════════════════════════════════
# 4. فحص ارتباط BTC-ETH
# ══════════════════════════════════════════════════════════════════════

def _btc_eth_correlation(btc_closes: List[float], eth_closes: List[float],
                          lookback: int = 20) -> Dict:
    """
    يحسب ارتباط Pearson بين BTC وETH على آخر N شمعة.
    إذا كان الارتباط < 0.5 → ETH منفصل عن BTC → تحذير.
    """
    try:
        n = min(lookback, len(btc_closes), len(eth_closes))
        if n < 10:
            return {"corr": 1.0, "aligned": True, "label": "بيانات غير كافية"}

        bx = btc_closes[:n]
        ex = eth_closes[:n]

        bm = sum(bx) / n
        em = sum(ex) / n

        num   = sum((bx[i] - bm) * (ex[i] - em) for i in range(n))
        den_b = sum((bx[i] - bm) ** 2 for i in range(n)) ** 0.5
        den_e = sum((ex[i] - em) ** 2 for i in range(n)) ** 0.5

        if den_b == 0 or den_e == 0:
            return {"corr": 1.0, "aligned": True, "label": "لا يمكن الحساب"}

        corr = num / (den_b * den_e)
        corr = max(-1.0, min(1.0, corr))

        if corr >= 0.75:
            aligned = True
            label   = f"ارتباط قوي ✅ ({corr:.2f})"
        elif corr >= 0.50:
            aligned = True
            label   = f"ارتباط متوسط 🟡 ({corr:.2f})"
        else:
            aligned = False
            label   = f"انفصال ETH عن BTC ⚠️ ({corr:.2f}) — تحليل مستقل"

        return {"corr": round(corr, 3), "aligned": aligned, "label": label}

    except Exception as e:
        logger.debug(f"[Corr] {e}")
        return {"corr": 1.0, "aligned": True, "label": "خطأ في الحساب"}


# ══════════════════════════════════════════════════════════════════════
# حساب مستويات S/R والأهداف (بدون تغيير جوهري)
# ══════════════════════════════════════════════════════════════════════

def _calc_levels(c1d, c4h, c1h, price: float, direction: str) -> Dict:
    """يحسب دعوم ومقاومات وأهداف حقيقية من Pivot + Fibonacci + EMA."""
    try:
        # Pivot Points من الشمعة اليومية السابقة
        if c1d and len(c1d) >= 2:
            prev = c1d[1]
            H = float(prev[2]); L = float(prev[3]); C = float(prev[4])
            PP = (H + L + C) / 3
            R1 = 2*PP - L;   R2 = PP + (H - L);   R3 = H + 2*(PP - L)
            S1 = 2*PP - H;   S2 = PP - (H - L);   S3 = L - 2*(H - PP)
        else:
            PP = price
            R1 = price*1.01; R2 = price*1.02; R3 = price*1.03
            S1 = price*0.99; S2 = price*0.98; S3 = price*0.97

        # Fibonacci من Swing الأخير (4H × 60 شمعة)
        lookback = (c4h or [])[:60]
        if len(lookback) >= 20:
            hi  = max(float(c[2]) for c in lookback)
            lo  = min(float(c[3]) for c in lookback)
            rng = hi - lo
            f382 = hi - rng * 0.382
            f500 = hi - rng * 0.500
            f618 = hi - rng * 0.618
            f786 = hi - rng * 0.786
            ext1272 = lo - rng * 0.272
            ext1618 = lo - rng * 0.618
        else:
            f382 = f500 = f618 = f786 = price
            hi = lo = price
            ext1272 = ext1618 = price

        # EMA Levels
        ema20  = IE.ema(c4h, 20)  if c4h else price
        ema50  = IE.ema(c4h, 50)  if c4h else price
        ema200 = IE.ema(c4h, 200) if c4h and len(c4h) >= 200 else price

        # Volume Profile POC
        vp  = calc_volume_profile(c4h[:40] if c4h else [], 15)
        poc = vp.get("poc", price)

        # أهداف بحسب الاتجاه
        if direction == "BULL":
            tp1 = R1; tp2 = R2; tp3 = R3
            sup1 = S1; sup2 = f618; sup3 = S2
        else:
            tp1 = S1; tp2 = S2; tp3 = S3
            sup1 = R1; sup2 = f382; sup3 = R2

        return {
            "tp1": tp1, "tp2": tp2, "tp3": tp3,
            "sup1": sup1, "sup2": sup2, "sup3": sup3,
            "pp": PP, "poc": poc,
            "ema20": ema20, "ema50": ema50, "ema200": ema200,
            "fib": {"38.2": f382, "50.0": f500, "61.8": f618, "78.6": f786},
            "ext": {"127.2": ext1272, "161.8": ext1618},
            "hi": hi, "lo": lo,
        }
    except Exception as e:
        logger.warning(f"[Levels] {e}")
        return {
            "tp1": price, "tp2": price, "tp3": price,
            "sup1": price, "sup2": price, "sup3": price,
            "pp": price, "poc": price,
            "ema20": price, "ema50": price, "ema200": price,
            "fib": {}, "ext": {}, "hi": price, "lo": price,
        }


# ══════════════════════════════════════════════════════════════════════
# تحليل الخبراء — بنقاط خام ثم تقييس بالأوزان
# ══════════════════════════════════════════════════════════════════════

def _expert_mini(c1d, c4h, c1h, c5m, price: float) -> Dict:
    """
    يُشغّل 7 مدارس تحليل ويُرجع نتيجة كل منها.
    score_raw  : النقطة الخام من الخبير
    score_norm : النقطة بعد التقييس على [-1, +1]
    score_weighted : score_norm × وزن الخبير
    """
    results: Dict = {}

    # ── 1. ClassicTA — RSI + MACD + SuperTrend + HeikinAshi ──────────
    try:
        rsi4h  = IE.rsi(c4h, 14)            if c4h else 50
        macd4h = IE.macd(c4h)               if c4h else {"hist": 0}
        st4h   = IE.supertrend(c4h, 3)      if c4h and len(c4h) >= 14 else None
        ha4h   = IE.heikin_ashi(c4h)        if c4h else None
        adx4h  = IE.adx(c4h, 14)            if c4h else None

        raw = 0.0
        if rsi4h < 45:    raw -= 1.0
        elif rsi4h > 55:  raw += 1.0
        if rsi4h < 35:    raw -= 1.0   # ذعر — إضافي
        elif rsi4h > 70:  raw += 0.5   # ذروة شراء — حذر

        if macd4h["hist"] > 0: raw += 1.0
        else:                  raw -= 1.0

        if st4h and st4h["bull"]: raw += 2.0
        elif st4h:                raw -= 2.0

        if ha4h and ha4h["bull"]: raw += 1.0
        elif ha4h:                raw -= 1.0

        trend_str = "قوي" if adx4h and adx4h["adx"] > 30 else "ضعيف"
        sig   = "صاعد 🟢" if raw >= 2.5 else "هابط 🔴" if raw <= -2.5 else "محايد 🟡"
        emoji = "🟢" if raw >= 2.5 else "🔴" if raw <= -2.5 else "🟡"

        results["ClassicTA"] = {
            "emoji": emoji, "signal": sig,
            "detail": (f"RSI={rsi4h:.0f} | "
                       f"MACD={'↑' if macd4h['hist']>0 else '↓'} | "
                       f"ST={'Bull' if st4h and st4h['bull'] else 'Bear'} | "
                       f"اتجاه {trend_str}"),
            "score_raw": raw,
        }
    except Exception as e:
        results["ClassicTA"] = {"emoji": "❓", "signal": "خطأ",
                                "detail": str(e), "score_raw": 0}

    # ── 2. SMC — BOS + Order Block ───────────────────────────────────
    try:
        raw = 0.0
        smc_detail = "بيانات غير كافية"
        smc_sig, smc_e = "محايد 🟡", "🟡"

        if c4h and len(c4h) >= 12:
            rH = max(float(c[2]) for c in c4h[:6])
            rL = min(float(c[3]) for c in c4h[:6])
            pH = max(float(c[2]) for c in c4h[6:12])
            pL = min(float(c[3]) for c in c4h[6:12])
            bos_bull = rH > pH * 1.002
            bos_bear = rL < pL * 0.998

            if bos_bull and price > pH:
                raw = 1.0; smc_sig, smc_e = "BOS صاعد 🟢", "🟢"
                smc_detail = f"كسر مقاومة ${pH:,.0f} — ترقب OB للشراء"
            elif bos_bear and price < pL:
                raw = -1.0; smc_sig, smc_e = "BOS هابط 🔴", "🔴"
                smc_detail = f"كسر دعم ${pL:,.0f} — ترقب OB للبيع"
            else:
                raw = 0.0
                smc_detail = f"Range: ${pL:,.0f} – ${pH:,.0f}"

        results["SMC"] = {"emoji": smc_e, "signal": smc_sig,
                          "detail": smc_detail, "score_raw": raw}
    except Exception as e:
        results["SMC"] = {"emoji": "❓", "signal": "خطأ",
                          "detail": str(e), "score_raw": 0}

    # ── 3. Wyckoff ────────────────────────────────────────────────────
    try:
        from strategies.expert_03_wyckoff import detect_wyckoff_phase
        w4h = detect_wyckoff_phase(c4h) if c4h and len(c4h) >= 30 \
              else {"phase": "UNKNOWN", "bias": 0, "events": []}
        phase  = w4h.get("phase", "UNKNOWN")
        bias   = float(w4h.get("bias", 0))
        events = w4h.get("events", [])
        phase_ar = {
            "ACCUMULATION": "تراكم (شراء)",
            "DISTRIBUTION":  "توزيع (بيع)",
            "MARKUP":        "صعود مؤكد",
            "MARKDOWN":      "هبوط مؤكد",
            "UNKNOWN":       "غير محدد",
        }.get(phase, phase)
        wyc_e  = "🟢" if bias > 0 else "🔴" if bias < 0 else "🟡"
        ev_str = " + ".join(events[:2]) if events else "—"
        results["Wyckoff"] = {
            "emoji": wyc_e, "signal": phase_ar,
            "detail": f"4H: {phase_ar} | أحداث: {ev_str}",
            "score_raw": bias,
        }
    except Exception as e:
        results["Wyckoff"] = {"emoji": "❓", "signal": "غير محدد",
                              "detail": "—", "score_raw": 0}

    # ── 4. Harmonic Patterns ──────────────────────────────────────────
    try:
        from strategies.expert_02_harmonic import analyze as harm_analyze
        hres = harm_analyze({"c5m": c5m or [], "c15m": [],
                             "c30m": [], "c1h": c1h or [], "c4h": c4h or []})
        raw = 0.0
        harm_sig, harm_e, harm_detail = "لا نموذج 🟡", "🟡", "—"
        if hres:
            ls = hres.get("long", 0); ss = hres.get("short", 0)
            hw = hres.get("why", {})
            if ls > 0.20:
                raw = 1.0; harm_sig, harm_e = "نموذج صاعد 🟢", "🟢"
                harm_detail = hw.get("harmonic", hw.get("fib", "Fibonacci دعم"))
            elif ss > 0.20:
                raw = -1.0; harm_sig, harm_e = "نموذج هابط 🔴", "🔴"
                harm_detail = hw.get("harmonic_bear", hw.get("fib", "Fibonacci مقاومة"))
            else:
                harm_detail = "السوق في منتصف النطاق"
        results["Harmonic"] = {"emoji": harm_e, "signal": harm_sig,
                               "detail": harm_detail, "score_raw": raw}
    except Exception as e:
        results["Harmonic"] = {"emoji": "❓", "signal": "لا نموذج",
                               "detail": "—", "score_raw": 0}

    # ── 5. Gann + Lunar ───────────────────────────────────────────────
    try:
        from strategies.expert_08_gann import _calc_lunar_phase, _gann_sq9
        lunar = _calc_lunar_phase()
        sq9   = _gann_sq9(price)
        phase_ar = {
            "NEW_MOON":  "هلال (بداية صعود)",
            "WAXING_G":  "تربع أول (صعود)",
            "FIRST_Q":   "ربع أول (ارتفاع)",
            "FULL_MOON": "بدر (تحذير انعكاس)",
            "WANING_G":  "تربع ثانٍ (هبوط)",
            "LAST_Q":    "ربع أخير (ضعف)",
            "WANING_C":  "محاق (نهاية هبوط)",
            "WAXING_C":  "هلال صغير",
        }.get(lunar["phase"], lunar["phase"])
        raw    = 1.0 if lunar["bias"] == "BULL" else -1.0
        gann_e = "🟢" if raw > 0 else "🔴"
        sq9_levels = sq9.get("levels", [])
        near_lvl   = min(sq9_levels, key=lambda x: abs(x - price)) \
                     if sq9_levels else price
        results["Gann"] = {
            "emoji": gann_e,
            "signal": f"{lunar['emoji']} {phase_ar}",
            "detail": f"يوم {lunar['days_into']:.0f} | Sq9 أقرب: ${near_lvl:,.0f}",
            "score_raw": raw,
        }
    except Exception as e:
        results["Gann"] = {"emoji": "❓", "signal": "غير محدد",
                           "detail": "—", "score_raw": 0}

    # ── 6. OBV + Volume Profile + VWAP ───────────────────────────────
    try:
        obv4h = IE.obv(c4h)  if c4h and len(c4h) >= 12 else None
        cmf4h = IE.cmf(c4h)  if c4h and len(c4h) >= 14 else None
        vwap  = IE.vwap(c4h) if c4h else 0
        vp    = calc_volume_profile(c4h[:40] if c4h else [], 10)
        poc   = vp.get("poc", price)

        raw = 0.0
        obv_sig, obv_e = "محايد 🟡", "🟡"
        if obv4h:
            if obv4h.get("div_bull"):  raw = 1.0;  obv_sig, obv_e = "تباعد صاعد (قوي) 🟢", "🟢"
            elif obv4h.get("rising"):  raw = 0.7;  obv_sig, obv_e = "حجم صاعد 🟢", "🟢"
            elif obv4h.get("div_bear"):raw = -1.0; obv_sig, obv_e = "تباعد هابط (تحذير) 🔴", "🔴"
            else:                      raw = -0.7; obv_sig, obv_e = "حجم هابط 🔴", "🔴"

        # CMF كمصحّح إضافي
        if cmf4h:
            cv = cmf4h.get("value", 0)
            raw += 0.3 if cv > 0.05 else -0.3 if cv < -0.05 else 0
        raw = max(-1.0, min(1.0, raw))

        above_vwap = "فوق VWAP ✅" if vwap and price > vwap else "تحت VWAP ⚠️"
        cmf_str    = f"CMF={cmf4h['value']:.3f}" if cmf4h else ""
        results["OBV"] = {
            "emoji": obv_e, "signal": obv_sig,
            "detail": f"POC=${poc:,.0f} | {above_vwap} | {cmf_str}",
            "score_raw": raw,
        }
    except Exception as e:
        results["OBV"] = {"emoji": "❓", "signal": "محايد",
                          "detail": "—", "score_raw": 0}

    # ── 7. Ichimoku + EMA Alignment ──────────────────────────────────
    try:
        ichi      = IE.ichimoku(c4h) if c4h and len(c4h) >= 55 else None
        ema20_4h  = IE.ema(c4h, 20)  if c4h else price
        ema50_4h  = IE.ema(c4h, 50)  if c4h else price
        ema200_4h = IE.ema(c4h, 200) if c4h and len(c4h) >= 200 else price

        ema_bull = price > ema20_4h > ema50_4h
        ema_bear = price < ema20_4h < ema50_4h

        raw = 0.0
        if ichi:
            if   ichi.get("above_cloud") and ichi.get("bull_cross") and ema_bull:
                raw = 1.0;  ich_sig, ich_e = "فوق السحابة (صاعد قوي) 🟢", "🟢"
            elif ichi.get("above_cloud"):
                raw = 0.7;  ich_sig, ich_e = "فوق السحابة 🟢", "🟢"
            elif ichi.get("below_cloud") and not ichi.get("bull_cross") and ema_bear:
                raw = -1.0; ich_sig, ich_e = "تحت السحابة (هابط قوي) 🔴", "🔴"
            elif ichi.get("below_cloud"):
                raw = -0.7; ich_sig, ich_e = "تحت السحابة 🔴", "🔴"
            else:
                raw = 0.0;  ich_sig, ich_e = "داخل السحابة 🟡", "🟡"
        else:
            if ema_bull:   raw = 0.8;  ich_sig, ich_e = "🟢 EMA صاعد", "🟢"
            elif ema_bear: raw = -0.8; ich_sig, ich_e = "🔴 EMA هابط", "🔴"
            else:          raw = 0.0;  ich_sig, ich_e = "🟡 EMA محايد", "🟡"

        results["Ichimoku"] = {
            "emoji": ich_e, "signal": ich_sig,
            "detail": (f"EMA20=${ema20_4h:,.0f} | "
                       f"EMA50=${ema50_4h:,.0f} | "
                       f"EMA200=${ema200_4h:,.0f}"),
            "score_raw": raw,
        }
    except Exception as e:
        results["Ichimoku"] = {"emoji": "❓", "signal": "محايد",
                               "detail": "—", "score_raw": 0}

    # ── تقييس النقاط بالأوزان ────────────────────────────────────────
    for name, res in results.items():
        raw     = res["score_raw"]
        max_raw = EXPERT_RAW_MAX.get(name, 1.0)
        norm    = max(-1.0, min(1.0, raw / max_raw))   # [-1, +1]
        weight  = EXPERT_WEIGHTS.get(name, 0.10)
        res["score_norm"]     = round(norm, 4)
        res["score_weighted"] = round(norm * weight, 4)
        # score متوافق مع باقي الكود القديم
        res["score"] = res["score_weighted"]

    return results


# ══════════════════════════════════════════════════════════════════════
# جلب USDT Dominance من CoinGecko — E11
# ══════════════════════════════════════════════════════════════════════

async def _fetch_usdt_dominance() -> Dict:
    """
    E11: يجلب USDT.D من CoinGecko ويحلل شارت USDT.D تحديداً.
    USDT.D صاعد → أموال تدخل stablecoins → سلبي للكريبتو → SHORT
    USDT.D هابط → أموال تخرج من stablecoins → إيجابي للكريبتو → LONG
    """
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            r = await client.get(
                "https://api.coingecko.com/api/v3/global",
                headers={"Accept": "application/json"},
            )
            if r.status_code == 200:
                data     = r.json().get("data", {})
                dom      = data.get("market_cap_percentage", {})
                usdt_d   = dom.get("usdt", 0)
                usdc_d   = dom.get("usdc", 0)
                btc_d    = dom.get("btc",  0)
                total_mc = data.get("total_market_cap",  {}).get("usd", 0)
                total_vol= data.get("total_volume",      {}).get("usd", 0)
                stables  = usdt_d + usdc_d

                # تحليل اتجاه USDT.D
                if usdt_d > 8.0:
                    trend = "BULL"; trend_ar = "USDT.D صاعد (خروج من كريبتو) 🔴"; signal = "SHORT"
                elif usdt_d > 7.0:
                    trend = "BULL_WEAK"; trend_ar = "USDT.D ارتفاع معتدل 🟡"; signal = "NEUTRAL"
                elif usdt_d > 6.0:
                    trend = "NEUTRAL"; trend_ar = "USDT.D محايد 🟡"; signal = "NEUTRAL"
                elif usdt_d > 5.0:
                    trend = "BEAR_WEAK"; trend_ar = "USDT.D انخفاض — دخول كريبتو 🟡"; signal = "NEUTRAL"
                else:
                    trend = "BEAR"; trend_ar = "USDT.D هابط (دخول للكريبتو) 🟢"; signal = "LONG"

                # اتجاه stablecoins المُجمَّع
                if stables > 15:
                    extra = "📊 نسبة stables مرتفعة جداً — سيولة تنتظر"
                elif stables > 12:
                    extra = "📊 نسبة stables مرتفعة — حذر"
                else:
                    extra = "📊 نسبة stables طبيعية"

                return {
                    "usdt_d":    round(usdt_d, 3),
                    "usdc_d":    round(usdc_d, 3),
                    "btc_d":     round(btc_d,  3),
                    "stables":   round(stables,3),
                    "total_mc":  total_mc,
                    "total_vol": total_vol,
                    "trend":     trend,
                    "trend_ar":  trend_ar,
                    "signal":    signal,
                    "extra":     extra,
                }
    except Exception as e:
        logger.warning(f"[E11/USDT.D] CoinGecko خطأ: {e}")

    # fallback
    return {
        "usdt_d": 7.0, "usdc_d": 2.0, "btc_d": 50, "stables": 9.0,
        "total_mc": 0, "total_vol": 0,
        "trend": "NEUTRAL", "trend_ar": "محايد 🟡 (بيانات غير متوفرة)",
        "signal": "NEUTRAL", "extra": "⚠️ فشل الاتصال بـ CoinGecko",
    }


# ══════════════════════════════════════════════════════════════════════
# تنسيق رسالة Telegram
# ══════════════════════════════════════════════════════════════════════

def _fmt_price(p: float) -> str:
    if p >= 10000: return f"${p:,.0f}"
    elif p >= 100: return f"${p:,.1f}"
    else:          return f"${p:,.3f}"


def _build_asset_section(sym: str, price: float, direction: str,
                          confidence: float, experts: Dict,
                          levels: Dict, atr_info: Dict,
                          proximity_info: Tuple[float, str],
                          corr_info: Optional[Dict] = None) -> str:
    """بناء قسم تحليل عملة واحدة للرسالة."""
    sym_c  = sym.replace("/USDT:USDT", "")
    icon   = "₿" if "BTC" in sym else "Ξ"
    dir_e  = "🟢 صاعد" if direction == "BULL" else "🔴 هابط" if direction == "BEAR" else "🟡 محايد"
    conf   = int(min(confidence * 100, 100))

    # تحذير ATR
    atr_warn = ""
    if not atr_info.get("tradeable", True):
        env_lbl = atr_info.get("label", "")
        atr_warn = f"\n  ⛔ <b>تحذير ATR:</b> {env_lbl} — لا يُنصح بالتداول"
    elif atr_info.get("env") in ("LOW", "HIGH"):
        atr_warn = f"\n  ⚠️ ATR: {atr_info.get('label','')}"

    # تحذير ارتباط
    corr_warn = ""
    if corr_info and not corr_info.get("aligned", True):
        corr_warn = f"\n  📡 {corr_info.get('label','')}"

    # معلومات الأهداف
    if direction == "BULL":
        targets = (
            f"   🎯 TP1: {_fmt_price(levels['tp1'])}\n"
            f"   🎯 TP2: {_fmt_price(levels['tp2'])}\n"
            f"   🎯 TP3: {_fmt_price(levels['tp3'])}"
        )
        supports = (
            f"   🛡️ S1: {_fmt_price(levels['sup1'])}  "
            f"S2: {_fmt_price(levels['sup2'])}  "
            f"S3: {_fmt_price(levels['sup3'])}"
        )
        targets_label  = "📈 أهداف الصعود:"
        supports_label = "🛡️ مستويات الدعم:"
    elif direction == "BEAR":
        targets = (
            f"   🎯 TP1: {_fmt_price(levels['tp1'])}\n"
            f"   🎯 TP2: {_fmt_price(levels['tp2'])}\n"
            f"   🎯 TP3: {_fmt_price(levels['tp3'])}"
        )
        supports = (
            f"   🚧 R1: {_fmt_price(levels['sup1'])}  "
            f"R2: {_fmt_price(levels['sup2'])}  "
            f"R3: {_fmt_price(levels['sup3'])}"
        )
        targets_label  = "📉 أهداف الهبوط:"
        supports_label = "🚧 مستويات المقاومة:"
    else:
        targets = (
            f"   📈 مقاومة: {_fmt_price(levels['tp1'])} / {_fmt_price(levels['tp2'])}\n"
            f"   📉 دعم:    {_fmt_price(levels['sup1'])} / {_fmt_price(levels['sup2'])}"
        )
        supports       = f"   ↔️ محور: {_fmt_price(levels['pp'])}"
        targets_label  = "📐 نطاق التداول:"
        supports_label = ""

    # نتائج الخبراء
    expert_icons = {
        "ClassicTA": "📊", "SMC": "🏛️", "Wyckoff": "⚖️",
        "Harmonic": "🎯", "Gann": "✨", "OBV": "📈", "Ichimoku": "☁️",
    }
    expert_lines = []
    for name, res in experts.items():
        ico    = expert_icons.get(name, "•")
        sig    = res.get("signal", "—")
        sig_s  = sig.replace(" 🟢","").replace(" 🔴","").replace(" 🟡","")
        if len(sig_s) > 20: sig_s = sig_s[:20] + "…"
        w_pct  = int(EXPERT_WEIGHTS.get(name, 0) * 100)
        expert_lines.append(
            f"  {ico} <b>{name}</b> ({w_pct}%): {res.get('emoji','')} {sig_s}"
        )
    experts_text = "\n".join(expert_lines)

    # Fibonacci
    fibs     = levels.get("fib", {})
    fib_line = (
        f"  38.2%: {_fmt_price(fibs.get('38.2',0))} | "
        f"61.8%: {_fmt_price(fibs.get('61.8',0))} 🌟"
    ) if fibs else ""

    # قرب المستويات
    prox_mult, prox_why = proximity_info
    prox_icon = "🎯" if prox_mult >= 1.25 else "📍" if prox_mult >= 1.0 else "💤"

    lines = [
        f"{'━'*32}",
        f"{icon} <b>{sym_c}/USDT</b>  —  {_fmt_price(price)}",
        f"📌 الاتجاه: <b>{dir_e}</b>  |  ثقة: <b>{conf}%</b>",
        f"📉 ATR: {atr_info.get('atr_pct',0):.2f}%  ({atr_info.get('label','—')}){atr_warn}{corr_warn}",
        f"{prox_icon} <b>التقاطع:</b> {prox_why}",
        f"",
        f"📋 <b>نتائج الخبراء (1D/4H/1H):</b>",
        experts_text,
        f"",
        f"📐 <b>Fibonacci:</b>",
        f"  نطاق: {_fmt_price(levels.get('lo',0))} ← {_fmt_price(levels.get('hi',0))}",
        fib_line,
        f"",
        f"<b>{targets_label}</b>",
        targets,
    ]
    if supports_label:
        lines += [f"", f"<b>{supports_label}</b>", supports]
    lines += [
        f"",
        f"  💹 POC: {_fmt_price(levels.get('poc',price))}  |  EMA50: {_fmt_price(levels.get('ema50',price))}",
    ]
    return "\n".join(l for l in lines if l is not None)


def build_telegram_message(btc_data: Dict, eth_data: Dict,
                            usdt_d: Dict, now_str: str) -> str:
    """بناء رسالة Telegram الكاملة."""
    btc_section = _build_asset_section(
        "BTC/USDT:USDT", btc_data["price"], btc_data["direction"],
        btc_data["confidence"], btc_data["experts"], btc_data["levels"],
        btc_data["atr"], btc_data["proximity"],
    )
    eth_section = _build_asset_section(
        "ETH/USDT:USDT", eth_data["price"], eth_data["direction"],
        eth_data["confidence"], eth_data["experts"], eth_data["levels"],
        eth_data["atr"], eth_data["proximity"],
        corr_info=eth_data.get("correlation"),
    )

    ud       = usdt_d.get("usdt_d", 0)
    btc_d    = usdt_d.get("btc_d", 0)
    sig11    = usdt_d.get("signal", "NEUTRAL")
    trend11  = usdt_d.get("trend_ar", "محايد 🟡")
    total_mc = usdt_d.get("total_mc", 0)
    total_b  = total_mc / 1e9 if total_mc else 0
    extra11  = usdt_d.get("extra", "")
    sig_e    = "🟢 فرص LONG" if sig11 == "LONG" else "🔴 فرص SHORT" if sig11 == "SHORT" else "🟡 انتظر"

    e11_section = (
        f"{'━'*32}\n"
        f"📡 <b>E11 — USDT Dominance (شارت USDT.D)</b>\n"
        f"  USDT.D: <b>{ud:.3f}%</b>  |  BTC.D: <b>{btc_d:.1f}%</b>\n"
        f"  حجم السوق الكلي: <b>${total_b:,.0f}B</b>\n"
        f"  {extra11}\n"
        f"\n"
        f"  الحالة: {trend11}\n"
        f"  📣 التوصية: <b>{sig_e}</b>"
    )

    # الخلاصة الموحّدة
    btc_ws = sum(e["score_weighted"] for e in btc_data["experts"].values())
    eth_ws = sum(e["score_weighted"] for e in eth_data["experts"].values())
    u_score = 0.15 if sig11 == "LONG" else -0.15 if sig11 == "SHORT" else 0
    total_ws = btc_ws + eth_ws + u_score

    # هل السوق قابل للتداول؟
    btc_tradeable = btc_data["atr"].get("tradeable", True)
    eth_tradeable = eth_data["atr"].get("tradeable", True)

    if not btc_tradeable:
        verdict = "⛔ BTC في بيئة غير قابلة للتداول — انتظر"
    elif total_ws >= 0.20:
        verdict = "🟢 السوق إيجابي — ابحث عن فرص LONG"
    elif total_ws <= -0.20:
        verdict = "🔴 السوق سلبي — ابحث عن فرص SHORT"
    else:
        verdict = "🟡 السوق محايد — انتظر تأكيداً"

    # توصية scalp/swing
    btc_atr_pct = btc_data["atr"].get("atr_pct", 2.0)
    if btc_atr_pct < 1.5:
        mode_rec = "⚡ مناسب لـ Scalp فقط (تذبذب منخفض)"
    elif btc_atr_pct > 4.0:
        mode_rec = "📊 مناسب لـ Swing فقط (تذبذب مرتفع)"
    else:
        mode_rec = "✅ مناسب لـ Scalp + Swing"

    msg = (
        f"📊 <b>التحليل اليومي — Ramos Ai 360 ♾️</b>\n"
        f"🕐 {now_str}\n"
        f"\n"
        f"{btc_section}\n"
        f"\n"
        f"{eth_section}\n"
        f"\n"
        f"{e11_section}\n"
        f"\n"
        f"{'━'*32}\n"
        f"🏁 <b>الخلاصة:</b> {verdict}\n"
        f"🎛️ <b>وضع التداول:</b> {mode_rec}\n"
        f"{'━'*32}\n"
        f"<i>🎖️ Ramos Ai 360 ♾️ — E10+E11 Analysis</i>"
    )
    return msg


# ══════════════════════════════════════════════════════════════════════
# الدالة الرئيسية
# ══════════════════════════════════════════════════════════════════════

async def run_full_analysis(db, notifier, fetcher) -> Dict:
    """
    يُشغّل التحليل الكامل لـ BTC + ETH + USDT.D
    يخزن النتيجة في Supabase ويرسل رسالة Telegram منظمة.
    """
    now     = datetime.now(timezone.utc)
    now_str = now.strftime("%d/%m/%Y %H:%M UTC")
    results: Dict = {}

    logger.info("[Analysis] 🔍 بدء التحليل الكامل …")

    # ── E11: جلب USDT.D أولاً (يؤثر على الخلاصة) ─────────────────
    usdt_d = await _fetch_usdt_dominance()
    logger.info(f"[E11] USDT.D={usdt_d['usdt_d']:.3f}% → {usdt_d['signal']}")

    # ── جلب BTC أولاً (يُستخدم لفحص الارتباط مع ETH) ─────────────
    btc_candles: Dict = {}
    for symbol in ["BTC/USDT:USDT", "ETH/USDT:USDT"]:
        sym_c = symbol.replace("/USDT:USDT", "")
        try:
            logger.info(f"[Analysis] تحليل {sym_c} …")

            c1d, c4h, c1h, c5m = await asyncio.gather(
                fetcher.get_candles(symbol, "1d", 60),
                fetcher.get_candles(symbol, "4h", 200),
                fetcher.get_candles(symbol, "1h", 100),
                fetcher.get_candles(symbol, "5m", 50),
            )

            if not c4h or len(c4h) < 20:
                logger.warning(f"[Analysis] {sym_c}: بيانات 4H غير كافية")
                continue

            price = float(c4h[0][4])

            # ── فلتر ATR ────────────────────────────────────────────
            atr_info = _atr_environment(c4h, price)
            logger.info(
                f"[ATR/{sym_c}] {atr_info['atr_pct']:.2f}% "
                f"({atr_info['env']}) — tradeable={atr_info['tradeable']}"
            )

            # ── تحليل الخبراء ────────────────────────────────────────
            experts = _expert_mini(c1d, c4h, c1h, c5m, price)

            # ── تقييم مُوزَّن نهائي ──────────────────────────────────
            weighted_sum = sum(e["score_weighted"] for e in experts.values())

            # تحديد الاتجاه بالنقاط المُوزَّنة (النطاق ±1.0)
            if weighted_sum >= 0.15:
                direction = "BULL"
            elif weighted_sum <= -0.15:
                direction = "BEAR"
            else:
                direction = "NEUTRAL"

            # الثقة = القيمة المطلقة للنقاط المُوزَّنة
            raw_confidence = min(abs(weighted_sum) / 0.5, 1.0)

            # ── حساب المستويات ───────────────────────────────────────
            levels = _calc_levels(c1d, c4h, c1h, price, direction)

            # ── فلتر تقارب المستويات ─────────────────────────────────
            proximity = _proximity_multiplier(price, levels, atr_info["atr_val"])
            prox_mult = proximity[0]
            confidence = min(raw_confidence * prox_mult * atr_info["score_mult"], 1.0)

            # ── فحص الارتباط مع BTC (لـ ETH فقط) ────────────────────
            corr_info: Optional[Dict] = None
            if sym_c == "ETH" and "BTC" in btc_candles:
                btc_cls = [float(c[4]) for c in btc_candles["c4h"][:30]]
                eth_cls = [float(c[4]) for c in c4h[:30]]
                corr_info = _btc_eth_correlation(btc_cls, eth_cls, 20)
                logger.info(f"[Corr BTC-ETH] {corr_info['label']}")
                # إذا كان منفصلاً عن BTC، نخفض الثقة
                if not corr_info["aligned"]:
                    confidence *= 0.75

            # حفظ بيانات BTC للاستخدام في فحص الارتباط
            if sym_c == "BTC":
                btc_candles["c4h"] = c4h

            # ── Regime ──────────────────────────────────────────────
            regime = IE.get_market_regime(c4h, c1h)

            results[sym_c] = {
                "price":       price,
                "direction":   direction,
                "confidence":  confidence,
                "experts":     experts,
                "levels":      levels,
                "regime":      regime,
                "score":       weighted_sum,
                "atr":         atr_info,
                "proximity":   proximity,
                "correlation": corr_info,
            }

            # حفظ في Supabase
            await db.log_regime(symbol, direction, round(confidence, 3))
            logger.info(
                f"[Analysis] {sym_c}: {direction} "
                f"ثقة={int(confidence*100)}% "
                f"ATR={atr_info['atr_pct']:.2f}% "
                f"Prox×{prox_mult:.2f}"
            )

        except Exception as e:
            logger.error(f"[Analysis] {sym_c} خطأ: {e}")

    # ── بناء وإرسال الرسالة ────────────────────────────────────────
    if "BTC" in results and "ETH" in results:
        msg = build_telegram_message(
            results["BTC"], results["ETH"], usdt_d, now_str
        )
        await notifier.send(msg)
        logger.success("[Analysis] ✅ رسالة Telegram أُرسلت")

    elif "BTC" in results:
        btc = results["BTC"]
        msg = (
            f"📊 <b>تحليل BTC</b> — {now_str}\n"
            f"السعر: {_fmt_price(btc['price'])} | "
            f"الاتجاه: {btc['direction']} | "
            f"ثقة: {int(btc['confidence']*100)}%\n"
            f"ATR: {btc['atr']['atr_pct']:.2f}% ({btc['atr']['label']})\n"
            f"ETH: بيانات غير متوفرة حالياً"
        )
        await notifier.send(msg)

    else:
        await notifier.send(f"⚠️ التحليل: لم تُجلب بيانات كافية — {now_str}")

    results["USDT_D"] = usdt_d
    return results

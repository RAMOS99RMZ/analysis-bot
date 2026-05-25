# strategies/expert_10_daily.py — E10: Market Core Analyzer (UPGRADED v2)
# ═══════════════════════════════════════════════════════════════════════════
# يحلل BTC + ETH على 1D / 4H / 1H باستخدام:
#   ✅ Swing High/Low الحقيقي (أعلى قمة / أدنى قاع مطلق)
#   ✅ SMC: Order Blocks, FVG, CHoCH, Liquidity Sweep
#   ✅ Gann: Square of 9, Gann Angles من القمم والقيعان
#   ✅ Harmonic Patterns: Gartley, Bat, Butterfly
#   ✅ Classic TA: RSI, MACD, OBV, Volume Profile
#   ✅ CME Gap بـ weekday() حقيقي
#   ✅ ATR Filter + Level Proximity
#   ✅ يخزن النتيجة في market_state للخبراء الآخرين
# ═══════════════════════════════════════════════════════════════════════════
from __future__ import annotations
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from engine.indicator_engine import IndicatorEngine
from engine.market_state import update_e10
from utils.helpers import calc_volume_profile, utc_weekday

IE = IndicatorEngine()


# ──────────────────────────────────────────────────────────────────────────
# A. البحث عن القمم والقيعان المطلقة (Swing High/Low)
# ──────────────────────────────────────────────────────────────────────────
def _find_swing_points(candles: List, lookback: int = None) -> Dict:
    """
    يبحث عن:
    - أعلى قمة مطلقة (Absolute High) في الفترة
    - أدنى قاع مطلق (Absolute Low) في الفترة
    - آخر قمة محلية (Local High) — أعلى نقطة في آخر 20 شمعة
    - آخر قاع محلية (Local Low) — أدنى نقطة في آخر 20 شمعة
    """
    if not candles or len(candles) < 5:
        return {}
    n = lookback or len(candles)
    c = candles[:n]

    abs_high_val = max(float(x[2]) for x in c)
    abs_low_val  = min(float(x[3]) for x in c)
    abs_high_i   = next(i for i, x in enumerate(c) if float(x[2]) == abs_high_val)
    abs_low_i    = next(i for i, x in enumerate(c) if float(x[3]) == abs_low_val)

    # قمة وقاع محليان (آخر 20 شمعة)
    local_n = min(20, len(c))
    lc = c[:local_n]
    local_high = max(float(x[2]) for x in lc)
    local_low  = min(float(x[3]) for x in lc)

    return {
        "abs_high": abs_high_val,
        "abs_low":  abs_low_val,
        "abs_high_bars_ago": abs_high_i,
        "abs_low_bars_ago":  abs_low_i,
        "local_high": local_high,
        "local_low":  local_low,
        "range": abs_high_val - abs_low_val,
    }


# ──────────────────────────────────────────────────────────────────────────
# B. SMC Analysis
# ──────────────────────────────────────────────────────────────────────────
def _smc_analysis(candles: List, price: float) -> Dict:
    """
    يستخرج:
    - Order Blocks (OB): آخر شمعة هابطة قبل دافع صاعد / آخر صاعدة قبل هابط
    - Fair Value Gaps (FVG): فجوات السعر بين الشمعة 1 والشمعة 3
    - CHoCH: كسر هيكل السوق
    - Liquidity Sweep: اختراق قمة/قاع ثم رجوع
    """
    result = {
        "bull_ob": None, "bear_ob": None,
        "fvg_bull": [], "fvg_bear": [],
        "choch": None, "liq_sweep": None,
        "score": 0.0,
    }
    if not candles or len(candles) < 10:
        return result

    try:
        # ── Order Blocks ──────────────────────────────────────────────
        for i in range(2, min(len(candles) - 1, 30)):
            curr = candles[i]
            prev = candles[i - 1]
            next_c = candles[i - 2] if i >= 2 else None
            if not next_c:
                continue

            c_open  = float(curr[1]); c_high = float(curr[2])
            c_low   = float(curr[3]); c_close = float(curr[4])
            n_close = float(next_c[4])

            is_bearish = c_close < c_open    # شمعة هابطة
            is_bullish = c_close > c_open    # شمعة صاعدة

            # Bullish OB: آخر شمعة هابطة قبل اندفاع صاعد
            if is_bearish and n_close > c_high and result["bull_ob"] is None:
                result["bull_ob"] = {
                    "high": c_high, "low": c_low,
                    "mid": (c_high + c_low) / 2,
                    "bars_ago": i,
                }

            # Bearish OB: آخر شمعة صاعدة قبل اندفاع هابط
            if is_bullish and n_close < c_low and result["bear_ob"] is None:
                result["bear_ob"] = {
                    "high": c_high, "low": c_low,
                    "mid": (c_high + c_low) / 2,
                    "bars_ago": i,
                }

            if result["bull_ob"] and result["bear_ob"]:
                break

        # ── Fair Value Gaps ───────────────────────────────────────────
        for i in range(1, min(len(candles) - 1, 20)):
            h0 = float(candles[i - 1][2]); l0 = float(candles[i - 1][3])
            h2 = float(candles[i + 1][2]); l2 = float(candles[i + 1][3])
            c1l = float(candles[i][3]);    c1h = float(candles[i][2])

            # Bullish FVG: فجوة صاعدة (low[i+1] > high[i-1])
            if l2 > h0 and len(result["fvg_bull"]) < 3:
                result["fvg_bull"].append({
                    "top": l2, "bot": h0,
                    "mid": (l2 + h0) / 2, "bars_ago": i
                })

            # Bearish FVG: فجوة هابطة (high[i+1] < low[i-1])
            if h2 < l0 and len(result["fvg_bear"]) < 3:
                result["fvg_bear"].append({
                    "top": l0, "bot": h2,
                    "mid": (l0 + h2) / 2, "bars_ago": i
                })

        # ── CHoCH (Change of Character) ───────────────────────────────
        if len(candles) >= 15:
            highs = [float(c[2]) for c in candles[:15]]
            lows  = [float(c[3]) for c in candles[:15]]
            prev_high = max(highs[1:])
            prev_low  = min(lows[1:])

            if float(candles[0][4]) > prev_high:
                result["choch"] = "BULL"     # كسر هيكل صاعد
            elif float(candles[0][4]) < prev_low:
                result["choch"] = "BEAR"     # كسر هيكل هابط

        # ── Liquidity Sweep ───────────────────────────────────────────
        if len(candles) >= 5:
            recent_high = max(float(c[2]) for c in candles[1:6])
            recent_low  = min(float(c[3]) for c in candles[1:6])
            curr_high   = float(candles[0][2])
            curr_low    = float(candles[0][3])
            curr_close  = float(candles[0][4])

            if curr_high > recent_high and curr_close < recent_high:
                result["liq_sweep"] = "BEAR"  # sweep القمة ثم رجع
            elif curr_low < recent_low and curr_close > recent_low:
                result["liq_sweep"] = "BULL"  # sweep القاع ثم رجع

        # ── حساب score SMC ────────────────────────────────────────────
        score = 0.0
        # قرب السعر من OB
        if result["bull_ob"]:
            ob = result["bull_ob"]
            if ob["low"] <= price <= ob["high"] * 1.005:
                score += 0.30   # داخل Bullish OB
        if result["bear_ob"]:
            ob = result["bear_ob"]
            if ob["low"] * 0.995 <= price <= ob["high"]:
                score -= 0.30   # داخل Bearish OB

        # FVG
        for fvg in result["fvg_bull"][:1]:
            if fvg["bot"] <= price <= fvg["top"]:
                score += 0.20
        for fvg in result["fvg_bear"][:1]:
            if fvg["bot"] <= price <= fvg["top"]:
                score -= 0.20

        # CHoCH
        if result["choch"] == "BULL":   score += 0.25
        elif result["choch"] == "BEAR": score -= 0.25

        # Liquidity Sweep
        if result["liq_sweep"] == "BULL":  score += 0.15
        elif result["liq_sweep"] == "BEAR": score -= 0.15

        result["score"] = round(max(-1.0, min(1.0, score)), 4)

    except Exception:
        pass

    return result


# ──────────────────────────────────────────────────────────────────────────
# C. Gann Mathematics
# ──────────────────────────────────────────────────────────────────────────
def _gann_sq9_levels(price: float, n_levels: int = 8) -> List[float]:
    """
    Square of 9: يحسب مستويات Gann القريبة من السعر الحالي.
    الخوارزمية: √price → أضف 0.125 (45°) أو مضاعفاتها → تربيع
    """
    if price <= 0:
        return []
    try:
        sqrt_p = math.sqrt(price)
        levels = []
        # كل 45° = 0.125 وحدة على sqrt
        for step in range(-4, 5):
            level = (sqrt_p + step * 0.125) ** 2
            if level > 0:
                levels.append(round(level, 4))
        return sorted(levels)
    except Exception:
        return []


def _gann_sq52_levels(price: float) -> List[float]:
    """
    Square of 52 (52 أسبوعاً): مستويات بناءً على 52 وحدة.
    يُقسَّم النطاق السنوي على 52 للحصول على وحدة الحركة.
    """
    try:
        unit = price * 0.01923   # 1/52 ≈ 1.923%
        return [round(price + unit * i, 4) for i in range(-4, 5)]
    except Exception:
        return []


def _gann_angles(high: float, low: float, bars_since: int,
                 price: float, is_from_low: bool) -> Dict:
    """
    زوايا Gann من قاع أو قمة:
    1×1 (45°): يتحرك وحدة سعر لكل شمعة
    2×1 (63°): وحدتان لكل شمعة
    1×2 (26°): نصف وحدة لكل شمعة
    """
    try:
        rng = high - low
        if rng <= 0 or bars_since <= 0:
            return {}

        unit = rng / max(bars_since, 1)

        if is_from_low:
            base = low
            angles = {
                "1x1":  round(base + unit       * bars_since, 4),
                "2x1":  round(base + unit * 2   * bars_since, 4),
                "1x2":  round(base + unit * 0.5 * bars_since, 4),
                "4x1":  round(base + unit * 4   * bars_since, 4),
                "1x4":  round(base + unit * 0.25* bars_since, 4),
            }
        else:  # from high
            base = high
            angles = {
                "1x1":  round(base - unit       * bars_since, 4),
                "2x1":  round(base - unit * 2   * bars_since, 4),
                "1x2":  round(base - unit * 0.5 * bars_since, 4),
                "4x1":  round(base - unit * 4   * bars_since, 4),
                "1x4":  round(base - unit * 0.25* bars_since, 4),
            }

        # تحديد أقرب زاوية للسعر الحالي
        nearest = min(angles.items(), key=lambda x: abs(x[1] - price))

        return {
            "angles": angles,
            "nearest_angle": nearest[0],
            "nearest_level": nearest[1],
            "distance_pct": round(abs(nearest[1] - price) / price * 100, 3),
        }
    except Exception:
        return {}


def _gann_score(candles: List, price: float, swings: Dict) -> Tuple[float, Dict]:
    """يحسب نقاط Gann الإجمالية من Square of 9 + Angles."""
    info = {}
    score = 0.0
    try:
        # Square of 9
        sq9 = _gann_sq9_levels(price)
        if sq9:
            nearest_sq9 = min(sq9, key=lambda x: abs(x - price))
            dist_pct = abs(nearest_sq9 - price) / price * 100
            info["sq9_nearest"]   = nearest_sq9
            info["sq9_dist_pct"]  = round(dist_pct, 3)
            info["sq9_levels"]    = sq9
            if dist_pct < 0.3:
                score += 0.15 if nearest_sq9 > price else -0.10
            elif dist_pct < 0.8:
                score += 0.08 if nearest_sq9 > price else -0.05

        # Square of 52
        sq52 = _gann_sq52_levels(price)
        if sq52:
            nearest_sq52 = min(sq52, key=lambda x: abs(x - price))
            info["sq52_nearest"] = nearest_sq52

        # Gann Angles
        if swings.get("abs_high") and swings.get("abs_low"):
            ang_from_low = _gann_angles(
                swings["abs_high"], swings["abs_low"],
                swings.get("abs_low_bars_ago", 20),
                price, is_from_low=True
            )
            ang_from_high = _gann_angles(
                swings["abs_high"], swings["abs_low"],
                swings.get("abs_high_bars_ago", 20),
                price, is_from_low=False
            )
            info["gann_from_low"]  = ang_from_low
            info["gann_from_high"] = ang_from_high

            # إذا السعر فوق زاوية 1x1 من القاع → قوة صاعدة
            angle_1x1_low  = ang_from_low.get("angles", {}).get("1x1", 0)
            angle_1x1_high = ang_from_high.get("angles", {}).get("1x1", float("inf"))
            if angle_1x1_low > 0 and price > angle_1x1_low:
                score += 0.20
                info["gann_signal"] = "فوق زاوية 1x1 من القاع 📈"
            elif angle_1x1_high < float("inf") and price < angle_1x1_high:
                score -= 0.20
                info["gann_signal"] = "تحت زاوية 1x1 من القمة 📉"

    except Exception:
        pass
    return round(max(-1.0, min(1.0, score)), 4), info


# ──────────────────────────────────────────────────────────────────────────
# D. Harmonic Patterns (Gartley, Bat, Butterfly)
# ──────────────────────────────────────────────────────────────────────────
def _find_pivot_points(candles: List, n: int = 5) -> List[Dict]:
    """يجد نقاط الانعكاس (Pivot Points) المحلية."""
    pivots = []
    candles = candles[:60]   # نبحث في آخر 60 شمعة
    for i in range(n, len(candles) - n):
        high_i = float(candles[i][2])
        low_i  = float(candles[i][3])

        # Local High
        if all(high_i >= float(candles[i-j][2]) for j in range(1, n+1)) and \
           all(high_i >= float(candles[i+j][2]) for j in range(1, n+1)):
            pivots.append({"type": "H", "price": high_i, "idx": i})

        # Local Low
        if all(low_i <= float(candles[i-j][3]) for j in range(1, n+1)) and \
           all(low_i <= float(candles[i+j][3]) for j in range(1, n+1)):
            pivots.append({"type": "L", "price": low_i, "idx": i})

    return sorted(pivots, key=lambda x: x["idx"])


def _check_harmonic(X, A, B, C, D) -> Optional[str]:
    """
    يفحص نسب Fibonacci للنماذج التوافقية.
    يُرجع اسم النموذج أو None.
    """
    try:
        XA = abs(A - X)
        AB = abs(B - A)
        BC = abs(C - B)
        CD = abs(D - C)
        AD = abs(D - A)
        if XA == 0 or AB == 0 or BC == 0:
            return None

        ab_xa = AB / XA
        bc_ab = BC / AB
        cd_bc = CD / BC if BC > 0 else 0
        ad_xa = AD / XA

        # Gartley
        if (0.58 <= ab_xa <= 0.65 and
                0.35 <= bc_ab <= 0.90 and
                0.75 <= ad_xa <= 0.80):
            return "Gartley"

        # Bat
        if (0.35 <= ab_xa <= 0.52 and
                0.35 <= bc_ab <= 0.90 and
                0.84 <= ad_xa <= 0.90):
            return "Bat"

        # Butterfly
        if (0.75 <= ab_xa <= 0.80 and
                0.35 <= bc_ab <= 0.90 and
                1.25 <= ad_xa <= 1.65):
            return "Butterfly"

        # Crab
        if (0.35 <= ab_xa <= 0.62 and
                0.35 <= bc_ab <= 0.90 and
                1.55 <= ad_xa <= 1.65):
            return "Crab"

    except Exception:
        pass
    return None


def _harmonic_score(candles: List, price: float) -> Tuple[float, str]:
    """يبحث عن نماذج توافقية ويُرجع نقطة واسم النموذج."""
    try:
        pivots = _find_pivot_points(candles, n=3)
        if len(pivots) < 5:
            return 0.0, "لا نموذج"

        # جرّب آخر 5 نقاط انعكاس
        for i in range(len(pivots) - 4):
            pts = pivots[i:i+5]
            prices = [p["price"] for p in pts]
            X, A, B, C, D = prices

            name = _check_harmonic(X, A, B, C, D)
            if name:
                # هل D قريب من السعر الحالي؟
                if abs(D - price) / price < 0.015:
                    # تحديد الاتجاه من النموذج
                    bullish = D < A   # D أدنى من A → صعود متوقع
                    score   = 0.25 if bullish else -0.25
                    return score, f"نموذج {name} {'صاعد 🟢' if bullish else 'هابط 🔴'}"

    except Exception:
        pass
    return 0.0, "لا نموذج"


# ──────────────────────────────────────────────────────────────────────────
# E. CME Gap
# ──────────────────────────────────────────────────────────────────────────
def _detect_cme_gap(c1d: List) -> Dict:
    try:
        if not c1d or len(c1d) < 5:
            return {"has_gap": False}
        friday_close = monday_open = None
        for candle in c1d[:14]:
            ts = int(candle[0]) / 1000
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            wd = dt.weekday()
            if wd == 4 and friday_close is None:
                friday_close = float(candle[4])
            elif wd == 0 and monday_open is None:
                monday_open  = float(candle[1])
            if friday_close and monday_open:
                break
        if not friday_close or not monday_open:
            return {"has_gap": False}
        gap_pct = (monday_open - friday_close) / friday_close
        if abs(gap_pct) > 0.005:
            return {"has_gap": True, "gap_pct": round(gap_pct, 4),
                    "gap_fill": friday_close, "bullish": gap_pct < 0}
        return {"has_gap": False}
    except Exception:
        return {"has_gap": False}


# ──────────────────────────────────────────────────────────────────────────
# الدالة الرئيسية لـ E10
# ──────────────────────────────────────────────────────────────────────────
def analyze(data: Dict) -> Optional[Dict]:
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

        # ── A. ATR Filter ─────────────────────────────────────────────
        try:
            atr_val = IE.atr(c4h, 14)
            atr_pct = (atr_val / price) * 100 if price > 0 else 2.0
        except Exception:
            atr_val, atr_pct = 1.0, 2.0

        if atr_pct < 0.5 or atr_pct > 7.0:
            atr_label = f"{'راكد' if atr_pct<0.5 else 'متطرف'} ({atr_pct:.2f}%)"
            return {"name": "Daily", "long": 0.0, "short": 0.0,
                    "why": {"atr": atr_label}, "trade_type": "NONE",
                    "atr_label": atr_label}

        atr_mult  = 1.0 if atr_pct <= 4.0 else 0.7
        atr_label = f"ATR={atr_pct:.2f}% {'✅' if atr_mult==1.0 else '⚠️'}"
        why["atr"] = atr_label

        # ── B. Swing High/Low المطلق ──────────────────────────────────
        swings_4h = _find_swing_points(c4h, lookback=200)
        swings_1d = _find_swing_points(c1d) if c1d else {}
        why["swings"] = (
            f"4H: H={swings_4h.get('abs_high', 0):,.0f} "
            f"L={swings_4h.get('abs_low', 0):,.0f}"
        )

        # الترند العام: هل السعر أقرب للقمة أم للقاع؟
        if swings_4h.get("range", 0) > 0:
            position = (price - swings_4h["abs_low"]) / swings_4h["range"]
            if position > 0.65:
                long_s  += 0.15; why["position"] = f"سعر في الثلث العلوي ({position:.0%}) ↑"
            elif position < 0.35:
                short_s += 0.15; why["position"] = f"سعر في الثلث السفلي ({position:.0%}) ↓"

        # ── C. Classic TA ─────────────────────────────────────────────
        if c1w and len(c1w) >= 4:
            wt = IE.get_trend(c1w)
            if wt == "BULL":   long_s  += 0.20; why["weekly"] = "Weekly BULL ↑"
            elif wt == "BEAR": short_s += 0.20; why["weekly"] = "Weekly BEAR ↓"

        if c1d and len(c1d) >= 20:
            dt = IE.get_trend(c1d)
            if dt == "BULL":   long_s  += 0.20; why["daily"] = "Daily BULL"
            elif dt == "BEAR": short_s += 0.20; why["daily"] = "Daily BEAR"

        # RSI
        try:
            rsi = IE.rsi(c4h, 14)
            if rsi < 40:   short_s -= 0.10; long_s  += 0.10; why["rsi"] = f"RSI={rsi:.0f} (ذروة بيع)"
            elif rsi > 65: long_s  -= 0.10; short_s += 0.10; why["rsi"] = f"RSI={rsi:.0f} (ذروة شراء)"
        except Exception: pass

        # MACD
        try:
            macd_d = IE.macd(c4h)
            if macd_d and macd_d.get("hist", 0) > 0:
                long_s  += 0.10; why["macd"] = "MACD إيجابي"
            elif macd_d:
                short_s += 0.10; why["macd"] = "MACD سلبي"
        except Exception: pass

        # OBV
        try:
            obv_d = IE.obv(c4h)
            if obv_d:
                if obv_d.get("rising"):   long_s  += 0.10; why["obv"] = "OBV صاعد 📈"
                elif obv_d.get("falling"):short_s += 0.10; why["obv"] = "OBV هابط 📉"
        except Exception: pass

        # Volume Profile POC
        try:
            vp  = calc_volume_profile(c4h[:60], 15)
            poc = vp.get("poc", 0)
            if poc > 0:
                if price > poc * 1.003:
                    long_s  += 0.10; why["poc"] = f"فوق POC ${poc:,.0f}"
                elif price < poc * 0.997:
                    short_s += 0.10; why["poc"] = f"تحت POC ${poc:,.0f}"
        except Exception: pass

        # ── D. SMC Analysis ───────────────────────────────────────────
        smc = _smc_analysis(c4h, price)
        smc_sc = smc.get("score", 0)
        if smc_sc > 0:
            long_s  += smc_sc * 0.30
            why["smc"] = f"SMC Bull: CHoCH={smc.get('choch')} Sweep={smc.get('liq_sweep')}"
        elif smc_sc < 0:
            short_s += abs(smc_sc) * 0.30
            why["smc"] = f"SMC Bear: CHoCH={smc.get('choch')} Sweep={smc.get('liq_sweep')}"

        # ── E. Gann Mathematics ───────────────────────────────────────
        gann_sc, gann_info = _gann_score(c4h, price, swings_4h)
        if gann_sc > 0:
            long_s  += gann_sc * 0.20
            why["gann"] = gann_info.get("gann_signal", f"Gann+{gann_sc:.2f}")
        elif gann_sc < 0:
            short_s += abs(gann_sc) * 0.20
            why["gann"] = gann_info.get("gann_signal", f"Gann{gann_sc:.2f}")

        # ── F. Harmonic Patterns ──────────────────────────────────────
        harm_sc, harm_name = _harmonic_score(c4h, price)
        if harm_sc != 0:
            if harm_sc > 0:  long_s  += harm_sc; why["harmonic"] = harm_name
            else:            short_s += abs(harm_sc); why["harmonic"] = harm_name

        # ── G. CME Gap ────────────────────────────────────────────────
        gap = _detect_cme_gap(c1d)
        if gap.get("has_gap"):
            if gap["bullish"]: long_s  += 0.20; why["cme"] = f"CME Gap صاعد {gap['gap_pct']:.2%}"
            else:              short_s += 0.20; why["cme"] = f"CME Gap هابط {gap['gap_pct']:.2%}"

        # ── H. 4H Regime + EMA ────────────────────────────────────────
        try:
            reg = IE.get_market_regime(c4h, c1h)
            if reg.get("bull_align"):   long_s  += 0.15; why["regime"] = "EMA صاعدة 4H"
            elif reg.get("bear_align"): short_s += 0.15; why["regime"] = "EMA هابطة 4H"
        except Exception: pass

        # ── تطبيق ATR كمضاعف ─────────────────────────────────────────
        long_s  = round(min(long_s  * atr_mult, 1.0), 4)
        short_s = round(min(short_s * atr_mult, 1.0), 4)

        # trade_type
        if atr_pct < 1.5:   trade_type = "SCALP"
        elif atr_pct > 4.0: trade_type = "SWING"
        else:               trade_type = "SCALP+SWING"

        result = {
            "name":       "Daily",
            "long":       long_s,
            "short":      short_s,
            "why":        why,
            "trade_type": trade_type,
            "atr_label":  atr_label,
            "swings":     swings_4h,
            "smc":        smc,
            "gann":       gann_info,
        }

        # ✅ تخزين النتيجة في market_state
        update_e10(result)
        return result

    except Exception as e:
        return {"name": "Daily", "long": 0.0, "short": 0.0,
                "why": {"err": str(e)}, "trade_type": "UNKNOWN", "atr_label": "—"}

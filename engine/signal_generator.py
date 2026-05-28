# engine/signal_generator.py — Enhanced with 7 new features
# ══════════════════════════════════════════════════════════════════
# الميزات المُضافة:
#   1. VCE  — Volume Confirmed Entry
#   2. HH/LL Engine — Higher High / Lower Low
#   3. Divergence — RSI + OBV
#   4. Funding Rate Extremes
#   5. OFI — Order Flow Imbalance
#   6. Liquidity Hunt Detection
#   7. Adaptive Threshold Learning
# ══════════════════════════════════════════════════════════════════
from __future__ import annotations
import uuid
from typing import Dict, List, Optional, Tuple
from loguru import logger

from config import CONFIG, ASSET_VOLATILITY, VOL_THRESHOLDS
from strategies import EXPERTS, EXPERT_NAMES, EXPERT_WEIGHTS
from engine.indicator_engine import IndicatorEngine
from engine.risk_manager import (
    calc_sltp, calc_position_size, get_trade_type,
    atr_pct_check, check_price_drift, portfolio_risk_check, get_obi_signal
)
from utils.helpers import (
    was_recently_sent, mark_sent, check_re_entry,
    record_sl_hit, get_session, is_opportunity_window
)

IE = IndicatorEngine()

# ── Adaptive Threshold Storage (في الذاكرة — يتحدث كل أسبوع) ────────────
_ADAPTIVE_THRESHOLDS: Dict[str, float] = {}   # symbol → adjusted_min_score
_PERFORMANCE_HISTORY: Dict[str, List[float]] = {}  # symbol → [pnl_pct, ...]


# ══════════════════════════════════════════════════════════════════
# 1. VCE — Volume Confirmed Entry
# ══════════════════════════════════════════════════════════════════
def _volume_confirmed(c4h: List, multiplier: float = 1.3) -> bool:
    """
    يتحقق أن حجم الشمعة الحالية أعلى من المتوسط.
    نستخدم 1.3× (أقل من 1.5 الصارم) لعدم حذف إشارات جيدة.
    """
    try:
        if not c4h or len(c4h) < 22:
            return True   # بيانات غير كافية → لا نحجب
        avg_vol = sum(float(c[5]) for c in c4h[1:21]) / 20
        curr_vol = float(c4h[0][5])
        if avg_vol <= 0:
            return True
        result = curr_vol >= avg_vol * multiplier
        if not result:
            logger.debug(f"[VCE] حجم ضعيف: {curr_vol:.0f} < {avg_vol*multiplier:.0f}")
        return result
    except Exception:
        return True


# ══════════════════════════════════════════════════════════════════
# 2. HH/HL/LH/LL Engine
# ══════════════════════════════════════════════════════════════════
def _hh_ll_trend(candles: List, n: int = 5, lookback: int = 20) -> Dict:
    """
    يتتبع Higher High / Higher Low / Lower High / Lower Low
    لتحديد الاتجاه بدقة رياضية.

    HH + HL = اتجاه صاعد مؤكد
    LH + LL = اتجاه هابط مؤكد
    """
    try:
        if not candles or len(candles) < lookback:
            return {"trend": "UNKNOWN", "score": 0.0, "pattern": "—"}

        # استخراج القمم والقيعان المحلية
        highs: List[Tuple[int, float]] = []
        lows:  List[Tuple[int, float]] = []

        for i in range(n, min(len(candles) - n, lookback)):
            h_i = float(candles[i][2])
            l_i = float(candles[i][3])

            # Local High: أعلى من الجيران
            if all(h_i >= float(candles[i-j][2]) for j in range(1, n+1)) and \
               all(h_i >= float(candles[i+j][2]) for j in range(1, n+1)):
                highs.append((i, h_i))

            # Local Low: أقل من الجيران
            if all(l_i <= float(candles[i-j][3]) for j in range(1, n+1)) and \
               all(l_i <= float(candles[i+j][3]) for j in range(1, n+1)):
                lows.append((i, l_i))

        if len(highs) < 2 or len(lows) < 2:
            return {"trend": "UNKNOWN", "score": 0.0, "pattern": "بيانات غير كافية"}

        # فرز بالترتيب الزمني (الأحدث أولاً)
        highs = sorted(highs, key=lambda x: x[0])
        lows  = sorted(lows,  key=lambda x: x[0])

        last_h, prev_h = highs[-1][1], highs[-2][1]
        last_l, prev_l = lows[-1][1],  lows[-2][1]

        hh = last_h > prev_h   # Higher High
        hl = last_l > prev_l   # Higher Low
        lh = last_h < prev_h   # Lower High
        ll = last_l < prev_l   # Lower Low

        if hh and hl:
            return {"trend": "BULL", "score": 0.35,
                    "pattern": f"HH({last_h:.0f}) + HL({last_l:.0f}) ↑"}
        elif hh and ll:
            return {"trend": "MIXED_BULL", "score": 0.15,
                    "pattern": f"HH + LL (اتجاه غير حاسم)"}
        elif lh and ll:
            return {"trend": "BEAR", "score": -0.35,
                    "pattern": f"LH({last_h:.0f}) + LL({last_l:.0f}) ↓"}
        elif lh and hl:
            return {"trend": "MIXED_BEAR", "score": -0.15,
                    "pattern": f"LH + HL (اتجاه غير حاسم)"}
        else:
            return {"trend": "RANGE", "score": 0.0, "pattern": "سوق جانبي"}

    except Exception as e:
        return {"trend": "UNKNOWN", "score": 0.0, "pattern": str(e)}


# ══════════════════════════════════════════════════════════════════
# 3. Divergence Detection (RSI + OBV)
# ══════════════════════════════════════════════════════════════════
def _detect_divergence(candles: List, direction: str,
                       lookback: int = 20) -> Dict:
    """
    يكتشف:
    - RSI Divergence: سعر يصنع قمة جديدة لكن RSI لا يتبعه
    - OBV Divergence: سعر يرتفع لكن OBV يهبط
    """
    try:
        if not candles or len(candles) < lookback:
            return {"rsi_div": None, "obv_div": None, "score": 0.0}

        closes = [float(c[4]) for c in candles[:lookback]]
        highs  = [float(c[2]) for c in candles[:lookback]]
        lows   = [float(c[3]) for c in candles[:lookback]]
        vols   = [float(c[5]) for c in candles[:lookback]]

        # ── RSI ──────────────────────────────────────────────────────
        period = 14
        rsi_vals: List[float] = []
        gains = [max(closes[i-1]-closes[i], 0) for i in range(1, len(closes))]
        losses= [max(closes[i]-closes[i-1], 0) for i in range(1, len(closes))]
        if len(gains) >= period:
            avg_g = sum(gains[:period]) / period
            avg_l = sum(losses[:period]) / period
            for i in range(period, len(gains)):
                avg_g = (avg_g * (period-1) + gains[i]) / period
                avg_l = (avg_l * (period-1) + losses[i]) / period
            rs = avg_g / avg_l if avg_l > 0 else 100
            rsi_now = 100 - 100 / (1 + rs)
        else:
            rsi_now = 50

        # RSI قبل 10 شمعات
        candles_old = candles[10:lookback]
        closes_old  = [float(c[4]) for c in candles_old]
        rsi_old = 50.0
        if len(closes_old) >= period:
            gains_old  = [max(closes_old[i-1]-closes_old[i], 0) for i in range(1, len(closes_old))]
            losses_old = [max(closes_old[i]-closes_old[i-1], 0) for i in range(1, len(closes_old))]
            if len(gains_old) >= period:
                ag = sum(gains_old[:period]) / period
                al = sum(losses_old[:period]) / period
                rs2 = ag / al if al > 0 else 100
                rsi_old = 100 - 100 / (1 + rs2)

        # ── OBV ──────────────────────────────────────────────────────
        obv = 0.0
        obv_vals = [0.0]
        for i in range(1, len(closes)):
            if closes[i-1] > closes[i]:    obv += vols[i]
            elif closes[i-1] < closes[i]:  obv -= vols[i]
            obv_vals.append(obv)

        obv_now = obv_vals[-1]  if obv_vals else 0
        obv_old = obv_vals[len(obv_vals)//2] if obv_vals else 0

        # ── تحليل الـ Divergence ──────────────────────────────────────
        price_up   = closes[0] > closes[10]    # السعر ارتفع
        price_down = closes[0] < closes[10]    # السعر انخفض
        rsi_up     = rsi_now > rsi_old
        rsi_down   = rsi_now < rsi_old
        obv_up     = obv_now > obv_old
        obv_down   = obv_now < obv_old

        rsi_div = None
        obv_div = None
        score   = 0.0

        # Bearish RSI Divergence: سعر↑ RSI↓
        if price_up and rsi_down and rsi_now > 60:
            rsi_div = "BEARISH"
            score  -= 0.25
        # Bullish RSI Divergence: سعر↓ RSI↑
        elif price_down and rsi_up and rsi_now < 45:
            rsi_div = "BULLISH"
            score  += 0.25

        # Bearish OBV Divergence: سعر↑ OBV↓
        if price_up and obv_down:
            obv_div = "BEARISH"
            score  -= 0.20
        # Bullish OBV Divergence: سعر↓ OBV↑
        elif price_down and obv_up:
            obv_div = "BULLISH"
            score  += 0.20

        return {
            "rsi_div": rsi_div, "obv_div": obv_div,
            "rsi_now": round(rsi_now, 1), "rsi_old": round(rsi_old, 1),
            "score":   round(max(-0.45, min(0.45, score)), 4),
        }

    except Exception as e:
        return {"rsi_div": None, "obv_div": None, "score": 0.0}


# ══════════════════════════════════════════════════════════════════
# 4. Funding Rate Extremes
# ══════════════════════════════════════════════════════════════════
def _funding_extreme_signal(funding: Dict, direction: str) -> Tuple[float, str]:
    """
    funding > +0.15% → تشبع Long → SHORT قوي
    funding < -0.10% → تشبع Short → LONG قوي
    """
    try:
        fr = float(funding.get("funding_rate", 0) or 0)
        if direction == "SHORT":
            if fr > 0.0015:    return 0.30, f"💰 Funding مرتفع جداً ({fr:.4f}) — SHORT قوي"
            elif fr > 0.0010:  return 0.15, f"Funding مرتفع ({fr:.4f})"
        elif direction == "LONG":
            if fr < -0.0010:   return 0.30, f"💰 Funding سالب شديد ({fr:.4f}) — LONG قوي"
            elif fr < -0.0005: return 0.15, f"Funding سالب ({fr:.4f})"
        return 0.0, ""
    except Exception:
        return 0.0, ""


# ══════════════════════════════════════════════════════════════════
# 5. OFI — Order Flow Imbalance
# ══════════════════════════════════════════════════════════════════
def _ofi_signal(book_imbalance: Optional[Dict], direction: str) -> Tuple[float, str]:
    """
    يحسب عدم توازن أوامر الشراء/البيع في Order Book.
    ratio > 0.65 + LONG → تأكيد
    ratio < 0.35 + SHORT → تأكيد
    """
    try:
        if not book_imbalance:
            return 0.0, ""
        ratio = float(book_imbalance.get("ratio", 0.5))
        if direction == "LONG":
            if ratio > 0.70:   return 0.20, f"📊 OFI قوي: {ratio:.2f} — ضغط شراء"
            elif ratio > 0.60: return 0.10, f"OFI معتدل: {ratio:.2f}"
            elif ratio < 0.35: return -0.15, f"⚠️ OFI عكسي: {ratio:.2f}"
        else:
            if ratio < 0.30:   return 0.20, f"📊 OFI قوي: {ratio:.2f} — ضغط بيع"
            elif ratio < 0.40: return 0.10, f"OFI معتدل: {ratio:.2f}"
            elif ratio > 0.65: return -0.15, f"⚠️ OFI عكسي: {ratio:.2f}"
        return 0.0, ""
    except Exception:
        return 0.0, ""


# ══════════════════════════════════════════════════════════════════
# 6. Liquidity Hunt Detection
# ══════════════════════════════════════════════════════════════════
def _detect_liquidity_hunt(c5m: List, c1h: List) -> Dict:
    """
    يكتشف Liquidity Sweep: اختراق قمة/قاع محلية ثم رجوع سريع.
    هذا يعني المؤسسات صادت الـ Stop Losses.
    الفرصة: دخول بعد الـ Sweep بنفس الاتجاه الأصلي.
    """
    try:
        if not c5m or len(c5m) < 10:
            return {"sweep": None, "score": 0.0}

        curr_high  = float(c5m[0][2])
        curr_low   = float(c5m[0][3])
        curr_close = float(c5m[0][4])

        # قمم وقيعان الساعة الماضية (آخر 12 شمعة 5m)
        recent_highs = [float(c[2]) for c in c5m[1:13]]
        recent_lows  = [float(c[3]) for c in c5m[1:13]]
        if not recent_highs: return {"sweep": None, "score": 0.0}

        prev_high = max(recent_highs)
        prev_low  = min(recent_lows)

        sweep = None
        score = 0.0

        # Bear Sweep: اخترق القمة ثم رجع تحتها
        if curr_high > prev_high and curr_close < prev_high:
            sweep = "BEAR_SWEEP"
            score = 0.20
        # Bull Sweep: اخترق القاع ثم رجع فوقه
        elif curr_low < prev_low and curr_close > prev_low:
            sweep = "BULL_SWEEP"
            score = 0.20

        return {"sweep": sweep, "score": score,
                "prev_high": prev_high, "prev_low": prev_low}

    except Exception:
        return {"sweep": None, "score": 0.0}


# ══════════════════════════════════════════════════════════════════
# 7. Adaptive Threshold Learning
# ══════════════════════════════════════════════════════════════════
def record_trade_result(symbol: str, score_at_entry: float,
                        pnl_pct: float) -> None:
    """يُسجَّل من jobs.py عند إغلاق كل صفقة."""
    if symbol not in _PERFORMANCE_HISTORY:
        _PERFORMANCE_HISTORY[symbol] = []
    _PERFORMANCE_HISTORY[symbol].append(pnl_pct)
    # احتفظ بآخر 50 صفقة فقط
    if len(_PERFORMANCE_HISTORY[symbol]) > 50:
        _PERFORMANCE_HISTORY[symbol].pop(0)
    _update_adaptive_threshold(symbol)


def _update_adaptive_threshold(symbol: str) -> None:
    """يحدّث الحد الأدنى للـ score بناءً على الأداء الأخير."""
    history = _PERFORMANCE_HISTORY.get(symbol, [])
    if len(history) < 10:
        return   # لا يكفي للتعلم

    wins     = sum(1 for p in history if p > 0)
    win_rate = wins / len(history)
    base     = CONFIG.SCALP_MIN_SCORE

    if win_rate >= 0.75:
        # أداء ممتاز → خفّف الحد لزيادة الصفقات
        new_thresh = max(base * 0.85, 0.15)
    elif win_rate >= 0.60:
        new_thresh = base   # لا تغيير
    elif win_rate >= 0.45:
        # أداء متوسط → رفع الحد قليلاً
        new_thresh = min(base * 1.10, 0.50)
    else:
        # أداء ضعيف → رفع الحد بشكل أكبر
        new_thresh = min(base * 1.20, 0.55)

    _ADAPTIVE_THRESHOLDS[symbol] = round(new_thresh, 4)
    logger.debug(
        f"[Adaptive] {symbol}: win_rate={win_rate:.0%} "
        f"→ threshold={new_thresh:.3f}"
    )


def _get_adaptive_threshold(symbol: str, default: float) -> float:
    return _ADAPTIVE_THRESHOLDS.get(symbol, default)


# ══════════════════════════════════════════════════════════════════
# الدوال الأصلية (بدون تعديل)
# ══════════════════════════════════════════════════════════════════
def _get_consensus(expert_results: List[Optional[Dict]], direction: str) -> Dict:
    scores, total_w, weighted_sum, votes = [], 0.0, 0.0, 0
    for i, res in enumerate(expert_results):
        s = (res.get("long", 0) if direction == "LONG" else res.get("short", 0)) if res else 0.0
        w = EXPERT_WEIGHTS[i] if i < len(EXPERT_WEIGHTS) else 1.0
        scores.append(s)
        weighted_sum += s * w
        total_w      += w
        if s >= 0.10: votes += 1
    avg      = sum(scores) / len(scores) if scores else 0.0
    weighted = (weighted_sum / total_w) if total_w > 0 else avg
    return {"avg_score": avg, "votes": votes, "weighted_score": weighted}


def _calc_z_score(c5m, c1h, c4h, direction: str) -> float:
    try:
        def zs(candles, n=20):
            if not candles or len(candles) < n: return 0.0
            cl = [float(c[4]) for c in candles[:n]]
            m  = sum(cl) / n
            s  = (sum((x-m)**2 for x in cl)/n)**0.5
            return (cl[0]-m)/s if s > 0 else 0.0
        zC = zs(c5m)*0.20 + zs(c1h)*0.30 + zs(c4h)*0.50
        if   zC < -2.0 and direction == "LONG":  return 0.30
        elif zC < -1.5 and direction == "LONG":  return 0.18
        elif zC >  2.0 and direction == "SHORT": return 0.30
        elif zC >  1.5 and direction == "SHORT": return 0.18
        return 0.0
    except: return 0.0


def _calc_momentum(c5m, c4h, direction: str) -> float:
    try:
        if not c5m or len(c5m) < 13: return 0.0
        p0,p3,p6,p12 = [float(c5m[i][4]) for i in [0,3,6,12]]
        m3=(p0-p3)/(p3 or 1); m6=(p0-p6)/(p6 or 1); m12=(p0-p12)/(p12 or 1)
        score = 0.0
        if direction == "LONG":
            if m3>0 and m6>0 and m12>0: score += 0.30
            elif m3>0 and m6>0:         score += 0.18
            elif m3>0:                  score += 0.08
            if m3>m6 and m6>0:          score += 0.12
        else:
            if m3<0 and m6<0 and m12<0: score += 0.30
            elif m3<0 and m6<0:         score += 0.18
            elif m3<0:                  score += 0.08
            if m3<m6 and m6<0:          score += 0.12
        if c4h and len(c4h) >= 8:
            q0,q3 = float(c4h[0][4]),float(c4h[3][4])
            q6 = float(c4h[min(6,len(c4h)-1)][4])
            if direction=="LONG"  and q0>q3 and q0>q6: score += 0.15
            elif direction=="SHORT" and q0<q3 and q0<q6: score += 0.15
        return score
    except: return 0.0


# ══════════════════════════════════════════════════════════════════
# generate_signal — مع الميزات الجديدة مُدمجة
# ══════════════════════════════════════════════════════════════════
def generate_signal(symbol: str, direction: str, data: Dict,
                    balance: float, open_trades: List[Dict],
                    run_id: str = "") -> Optional[Dict]:

    c5m  = data.get("c5m",  [])
    c15m = data.get("c15m", [])
    c30m = data.get("c30m", [])
    c1h  = data.get("c1h",  [])
    c4h  = data.get("c4h",  [])
    if not c5m or len(c5m) < 20: return None

    price   = float(c5m[0][4])
    funding = data.get("funding", {})

    # ── Portfolio check ───────────────────────────────────────────
    port = portfolio_risk_check(open_trades, symbol, direction, balance)
    if not port["pass"]:
        logger.debug(f"[SG] {symbol} {direction} — portfolio: {port['reason']}")
        return None

    # ── ✅ 1. VCE — Volume Confirmed Entry ────────────────────────
    if not _volume_confirmed(c4h, multiplier=1.3):
        logger.debug(f"[SG] {symbol} {direction} — VCE REJECTED: حجم ضعيف")
        return None

    # ── ✅ 6. Liquidity Hunt — كشف الصيد ──────────────────────────
    hunt = _detect_liquidity_hunt(c5m, c1h)
    if hunt.get("sweep"):
        sweep_type = hunt["sweep"]
        # إذا هناك sweep ضد اتجاه الصفقة → رفض
        if sweep_type == "BEAR_SWEEP" and direction == "LONG":
            logger.debug(f"[SG] {symbol} — Bear Sweep كشف، لا دخول LONG الآن")
            return None
        if sweep_type == "BULL_SWEEP" and direction == "SHORT":
            logger.debug(f"[SG] {symbol} — Bull Sweep كشف، لا دخول SHORT الآن")
            return None

    # ── Cooldown + Re-entry ───────────────────────────────────────
    if was_recently_sent(symbol, direction, True): return None
    re = check_re_entry(symbol, direction)
    if re["blocked"]: return None

    # ── Run all experts ───────────────────────────────────────────
    expert_results: List[Optional[Dict]] = []
    for mod in EXPERTS:
        try:
            res = mod.analyze(data)
            expert_results.append(res)
        except Exception as e:
            logger.warning(f"[SG] Expert {mod.__name__}: {e}")
            expert_results.append(None)

    # ── Consensus ─────────────────────────────────────────────────
    consensus   = _get_consensus(expert_results, direction)
    score       = consensus["weighted_score"]
    votes       = consensus["votes"]

    # ── Quant sub-scores ──────────────────────────────────────────
    z_score  = _calc_z_score(c5m, c1h, c4h, direction)
    mom_sc   = _calc_momentum(c5m, c4h, direction)
    iw       = IE.calc_iw_score(c5m, c1h, c4h, direction)
    mtf      = IE.get_mtf_score(c5m, c15m, c30m, c1h, c4h, direction)
    regime_d = IE.get_market_regime(c4h, c1h)

    final_score = (score        * 0.45 + z_score      * 0.15 +
                   mom_sc       * 0.15 + iw["score"]  * 0.15 +
                   mtf["score"] * 0.10)
    mtf_score = mtf["score"]

    obi = get_obi_signal(data.get("book_imbalance"), direction)
    final_score += obi["boost"] * 0.10

    regime = regime_d.get("regime", "UNKNOWN")

    # ── ✅ 2. HH/LL Engine ────────────────────────────────────────
    hh_ll    = _hh_ll_trend(c4h, n=3, lookback=30)
    hh_score = hh_ll.get("score", 0.0)
    if hh_score > 0 and direction == "LONG":
        final_score += hh_score * 0.15
    elif hh_score < 0 and direction == "SHORT":
        final_score += abs(hh_score) * 0.15
    elif hh_score > 0 and direction == "SHORT":
        final_score -= hh_score * 0.10    # عقوبة: HH/HL ضد SHORT
    elif hh_score < 0 and direction == "LONG":
        final_score -= abs(hh_score) * 0.10

    # ── ✅ 3. Divergence Detection ────────────────────────────────
    div      = _detect_divergence(c4h, direction, lookback=25)
    div_sc   = div.get("score", 0.0)
    # Divergence عكسي يُضعف الإشارة
    if div_sc < 0 and direction == "LONG":
        final_score += div_sc * 0.20   # يُخفض
    elif div_sc > 0 and direction == "SHORT":
        final_score -= div_sc * 0.20
    # Divergence موافق يُقوي الإشارة
    elif div_sc > 0 and direction == "LONG":
        final_score += div_sc * 0.10
    elif div_sc < 0 and direction == "SHORT":
        final_score += abs(div_sc) * 0.10

    # ── ✅ 4. Funding Rate Extremes ────────────────────────────────
    fund_boost, fund_reason = _funding_extreme_signal(funding, direction)
    final_score += fund_boost * 0.15

    # ── ✅ 5. OFI — Order Flow Imbalance ─────────────────────────
    ofi_boost, ofi_reason = _ofi_signal(data.get("book_imbalance"), direction)
    final_score += ofi_boost * 0.10

    # ── Clamp score ───────────────────────────────────────────────
    final_score = round(min(max(final_score, 0.0), 1.0), 4)

    # ── ATR & size ────────────────────────────────────────────────
    atr = IE.atr(c5m, 14)
    if not atr_pct_check(price, atr): return None

    trade_type = get_trade_type(c5m, c1h, c4h, direction, regime)
    sltp       = calc_sltp(price, atr, direction, trade_type)
    if sltp["rr"] < CONFIG.MIN_RR: return None

    size_usdt = calc_position_size(balance, price, atr)
    if size_usdt <= 0: return None

    # ── ✅ 7. Adaptive Threshold ──────────────────────────────────
    vol_profile = ASSET_VOLATILITY.get(symbol, "MEDIUM")
    thresh      = VOL_THRESHOLDS[vol_profile]
    is_scalp    = "Scalp" in trade_type
    session     = get_session()

    if is_opportunity_window() and session.get("overlap"):
        min_score = CONFIG.OPP_SCORE
        min_votes = CONFIG.OPP_VOTES
        min_mtf   = CONFIG.OPP_MTF
    elif is_scalp:
        base_min  = max(thresh["min_score"], CONFIG.SCALP_MIN_SCORE)
        min_score = _get_adaptive_threshold(symbol, base_min)  # ✅ Adaptive
        min_votes = max(thresh["min_votes"], CONFIG.SCALP_MIN_VOTES)
        min_mtf   = max(thresh["min_mtf"],   CONFIG.SCALP_MIN_MTF)
    else:
        base_min  = max(thresh["min_score"], CONFIG.SWING_MIN_SCORE)
        min_score = _get_adaptive_threshold(symbol, base_min)  # ✅ Adaptive
        min_votes = max(thresh["min_votes"], CONFIG.SWING_MIN_VOTES)
        min_mtf   = max(thresh["min_mtf"],   CONFIG.SWING_MIN_MTF)

    if final_score < min_score:
        logger.debug(f"[SG] {symbol} {direction} REJECT score={final_score:.3f} < {min_score:.3f}")
        return None
    if votes < min_votes:
        logger.debug(f"[SG] {symbol} {direction} REJECT votes={votes} < {min_votes}")
        return None
    if mtf_score < min_mtf:
        logger.debug(f"[SG] {symbol} {direction} REJECT mtf={mtf_score:.3f} < {min_mtf}")
        return None

    experts_fired = [
        EXPERT_NAMES[i] for i, r in enumerate(expert_results)
        if r and ((direction=="LONG"  and r.get("long",  0)>=0.10) or
                  (direction=="SHORT" and r.get("short", 0)>=0.10))
    ]

    mark_sent(symbol, direction, is_scalp)

    signal = {
        "symbol":        symbol,
        "direction":     direction,
        "trade_type":    trade_type,
        "score":         final_score,
        "expert_votes":  votes,
        "mtf_score":     round(mtf_score, 4),
        "entry":         round(price, 6),
        "sl":            sltp["sl"],
        "tp1":           sltp["tp1"],
        "tp2":           sltp["tp2"],
        "tp3":           sltp["tp3"],
        "rr":            sltp["rr"],
        "size_usdt":     size_usdt,
        "regime":        regime,
        "timeframe":     "5m",
        "experts_fired": experts_fired,
        "ai_confirmation": "PENDING",
        "run_id":        run_id or str(uuid.uuid4())[:8],
        "session":       session["name"],
        "obi_boost":     obi["boost"],
        # ── ميزات جديدة في البيانات ──────────────────────────────
        "hh_ll":         hh_ll.get("pattern", "—"),
        "divergence":    f"RSI:{div.get('rsi_div')} OBV:{div.get('obv_div')}",
        "funding_boost": round(fund_boost, 3),
        "ofi_boost":     round(ofi_boost, 3),
        "liq_hunt":      hunt.get("sweep", "None"),
        "vce_ok":        True,
    }
    logger.info(
        f"✅ Signal: {symbol} {direction} {trade_type} "
        f"score={final_score:.3f} votes={votes} "
        f"HH={hh_ll.get('trend','?')} "
        f"div={div.get('rsi_div','—')} "
        f"fund={fund_boost:.2f}"
    )
    return signal


def run_all_assets(assets_data: Dict, balance: float, open_trades: List[Dict],
                   run_id: str = "") -> List[Dict]:
    signals = []
    for symbol, data in assets_data.items():
        if not data: continue
        for direction in ["LONG", "SHORT"]:
            sig = generate_signal(symbol, direction, data,
                                  balance, open_trades, run_id)
            if sig:
                signals.append(sig)
    signals.sort(key=lambda s: s["score"], reverse=True)
    return signals[:CONFIG.MAX_SIGNALS_PER_RUN]

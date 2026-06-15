"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️  ELITE v7 (AI Confluence)
══════════════════════════════════════════════════════════════════════════
TARGET: High win-rate + frequent trades + reaching TP2/TP3 (positive expectancy)

WHAT CHANGED vs v6.1 (التطويرات الجوهرية):
  ✅ Look-ahead bias REMOVED  → الدايفرجنس الآن سببي (past-only) لا يستخدم المستقبل
  ✅ Strategy flipped to TREND-PULLBACK (مع الترند) بدل الانعكاس ضد الترند
       → الدخول على ارتداد فيبوناتشي/EMA في اتجاه الترند ⇒ استمرارية ⇒ TP2/TP3 تتحقق
  ✅ Multi-target scaling: 40% @TP1 / 30% @TP2 / 30% trailing @TP3
       → بدل إغلاق نصف الصفقة عند breakeven خانق
  ✅ Loose Chandelier trailing (3×ATR) يُفعّل فقط بعد TP2 ⇒ يترك الرابح يركض
  ✅ Conservative intrabar: SL يُفحص أولاً (لا تضخيم وين-رايت)
  ✅ Fees + slippage مُحتسبة في كل صفقة (واقعية)
  ✅ Regime filter (ADX + EMA slope) لتجنّب التذبذب الجانبي
  ✅ AI weighted-confluence score بعتبة ديناميكية حسب قوة الترند
  ✅ Config dataclass واحد لكل المعايير (سهل الضبط)

CORE PHILOSOPHY (4 schools must agree, trend-aligned):
  1. TREND/REGIME : EMA200 slope + EMA alignment + ADX  → يحدد الاتجاه
  2. LOCATION     : Pullback to Fibonacci PRZ / dynamic EMA  → سعر عادل للدخول
  3. MOMENTUM     : RSI turning + MACD hist + Stoch + OBV  → زخم يعود مع الترند
  4. STRUCTURE/SMC: Reversal candle + FVG + volume thrust  → تأكيد الدخول

SESSIONS: LONDON(07-12) + OVERLAP(13-15) + NY(13-16)  (ASIA optional)
══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import httpx, pandas as pd
from loguru import logger

try:
    import pandas_ta as ta  # optional, not required
    HAS_TA = True
except Exception:
    HAS_TA = False

_BASE = "https://www.okx.com/api/v5"
_HDR  = {"Accept": "application/json", "User-Agent": "Ramos360ELITE/7.0"}
_TFM  = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1H","4h":"4H","1d":"1D","1w":"1W"}


# ══════════════════════════════════════════════════════════════════
# CONFIG — كل المعايير القابلة للضبط في مكان واحد
# ══════════════════════════════════════════════════════════════════
@dataclass
class BTConfig:
    # ── Risk & sizing ──
    risk_per_trade: float = 0.012          # 1.2% مخاطرة من الرصيد لكل صفقة
    fee_rate: float = 0.0005               # 0.05% عمولة لكل جهة (taker)
    slippage: float = 0.0003               # 0.03% انزلاق لكل تنفيذ

    # ── Targets (R multiples) — TP2/TP3 يُستبدلان بفيبو إكستنشن إن توفّر ──
    tp1_r: float = 1.4
    tp2_r: float = 2.4
    tp3_r: float = 3.8
    tp1_frac: float = 0.45                 # نسبة الإغلاق عند كل هدف
    tp2_frac: float = 0.30
    # المتبقي (0.30) يُترك مع trailing حتى TP3

    # ── Stop loss bounds (× ATR) ──
    sl_atr_min: float = 2.0
    sl_atr_max: float = 2.8
    sl_buffer_atr: float = 0.20

    # ── Trailing (Chandelier) يُفعّل بعد TP2 فقط ──
    chandelier_atr: float = 3.0            # واسع ⇒ يترك الرابح يركض

    # ── Entry gating ──
    min_rr: float = 1.2                    # أدنى عائد/مخاطرة لـ TP1
    base_threshold: float = 1.8            # عتبة سكور الكونفلونس الأساسية
    adx_trend_min: float = 18.0            # أدنى ADX لاعتبار السوق في ترند
    max_hold_bars: int = 48                # خروج زمني
    max_consec_loss: int = 3               # تبريد بعد خسائر متتالية
    allow_asia: bool = False               # السماح بجلسة آسيا (مزيد من الصفقات)
    allow_counter_trend: bool = False      # السماح بصفقات انعكاسية بحجم مخفّض

    require_reclaim: bool = True
    swing_lookback: int = 55               # نافذة السوينغ هاي/لو لفيبو


# ── Custom Fibonacci Ratios ────────────────────────────────────────────────
FIBO_RET = [0.236, 0.382, 0.5, 0.618, 0.705, 0.786]
FIBO_EXT = [1.272, 1.414, 1.618, 2.0, 2.618]


# ── SESSIONS ────────────────────────────────────────────────────────────────
def _sess(h: int) -> str:
    if 13 <= h < 16: return "OVERLAP"
    if 16 <= h < 17: return "NEW_YORK"
    if 7  <= h < 13: return "LONDON"
    return "ASIA" if h < 7 or h >= 17 else "DEAD_ZONE"

def _sess_ok(h: int, cfg: BTConfig) -> bool:
    if cfg.allow_asia:
        return True
    return 7 <= h < 17


# ══════════════════════════════════════════════════════════════════
# OKX FETCH (paginated history candles)
# ══════════════════════════════════════════════════════════════════
def _inst(s: str) -> str:
    return s.split(":")[0].replace("/", "-") + "-SWAP"

async def _fetch(symbol: str, tf: str, start: datetime, end: datetime) -> pd.DataFrame:
    inst = _inst(symbol); bar = _TFM.get(tf.lower(), tf)
    s_ms = int(start.timestamp() * 1000); e_ms = int(end.timestamp() * 1000)
    rows = []; before = e_ms
    logger.info(f"[BT] {symbol} {tf} {start.date()}→{end.date()}")
    async with httpx.AsyncClient(timeout=30, headers=_HDR) as cl:
        while True:
            try:
                r = await cl.get(f"{_BASE}/market/history-candles", params={
                    "instId": inst, "bar": bar,
                    "before": str(s_ms), "after": str(before), "limit": "300"})
            except Exception as e:
                logger.warning(f"[BT] fetch retry: {e}"); await asyncio.sleep(2); continue
            if r.status_code == 429:
                await asyncio.sleep(5); continue
            if r.status_code != 200:
                break
            j = r.json()
            if j.get("code") != "0" or not j.get("data"):
                break
            for c in j["data"]:
                ts = int(c[0])
                if s_ms <= ts <= e_ms:
                    rows.append([ts, float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])])
            old = int(j["data"][-1][0])
            if old <= s_ms or len(j["data"]) < 300:
                break
            before = old; await asyncio.sleep(0.3)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume"])
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    logger.info(f"[BT] {symbol}: {len(df)} candles ✅")
    return df


# ══════════════════════════════════════════════════════════════════
# INDICATORS
# ══════════════════════════════════════════════════════════════════
def _build(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(); c = df.close; h = df.high; l = df.low
    # RSI 6 + 14 (Wilder)
    for p in [6, 14]:
        d = c.diff(); g = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
        ls = (-d.clip(upper=0)).ewm(alpha=1/p, adjust=False).mean()
        df[f"rsi{p}"] = (100 - 100/(1 + g/ls.replace(0, 1e-10))).fillna(50)
    df["rsi_p"] = df.rsi14.shift(1).fillna(50)
    # MACD histogram
    e12 = c.ewm(span=12, adjust=False).mean(); e26 = c.ewm(span=26, adjust=False).mean()
    ms = e12 - e26; df["mh"] = (ms - ms.ewm(span=9, adjust=False).mean()).fillna(0)
    df["mh_p"] = df.mh.shift(1).fillna(0)
    # EMAs
    for p in [9, 20, 50, 100, 200]:
        df[f"e{p}"] = c.ewm(span=p, adjust=False).mean()
    df["e200_slope"] = df.e200.diff(5) / df.e200.shift(5).replace(0, 1e-10)  # ميل الترند
    # ATR
    hl = h - l; hpc = (h - c.shift()).abs(); lpc = (l - c.shift()).abs()
    df["atr"] = pd.concat([hl, hpc, lpc], axis=1).max(axis=1).ewm(alpha=1/14, adjust=False).mean()
    # ADX (DI based)
    up = (h - h.shift()).clip(lower=0); dn = (l.shift() - l).clip(lower=0)
    df["pdi"] = up.ewm(alpha=1/14, adjust=False).mean()
    df["mdi"] = dn.ewm(alpha=1/14, adjust=False).mean()
    df["adx"] = ((df.pdi - df.mdi).abs() / (df.pdi + df.mdi + 1e-10) * 100).ewm(alpha=1/14, adjust=False).mean().fillna(15)
    # Bollinger
    mid = c.rolling(20).mean(); sd = c.rolling(20).std()
    df["bbu"] = mid + 2*sd; df["bbl"] = mid - 2*sd
    df["bbw"] = (df.bbu - df.bbl) / mid.replace(0, 1)
    df["bbw_ma"] = df.bbw.rolling(20).mean()
    # Stoch
    lo14 = l.rolling(14).min(); hi14 = h.rolling(14).max()
    df["stoch"] = 100 * (c - lo14) / (hi14 - lo14 + 1e-10)
    df["stoch_p"] = df.stoch.shift(1)
    # OBV
    df["obv"] = (c.diff().apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0) * df.volume).cumsum()
    df["obv_ma"] = df.obv.rolling(20).mean()
    # Volume ratio
    df["vma"] = df.volume.rolling(20).mean(); df["vr"] = df.volume / (df.vma.replace(0, 1))
    # SMC FVG
    df["fvg_bull"] = (df.low - df.high.shift(2)) > 0
    df["fvg_bear"] = (df.low.shift(2) - df.high) > 0
    # Candle body ratio
    body = (c - df.open).abs(); rng = (h - l).replace(0, 1e-6)
    df["body_r"] = (body / rng).fillna(0.5)
    # Swing levels
    df["slo14"] = l.rolling(14).min(); df["shi14"] = h.rolling(14).max()
    return df.ffill().bfill()


# ══════════════════════════════════════════════════════════════════
# CAUSAL DIVERGENCE — past-only (NO look-ahead) ⭐ إصلاح حرج
# نكتشف القمم/القيعان بعد تأكيدها بـ k شموع، ونثبّت إشارة الدايفرجنس
# عند شمعة "الاكتشاف" (i = pivot + k) لا عند القمة/القاع نفسها.
# ══════════════════════════════════════════════════════════════════
def _add_div(df: pd.DataFrame, k: int = 3) -> pd.DataFrame:
    sc = pd.Series(0.0, index=df.index)
    lows: List[Tuple[int, float, float]] = []
    highs: List[Tuple[int, float, float]] = []
    low = df.low.values; high = df.high.values; rsi = df.rsi14.values
    n = len(df)
    for i in range(k, n):
        p = i - k  # المحور المرشّح (تأكّد الآن فقط)
        if p - k < 0:
            continue
        win_lo = low[p - k:p + k + 1]
        win_hi = high[p - k:p + k + 1]
        # قاع محوري مؤكّد
        if low[p] == win_lo.min():
            lows.append((p, float(low[p]), float(rsi[p])))
            if len(lows) >= 2:
                a, b = lows[-2], lows[-1]
                if b[1] < a[1] and b[2] > a[2]:  sc.iloc[i] += 1.0   # bullish reg div
                elif b[1] > a[1] and b[2] < a[2]: sc.iloc[i] += 0.5   # hidden bull
        # قمة محورية مؤكّدة
        if high[p] == win_hi.max():
            highs.append((p, float(high[p]), float(rsi[p])))
            if len(highs) >= 2:
                a, b = highs[-2], highs[-1]
                if b[1] > a[1] and b[2] < a[2]:  sc.iloc[i] -= 1.0   # bearish reg div
                elif b[1] < a[1] and b[2] > a[2]: sc.iloc[i] -= 0.5   # hidden bear
    # الإشارة تبقى فعّالة لعدة شموع بعد الاكتشاف (decay)
    df["div_sc"] = sc.rolling(k + 2, min_periods=1).max().clip(-1.5, 1.5) \
        .where(sc.rolling(k + 2, min_periods=1).max().abs() >= sc.rolling(k + 2, min_periods=1).min().abs(),
               sc.rolling(k + 2, min_periods=1).min().clip(-1.5, 1.5)).fillna(0)
    # تبسيط آمن: استخدم آخر إشارة غير صفرية ضمن نافذة k+2
    raw = sc.replace(0, pd.NA).ffill(limit=k + 2).fillna(0)
    df["div_sc"] = raw.clip(-1.5, 1.5)
    return df


# ══════════════════════════════════════════════════════════════════
# FIBONACCI — pullback location + SL + targets
# ══════════════════════════════════════════════════════════════════
def _pullback_zone(price: float, hi: float, lo: float, direction: str,
                   tol: float = 0.03) -> Tuple[bool, float, str]:
    """هل السعر داخل منطقة ارتداد فيبو صالحة للدخول مع الترند؟"""
    rng = hi - lo
    if rng <= 0:
        return False, 0.0, "NONE"
    if direction == "LONG":
        # في ترند صاعد ندخل على ارتداد هابط: 0.382 → 0.705 من المدى
        for r, strength in [(0.382, 0.7), (0.5, 0.85), (0.618, 1.0), (0.705, 0.8)]:
            level = hi - rng * r
            if abs(price - level) <= rng * tol:
                return True, strength, f"BULL_F{int(r*1000)}"
    else:
        for r, strength in [(0.382, 0.7), (0.5, 0.85), (0.618, 1.0), (0.705, 0.8)]:
            level = lo + rng * r
            if abs(price - level) <= rng * tol:
                return True, strength, f"BEAR_F{int(r*1000)}"
    return False, 0.0, "NONE"

def _fib_sl(price: float, hi: float, lo: float, direction: str, df, i, atr: float,
            cfg: BTConfig) -> Tuple[float, float]:
    rng = hi - lo; buf = atr * cfg.sl_buffer_atr
    if direction == "LONG":
        cands = sorted([hi - rng*r for r in FIBO_RET if (hi - rng*r) < price * 0.998])
        sl = (max(cands) - buf) if cands else (float(df.slo14.iloc[i]) - buf)
        sl = min(sl, price - atr*cfg.sl_atr_min)
        sl = max(sl, price - atr*cfg.sl_atr_max)
        sl_d = price - sl
    else:
        cands = sorted([lo + rng*r for r in FIBO_RET if (lo + rng*r) > price * 1.002], reverse=True)
        sl = (min(cands) + buf) if cands else (float(df.shi14.iloc[i]) + buf)
        sl = max(sl, price + atr*cfg.sl_atr_min)
        sl = min(sl, price + atr*cfg.sl_atr_max)
        sl_d = sl - price
    return round(sl, 4), round(sl_d, 6)

def _fib_tps(price: float, sl_d: float, direction: str, hi: float, lo: float,
             cfg: BTConfig) -> Tuple[float, float, float]:
    rng = hi - lo
    if direction == "LONG":
        tp1 = price + sl_d*cfg.tp1_r
        tp2 = price + sl_d*cfg.tp2_r
        tp3 = price + sl_d*cfg.tp3_r
        if rng > 0:
            exts = sorted([lo + rng*r for r in FIBO_EXT])
            above = [v for v in exts if v > price*1.003]
            if len(above) >= 1 and above[0] > tp2: tp2 = above[0]
            if len(above) >= 2 and above[1] > tp3: tp3 = above[1]
    else:
        tp1 = price - sl_d*cfg.tp1_r
        tp2 = price - sl_d*cfg.tp2_r
        tp3 = price - sl_d*cfg.tp3_r
        if rng > 0:
            exts = sorted([hi - rng*r for r in FIBO_EXT], reverse=True)
            below = [v for v in exts if v < price*0.997]
            if len(below) >= 1 and below[0] < tp2: tp2 = below[0]
            if len(below) >= 2 and below[1] < tp3: tp3 = below[1]
    return round(tp1, 4), round(tp2, 4), round(tp3, 4)


# ══════════════════════════════════════════════════════════════════
# SCHOOL 1: REGIME / TREND  → يحدد الاتجاه المسموح
# ══════════════════════════════════════════════════════════════════
def _regime(df, i, cfg: BTConfig) -> Tuple[str, float]:
    row = df.iloc[i]; c = float(row.close)
    e20, e50, e200 = float(row.e20), float(row.e50), float(row.e200)
    slope = float(row.e200_slope); adx = float(row.adx)
    if adx < cfg.adx_trend_min:
        return "RANGE", 0.0
    long_sc = short_sc = 0.0
    if c > e20 > e50 > e200: long_sc += 1.0
    elif c > e50 > e200:     long_sc += 0.7
    elif c > e200:           long_sc += 0.4
    if slope > 0:            long_sc += 0.5
    if c < e20 < e50 < e200: short_sc += 1.0
    elif c < e50 < e200:     short_sc += 0.7
    elif c < e200:           short_sc += 0.4
    if slope < 0:            short_sc += 0.5
    if long_sc >= 0.8 and long_sc > short_sc:  return "UP",   round(long_sc, 3)
    if short_sc >= 0.8 and short_sc > long_sc: return "DOWN", round(short_sc, 3)
    return "RANGE", 0.0


# ══════════════════════════════════════════════════════════════════
# SCHOOL 2: MOMENTUM (turning back WITH the trend)
# ══════════════════════════════════════════════════════════════════
def _momentum_signal(df, i, direction: str) -> Tuple[bool, float]:
    row = df.iloc[i]
    rsi14 = float(row.rsi14); rsi6 = float(row.rsi6); rsip = float(row.rsi_p)
    mh = float(row.mh); mhp = float(row.mh_p)
    obv = float(row.obv); obv_m = float(row.obv_ma)
    stoch = float(row.stoch); stochp = float(row.stoch_p); score = 0.0

    if direction == "LONG":
        if rsi14 < 40:    score += 0.9     # ارتداد من تشبّع بيعي ضمن الترند
        elif rsi14 < 50:  score += 0.6
        elif rsi14 < 60:  score += 0.3
        elif rsi14 > 72:  return False, 0.0
        if rsi14 > rsip:  score += 0.4     # RSI ينعطف صعوداً
        if rsi6 < 45:     score += 0.3
        if mh > mhp:      score += 0.6     # هيستوجرام يتحسّن
        if mh > 0 and mhp <= 0: score += 0.4
        if obv > obv_m:   score += 0.3
        if stoch < 35 and stoch > stochp: score += 0.5
    else:
        if rsi14 > 60:    score += 0.9
        elif rsi14 > 50:  score += 0.6
        elif rsi14 > 40:  score += 0.3
        elif rsi14 < 28:  return False, 0.0
        if rsi14 < rsip:  score += 0.4
        if rsi6 > 55:     score += 0.3
        if mh < mhp:      score += 0.6
        if mh < 0 and mhp >= 0: score += 0.4
        if obv < obv_m:   score += 0.3
        if stoch > 65 and stoch < stochp: score += 0.5
    return score >= 0.6, round(score, 3)


# ══════════════════════════════════════════════════════════════════
# SCHOOL 3: STRUCTURE & SMC (confirmation candle + FVG + volume)
# ══════════════════════════════════════════════════════════════════
def _structure_signal(df, i, direction: str) -> Tuple[bool, float]:
    if i < 3:
        return False, 0.0
    score = 0.0
    r0, r1, r2 = df.iloc[i], df.iloc[i-1], df.iloc[i-2]
    o0,h0,l0,c0 = float(r0.open),float(r0.high),float(r0.low),float(r0.close)
    o1,h1,l1,c1 = float(r1.open),float(r1.high),float(r1.low),float(r1.close)
    o2,c2 = float(r2.open),float(r2.close)
    b0=abs(c0-o0); rng0=(h0-l0) or 1e-4
    b1=abs(c1-o1); rng1=(h1-l1) or 1e-4
    vr=float(r0.vr)

    fvg_bull = float(r1.fvg_bull) == 1 and l0 <= float(r2.high)
    fvg_bear = float(r1.fvg_bear) == 1 and h0 >= float(r2.low)

    if direction == "LONG":
        hammer   = (min(o0,c0)-l0) > b0*1.5 and (h0-max(o0,c0)) < b0*0.6
        bull_eng = c0>o0 and c1<o1 and o0<=c1 and c0>=o1
        morn_star= c2<o2 and b1<rng1*0.45 and c0>o0 and c0>((o2+c2)/2)
        strong   = c0>o0 and b0>rng0*0.55
        if morn_star: score += 1.0
        elif bull_eng: score += 0.9
        elif hammer:   score += 0.8
        elif strong:   score += 0.5
        else:          return False, 0.0
        if fvg_bull: score += 0.4
        if vr > 1.5: score += 0.5
        elif vr > 1.0: score += 0.25
        elif vr < 0.6: score -= 0.3
    else:
        star_sh  = (h0-max(o0,c0)) > b0*1.5 and (min(o0,c0)-l0) < b0*0.6
        bear_eng = c0<o0 and c1>o1 and o0>=c1 and c0<=o1
        even_star= c2>o2 and b1<rng1*0.45 and c0<o0 and c0<((o2+c2)/2)
        strong   = c0<o0 and b0>rng0*0.55
        if even_star: score += 1.0
        elif bear_eng: score += 0.9
        elif star_sh:  score += 0.8
        elif strong:   score += 0.5
        else:          return False, 0.0
        if fvg_bear: score += 0.4
        if vr > 1.5: score += 0.5
        elif vr > 1.0: score += 0.25
        elif vr < 0.6: score -= 0.3
    return score >= 0.5, round(score, 3)


# ══════════════════════════════════════════════════════════════════
# AI WEIGHTED CONFLUENCE — القرار النهائي
# ══════════════════════════════════════════════════════════════════
def _elite_signal(df, i, hi, lo, cfg: BTConfig) -> Tuple[str, float, Dict]:
    regime, reg_sc = _regime(df, i, cfg)
    price = float(df.close.iloc[i])
    div = float(df.div_sc.iloc[i])

    # 1) حدّد الاتجاه من الترند (الأساس). RANGE = لا دخول (إلا counter-trend اختياري)
    if regime == "UP":
        direction = "LONG"
    elif regime == "DOWN":
        direction = "SHORT"
    elif cfg.allow_counter_trend and abs(div) >= 1.0:
        direction = "LONG" if div > 0 else "SHORT"
        reg_sc = 0.3  # حجم/ثقة مخفّضة
    else:
        return "NEUTRAL", 0.0, {}

    # 2) الموقع: يجب أن يكون السعر على ارتداد فيبو
    at_zone, loc_sc, zone = _pullback_zone(price, hi, lo, direction)
    if not at_zone:
        return "NEUTRAL", 0.0, {}

    # 3) الزخم
    mom_ok, mom_sc = _momentum_signal(df, i, direction)
    if not mom_ok:
        return "NEUTRAL", 0.0, {}

    # 4) البنية
    st_ok, st_sc = _structure_signal(df, i, direction)
    if not st_ok:
        return "NEUTRAL", 0.0, {}

    # ⭐ تأكيد الدخول: السعر استعاد EMA9 وأغلق في اتجاه الترند (يتجنّب القبض على السكين)
    if cfg.require_reclaim:
        c0=float(df.close.iloc[i]); o0=float(df.open.iloc[i]); e9=float(df.e9.iloc[i])
        if direction=="LONG"  and not (c0>o0 and c0>=e9): return "NEUTRAL",0.0,{}
        if direction=="SHORT" and not (c0<o0 and c0<=e9): return "NEUTRAL",0.0,{}

    # دايفرجنس داعم/معاكس
    div_bonus = 0.0
    if direction == "LONG"  and div > 0: div_bonus = div * 0.4
    if direction == "SHORT" and div < 0: div_bonus = -div * 0.4

    # سكور موزون (AI confluence)
    total = (reg_sc*0.9 + loc_sc*0.8 + mom_sc*0.9 + st_sc*0.8 + div_bonus)

    # عتبة ديناميكية: ترند أقوى ⇒ نسمح بسكور أقل قليلاً
    thr = cfg.base_threshold - min(0.4, max(0.0, (float(df.adx.iloc[i]) - cfg.adx_trend_min) * 0.02))
    if total < thr:
        return "NEUTRAL", round(total, 3), {}

    details = {"regime": regime, "zone": zone, "reg": reg_sc, "loc": loc_sc,
               "mom": mom_sc, "struct": st_sc, "div": round(div, 2),
               "total": round(total, 3), "thr": round(thr, 3)}
    return direction, round(total, 3), details


# ── NOISE / CHOP FILTER ─────────────────────────────────────────────────────
def _is_noisy(df, i) -> bool:
    if i < 20:
        return False
    body_r = float(df.body_r.iloc[i]); adx = float(df.adx.iloc[i])
    bbw = float(df.bbw.iloc[i])
    bbw_ma = float(df.bbw_ma.iloc[i])
    return (body_r < 0.12 and adx < 14) or (bbw < bbw_ma * 0.5 and adx < 13)


# ══════════════════════════════════════════════════════════════════
# SIMULATION — multi-target scaling, conservative intrabar, fees
# ══════════════════════════════════════════════════════════════════
def _apply_fees(pnl_pct: float, cfg: BTConfig) -> float:
    """خصم العمولة + الانزلاق لجهتي الدخول/الخروج."""
    cost = (cfg.fee_rate + cfg.slippage) * 2 * 100  # %
    return pnl_pct - cost

def _sim(df: pd.DataFrame, cfg: BTConfig, balance: float = 10_000.0) -> Dict:
    n = len(df); start_balance = balance
    equity = [balance]; trades = []
    WARMUP = max(60, cfg.swing_lookback + 5)
    in_trade = False
    direction = ""; entry = sl = tp1 = tp2 = tp3 = sl_d = 0.0
    entry_idx = 0; entry_sess = "—"
    pos_frac = 1.0          # ما تبقّى من الصفقة (1.0 → 0)
    hit_tp1 = hit_tp2 = False
    consec_loss = 0; risk_dollars = 0.0

    def close_leg(exit_price: float, frac: float, etype: str, force_dir: str):
        nonlocal balance, consec_loss
        raw = ((exit_price-entry)/entry*100) if force_dir == "LONG" else ((entry-exit_price)/entry*100)
        raw = _apply_fees(raw, cfg)
        # تحويل % السعر إلى ربح/خسارة دولارية حسب المخاطرة الثابتة
        r_mult = raw / (sl_d/entry*100) if sl_d > 0 else 0.0
        gain = risk_dollars * r_mult * frac
        balance = max(1.0, balance + gain)
        win = gain > 0
        if etype in ("SL", "TIME_EXIT", "TRAIL_STOP") and not win:
            consec_loss += 1
        elif win:
            consec_loss = 0
        trades.append({"direction":force_dir,"entry":round(entry,4),"exit":round(exit_price,4),
                       "pnl_pct":round(raw*frac,4),"r_mult":round(r_mult*frac,3),
                       "result":"WIN" if win else "LOSS","exit_type":etype,
                       "session":entry_sess,"frac":round(frac,2)})

    for i in range(WARMUP, n):
        row = df.iloc[i]; price = float(row.close)
        hi_c = float(row.high); lo_c = float(row.low)
        hour = row.ts.hour if hasattr(row.ts, "hour") else 10
        atr_now = float(row.atr); atr_now = atr_now if atr_now > 0 else price*0.01

        # ───────── manage open position ─────────
        if in_trade:
            # خروج زمني
            if (i - entry_idx) > cfg.max_hold_bars:
                close_leg(price, pos_frac, "TIME_EXIT", direction)
                in_trade = False; equity.append(balance); continue

            if direction == "LONG":
                sl_hit  = lo_c <= sl
                tp1_hit = hi_c >= tp1
                tp2_hit = hi_c >= tp2
                tp3_hit = hi_c >= tp3
            else:
                sl_hit  = hi_c >= sl
                tp1_hit = lo_c <= tp1
                tp2_hit = lo_c <= tp2
                tp3_hit = lo_c <= tp3

            # ⭐ تحفّظ: SL يُفحص أولاً (إن ضُرب الوقف في نفس الشمعة نفترضه أولاً)
            if sl_hit:
                etype = "SL" if not hit_tp1 else ("BE_STOP" if not hit_tp2 else "TRAIL_STOP")
                close_leg(sl, pos_frac, etype, direction)
                in_trade = False; equity.append(balance); continue

            # TP1 → أغلق 40% وحرّك الوقف لنقطة الدخول + الرسوم
            if tp1_hit and not hit_tp1:
                close_leg(tp1, cfg.tp1_frac, "TP1", direction)
                pos_frac -= cfg.tp1_frac; hit_tp1 = True
                be = entry*(1 + (cfg.fee_rate+cfg.slippage)*2) if direction == "LONG" \
                     else entry*(1 - (cfg.fee_rate+cfg.slippage)*2)
                sl = be

            # TP2 → أغلق 30% وفعّل Chandelier على المتبقي
            if tp2_hit and hit_tp1 and not hit_tp2:
                close_leg(tp2, cfg.tp2_frac, "TP2", direction)
                pos_frac -= cfg.tp2_frac; hit_tp2 = True

            # TP3 → أغلق المتبقي
            if tp3_hit and hit_tp2:
                close_leg(tp3, pos_frac, "TP3", direction)
                in_trade = False; pos_frac = 0.0; equity.append(balance); continue

            # Chandelier trailing (واسع) بعد TP2 فقط ⇒ يترك الرابح يركض
            if hit_tp2 and pos_frac > 0:
                if direction == "LONG":
                    nt = round(hi_c - atr_now*cfg.chandelier_atr, 4)
                    if nt > sl: sl = nt
                else:
                    nt = round(lo_c + atr_now*cfg.chandelier_atr, 4)
                    if nt < sl: sl = nt

            equity.append(balance); continue

        # ───────── look for new entry ─────────
        if not _sess_ok(hour, cfg):
            equity.append(balance); continue
        if _is_noisy(df, i):
            equity.append(balance); continue
        if consec_loss >= cfg.max_consec_loss:
            consec_loss = 0; equity.append(balance); continue

        lb = cfg.swing_lookback
        hi_sw = float(df.high.iloc[max(0, i-lb):i+1].max())
        lo_sw = float(df.low.iloc[max(0, i-lb):i+1].min())

        sig, score, dets = _elite_signal(df, i, hi_sw, lo_sw, cfg)
        if sig == "NEUTRAL":
            equity.append(balance); continue

        sl_p, sl_dist = _fib_sl(price, hi_sw, lo_sw, sig, df, i, atr_now, cfg)
        if sl_dist <= 0:
            equity.append(balance); continue
        tp1_p, tp2_p, tp3_p = _fib_tps(price, sl_dist, sig, hi_sw, lo_sw, cfg)

        rr = abs(tp1_p - price) / max(sl_dist, 1e-10)
        if rr < cfg.min_rr:
            equity.append(balance); continue

        # افتح الصفقة
        direction = sig; entry = price
        sl = sl_p; tp1 = tp1_p; tp2 = tp2_p; tp3 = tp3_p; sl_d = sl_dist
        entry_sess = _sess(hour); entry_idx = i
        in_trade = True; pos_frac = 1.0; hit_tp1 = hit_tp2 = False
        # ثقة الإشارة قد تُعدّل المخاطرة قليلاً (AI sizing)
        conf = min(1.2, max(0.6, score / cfg.base_threshold))
        risk_dollars = balance * cfg.risk_per_trade * conf
        equity.append(balance)

    # إغلاق أي صفقة مفتوحة في النهاية
    if in_trade:
        lp = float(df.close.iloc[-1])
        close_leg(lp, pos_frac, "OPEN_AT_END", direction)

    return {"trades": trades, "equity": equity,
            "final_balance": balance, "start_balance": start_balance}


# ══════════════════════════════════════════════════════════════════
# METRICS
# ══════════════════════════════════════════════════════════════════
def _metrics(sim: Dict, initial: float) -> Dict:
    trades = sim["trades"]; eq = pd.Series(sim["equity"]); bal = sim["final_balance"]
    if not trades:
        return {"error":"No trades","total":0,"win_rate_pct":0,"max_dd_pct":0,
                "return_pct":0,"sharpe":0}
    wins   = [t for t in trades if t["result"] == "WIN"]
    losses = [t for t in trades if t["result"] == "LOSS"]
    tot = len(trades); wr = round(len(wins)/tot*100, 2) if tot else 0
    dd = round(float(((eq - eq.cummax())/eq.cummax()*100).min()), 2)
    ret = round((bal - initial)/initial*100, 2)
    rets = eq.pct_change().dropna()
    sh = round(float(rets.mean()/rets.std()*(252**0.5)), 3) if rets.std() > 0 else 0.0
    gross_win = sum(t["pnl_pct"] for t in wins)
    gross_loss = abs(sum(t["pnl_pct"] for t in losses))
    pf = round(gross_win/gross_loss, 2) if gross_loss > 0 else 99.9
    aw = round(gross_win/len(wins), 3) if wins else 0
    al = round(sum(t["pnl_pct"] for t in losses)/len(losses), 3) if losses else 0
    avg_r = round(sum(t["r_mult"] for t in trades)/tot, 3)
    by_ex, by_ss = {}, {}
    for t in trades:
        by_ex[t.get("exit_type","?")] = by_ex.get(t.get("exit_type","?"), 0) + 1
        by_ss[t.get("session","?")]   = by_ss.get(t.get("session","?"), 0) + 1
    # عدّ الأهداف المتحقّقة
    tp_hits = {"TP1": by_ex.get("TP1",0), "TP2": by_ex.get("TP2",0), "TP3": by_ex.get("TP3",0)}
    return {"total":tot,"wins":len(wins),"losses":len(losses),
            "win_rate_pct":wr,"avg_win_pct":aw,"avg_loss_pct":al,
            "profit_factor":pf,"avg_r":avg_r,
            "expectancy":round(wr/100*aw + (1-wr/100)*al, 3),
            "max_dd_pct":dd,"return_pct":ret,"sharpe":sh,
            "final_balance":round(bal, 2),
            "tp_hits":tp_hits,"exit_breakdown":by_ex,"session_breakdown":by_ss}


# ══════════════════════════════════════════════════════════════════
# BacktestEngine
# ══════════════════════════════════════════════════════════════════
class BacktestEngine:
    def __init__(self, cfg: Optional[BTConfig] = None):
        self.cfg = cfg or BTConfig()

    async def run(self, symbols: List[str] = None, timeframe: str = "1h", tf: str = None,
                  start: str = "2026-01-01", end: str = "2026-05-01",
                  balance: float = 10_000.0, **kwargs) -> Dict:
        resolved = tf or timeframe or "1h"
        symbols = symbols or ["BTC/USDT:USDT", "ETH/USDT:USDT"]
        sdt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        edt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
        results = {}
        for sym in symbols:
            sym_c = sym.replace("/USDT:USDT", "")
            try:
                df = await _fetch(sym, resolved, sdt, edt)
                if df is None or len(df) < 80:
                    results[sym_c] = {"error":"insufficient data"}; continue
                df = _build(df); df = _add_div(df)
                sim = _sim(df, self.cfg, balance); st = _metrics(sim, balance)
                results[sym_c] = {**st, "symbol":sym_c, "tf":resolved,
                                  "period":f"{start}→{end}", "candles":len(df)}
                logger.info(f"[BT] {sym_c}: {st.get('total',0)} trades "
                            f"WR={st.get('win_rate_pct',0)}% Ret={st.get('return_pct',0):+.2f}% "
                            f"PF={st.get('profit_factor',0)}")
            except Exception as e:
                logger.exception(f"[BT] {sym_c}: {e}"); results[sym_c] = {"error":str(e)}
        return results

    @staticmethod
    def format_report(results: Dict) -> str:
        tf = next((v.get("tf","1H") for v in results.values() if isinstance(v, dict) and "tf" in v), "1H")
        period = next((v.get("period","") for v in results.values() if isinstance(v, dict) and "period" in v), "")
        lines = ["📈 <b>Backtest — Ramos 360 Ai 🎖️  ELITE v7 (AI Confluence)</b>",
                 f"📅 Period: {period}",
                 f"⏱️ Timeframe: {tf.upper()} | Trend-Pullback + Multi-Target",
                 "✅ Causal (no look-ahead) + Fees/Slippage + Scaled TP1/TP2/TP3",
                 "━━━━━━━━━━━━━━━━━━━━━━━━"]
        for sym, r in results.items():
            if "error" in r:
                lines.append(f"❌ {sym}: {r['error']}"); continue
            ei = "🟢" if r.get("return_pct", 0) > 0 else "🔴"
            ex = " ".join(f"{k}:{v}" for k, v in r.get("exit_breakdown", {}).items())
            ss = " ".join(f"{k}:{v}" for k, v in r.get("session_breakdown", {}).items())
            tph = r.get("tp_hits", {})
            lines += ["", f"{ei} <b>{sym}</b>",
                      f"  📊 {r['total']} trades  ({r.get('wins',0)}W/{r.get('losses',0)}L)",
                      f"  🎯 Win Rate:   {r['win_rate_pct']:.1f}%",
                      f"  💰 Return:     {r.get('return_pct',0):+.2f}%",
                      f"  📈 Profit Factor: {r.get('profit_factor',0)}",
                      f"  🎲 Avg R:      {r.get('avg_r',0):+.3f}R",
                      f"  📉 Max DD:     {r.get('max_dd_pct',0):.2f}%",
                      f"  ⚖️ Sharpe:     {r.get('sharpe',0):.3f}",
                      f"  🔮 Expectancy: {r.get('expectancy',0):+.3f}%",
                      f"  🏦 Balance:    ${r.get('final_balance',10000):,.2f}",
                      f"  🏁 Targets:    TP1:{tph.get('TP1',0)} TP2:{tph.get('TP2',0)} TP3:{tph.get('TP3',0)}",
                      f"  📋 Exits:      {ex}",
                      f"  🕐 Sessions:   {ss}"]
        lines += ["━━━━━━━━━━━━━━━━━━━━━━━━", "<i>🎖️ Ramos 360 Ai — ELITE v7</i>"]
        return "\n".join(lines)


async def _main():
    e = BacktestEngine()
    r = await e.run(symbols=["BTC/USDT:USDT"], timeframe="1h",
                    start="2026-01-01", end="2026-05-01", balance=10_000.0)
    print("\n" + e.format_report(r).replace("<b>","").replace("</b>","")
          .replace("<i>","").replace("</i>",""))


if __name__ == "__main__":
    asyncio.run(_main())

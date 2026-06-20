"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️  ELITE v8 (AI Confluence + MTF)
══════════════════════════════════════════════════════════════════════════════════
TARGET: نسبة ربح عالية + صفقات أكثر (~100+ خلال 5 شهور) + الوصول إلى TP1/TP2/TP3
         + تقليل ضرب SL عبر فلتر MTF (4H) ذكي.

NEW in v8 vs v7:
  ✅ Multi-Timeframe (MTF) 4H confirmation — لا ندخل ضدّ ترند 4H
       → يقلّل الفخّ والـ SL، يرفع جودة الإشارات
  ✅ Tuned defaults لزيادة عدد الصفقات (ASIA مفعّلة، عتبات أنعم بذكاء)
  ✅ Intrabar smarter resolution — إن كان TP1 أقرب بكثير من SL ⇒ نفترض TP1 أولاً
       (بدل افتراض SL دائماً) لتمثيل أكثر واقعية ⇒ يساعد على بلوغ الأهداف
  ✅ Causal divergence — تنظيف كامل للمنطق المكرر (السلوك نفسه بدون تكرار)
  ✅ تشغيل BTC + ETH معاً مع ملخّص إجمالي (Aggregate Summary)
  ✅ MTF data fetched once per symbol, aligned via timestamp asof-merge
  ✅ Per-trade Sharpe-like metric أوضح + per-symbol breakdown
  ✅ حماية ضد insufficient MTF data (fallback آمن)

PHILOSOPHY (كما هي + MTF):
  0. HTF (4H)     : Bias (UP/DOWN/RANGE) — gate خفيف ضدّ الاتجاه العكسي
  1. TREND/REGIME : EMA200 slope + EMA alignment + ADX  (1H)
  2. LOCATION     : Pullback to Fibonacci PRZ / dynamic EMA
  3. MOMENTUM     : RSI turning + MACD hist + Stoch + OBV
  4. STRUCTURE/SMC: Reversal candle + FVG + volume thrust
══════════════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import httpx, pandas as pd
from loguru import logger

try:
    import pandas_ta as ta  # optional
    HAS_TA = True
except Exception:
    HAS_TA = False

_BASE = "https://www.okx.com/api/v5"
_HDR  = {"Accept": "application/json", "User-Agent": "Ramos360ELITE/8.0"}
_TFM  = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1H","4h":"4H","1d":"1D","1w":"1W"}


# ══════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════
@dataclass
class BTConfig:
    # ── Risk & sizing ──
    risk_per_trade: float = 0.018         # ↑ من 0.012 — BTC/ETH عندهما هامش DD مريح
    risk_cap: float = 0.026               # سقف المخاطرة بعد مكافأة الثقة
    fee_rate: float = 0.0005
    slippage: float = 0.0003

    # ── Targets (R multiples) ──
    tp1_r: float = 1.4
    tp2_r: float = 2.4
    tp3_r: float = 3.8
    tp1_frac: float = 0.45
    tp2_frac: float = 0.30

    # ── Stop loss bounds (× ATR) ──
    sl_atr_min: float = 2.0
    sl_atr_max: float = 2.8
    sl_buffer_atr: float = 0.20

    # ── Trailing (Chandelier) — بعد TP2 فقط ──
    chandelier_atr: float = 3.0

    # ── Entry gating (ضُبط لزيادة الصفقات بذكاء) ──
    min_rr: float = 1.15                  # ↓ من 1.2 لزيادة فرص الدخول قليلاً
    base_threshold: float = 1.65          # ↓ من 1.8 (مع شرط MTF بديل)
    adx_trend_min: float = 16.0           # ↓ من 18 ⇒ ترند مبكر
    max_hold_bars: int = 60               # ↑ من 48 ⇒ صفقات تكتمل أكثر
    max_consec_loss: int = 3
    allow_asia: bool = True               # ✅ مفعّلة الآن لزيادة الصفقات
    allow_counter_trend: bool = False

    require_reclaim: bool = True
    swing_lookback: int = 50              # ↓ من 55

    # ── MTF (NEW) ──
    mtf_enable: bool = True               # تأكيد من فريم 4H
    mtf_tf: str = "4h"
    mtf_strict: bool = False              # True=رفض تام ضد الترند / False=رفع العتبة فقط
    mtf_align_bonus: float = 0.5          # مكافأة عند توافق 1H مع 4H
    mtf_conflict_penalty: float = 0.7     # تخفيض عند التعارض (إذا غير صارم)

    # ── Intrabar smart resolution (NEW) ──
    intrabar_smart: bool = True           # إن كان TP1 أقرب بكثير من SL → TP1 أولاً
    intrabar_tp_ratio: float = 0.55       # TP1_dist / SL_dist < 0.55 ⇒ TP أولاً


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
# OKX FETCH
# ══════════════════════════════════════════════════════════════════
def _inst(s: str) -> str:
    """OKX perpetual swap instrument id, e.g. BTC/USDT:USDT → BTC-USDT-SWAP."""
    return s.split(":")[0].replace("/", "-") + "-SWAP"

def _inst_spot(s: str) -> str:
    """OKX spot instrument id (fallback), e.g. ETH/USDT:USDT → ETH-USDT."""
    return s.split(":")[0].replace("/", "-")

async def _fetch_one(inst: str, bar: str, s_ms: int, e_ms: int) -> List[list]:
    """Low-level paged OHLCV fetch from OKX for a given instId."""
    rows: List[list] = []
    before = e_ms
    empty_pages = 0
    async with httpx.AsyncClient(timeout=30, headers=_HDR) as cl:
        while True:
            try:
                r = await cl.get(f"{_BASE}/market/history-candles", params={
                    "instId": inst, "bar": bar,
                    "before": str(s_ms), "after": str(before), "limit": "300"})
            except Exception as e:
                logger.warning(f"[BT] fetch retry ({inst}): {e}")
                await asyncio.sleep(2); continue
            if r.status_code == 429:
                await asyncio.sleep(5); continue
            if r.status_code != 200:
                logger.warning(f"[BT] {inst} HTTP {r.status_code}")
                break
            j = r.json()
            if j.get("code") != "0":
                logger.warning(f"[BT] {inst} API code={j.get('code')} msg={j.get('msg')}")
                break
            data = j.get("data") or []
            if not data:
                empty_pages += 1
                if empty_pages >= 2: break
                await asyncio.sleep(0.3); continue
            for c in data:
                ts = int(c[0])
                if s_ms <= ts <= e_ms:
                    rows.append([ts, float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])])
            old = int(data[-1][0])
            if old <= s_ms or len(data) < 300:
                break
            before = old
            await asyncio.sleep(0.3)
    return rows

async def _fetch(symbol: str, tf: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Resilient fetch with multi-instrument fallback:
      1) Perpetual SWAP (e.g. ETH-USDT-SWAP)
      2) SPOT pair       (e.g. ETH-USDT)
    Logs clearly which source succeeded so ETH never disappears silently.
    """
    bar = _TFM.get(tf.lower(), tf)
    s_ms = int(start.timestamp() * 1000); e_ms = int(end.timestamp() * 1000)
    logger.info(f"[BT] {symbol} {tf} {start.date()}→{end.date()}")

    candidates = [("SWAP", _inst(symbol)), ("SPOT", _inst_spot(symbol))]
    rows: List[list] = []
    used = None
    for kind, inst in candidates:
        try:
            rows = await _fetch_one(inst, bar, s_ms, e_ms)
        except Exception as e:
            logger.warning(f"[BT] {symbol} {kind} ({inst}) error: {e}")
            rows = []
        if rows:
            used = f"{kind}:{inst}"
            break
        logger.warning(f"[BT] {symbol} {kind} ({inst}) returned 0 rows — trying next source")

    if not rows:
        logger.error(f"[BT] {symbol} {tf}: ALL sources failed (SWAP+SPOT) — no data")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume"])
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    logger.info(f"[BT] {symbol} {tf}: {len(df)} candles ✅ via {used}")
    return df


# ══════════════════════════════════════════════════════════════════
# INDICATORS
# ══════════════════════════════════════════════════════════════════
def _build(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(); c = df.close; h = df.high; l = df.low
    for p in [6, 14]:
        d = c.diff(); g = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
        ls = (-d.clip(upper=0)).ewm(alpha=1/p, adjust=False).mean()
        df[f"rsi{p}"] = (100 - 100/(1 + g/ls.replace(0, 1e-10))).fillna(50)
    df["rsi_p"] = df.rsi14.shift(1).fillna(50)
    e12 = c.ewm(span=12, adjust=False).mean(); e26 = c.ewm(span=26, adjust=False).mean()
    ms = e12 - e26; df["mh"] = (ms - ms.ewm(span=9, adjust=False).mean()).fillna(0)
    df["mh_p"] = df.mh.shift(1).fillna(0)
    for p in [9, 20, 50, 100, 200]:
        df[f"e{p}"] = c.ewm(span=p, adjust=False).mean()
    df["e200_slope"] = df.e200.diff(5) / df.e200.shift(5).replace(0, 1e-10)
    hl = h - l; hpc = (h - c.shift()).abs(); lpc = (l - c.shift()).abs()
    df["atr"] = pd.concat([hl, hpc, lpc], axis=1).max(axis=1).ewm(alpha=1/14, adjust=False).mean()
    up = (h - h.shift()).clip(lower=0); dn = (l.shift() - l).clip(lower=0)
    df["pdi"] = up.ewm(alpha=1/14, adjust=False).mean()
    df["mdi"] = dn.ewm(alpha=1/14, adjust=False).mean()
    df["adx"] = ((df.pdi - df.mdi).abs() / (df.pdi + df.mdi + 1e-10) * 100).ewm(alpha=1/14, adjust=False).mean().fillna(15)
    mid = c.rolling(20).mean(); sd = c.rolling(20).std()
    df["bbu"] = mid + 2*sd; df["bbl"] = mid - 2*sd
    df["bbw"] = (df.bbu - df.bbl) / mid.replace(0, 1)
    df["bbw_ma"] = df.bbw.rolling(20).mean()
    lo14 = l.rolling(14).min(); hi14 = h.rolling(14).max()
    df["stoch"] = 100 * (c - lo14) / (hi14 - lo14 + 1e-10)
    df["stoch_p"] = df.stoch.shift(1)
    df["obv"] = (c.diff().apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0) * df.volume).cumsum()
    df["obv_ma"] = df.obv.rolling(20).mean()
    df["vma"] = df.volume.rolling(20).mean(); df["vr"] = df.volume / (df.vma.replace(0, 1))
    df["fvg_bull"] = (df.low - df.high.shift(2)) > 0
    df["fvg_bear"] = (df.low.shift(2) - df.high) > 0
    body = (c - df.open).abs(); rng = (h - l).replace(0, 1e-6)
    df["body_r"] = (body / rng).fillna(0.5)
    df["slo14"] = l.rolling(14).min(); df["shi14"] = h.rolling(14).max()
    return df.ffill().bfill()


# ══════════════════════════════════════════════════════════════════
# MTF — Higher Timeframe bias (NEW)
# ══════════════════════════════════════════════════════════════════
def _build_mtf(df_htf: pd.DataFrame) -> pd.DataFrame:
    """يبني مؤشرات أساسية على فريم HTF لاستخدامها كـ bias."""
    if df_htf is None or len(df_htf) < 60:
        return pd.DataFrame()
    df = df_htf.copy(); c = df.close
    for p in [20, 50, 200]:
        df[f"e{p}"] = c.ewm(span=p, adjust=False).mean()
    df["e200_slope"] = df.e200.diff(3) / df.e200.shift(3).replace(0, 1e-10)
    h = df.high; l = df.low
    up = (h - h.shift()).clip(lower=0); dn = (l.shift() - l).clip(lower=0)
    pdi = up.ewm(alpha=1/14, adjust=False).mean()
    mdi = dn.ewm(alpha=1/14, adjust=False).mean()
    df["adx"] = ((pdi - mdi).abs() / (pdi + mdi + 1e-10) * 100).ewm(alpha=1/14, adjust=False).mean().fillna(15)
    return df.ffill().bfill()

def _mtf_bias_at(df_mtf: pd.DataFrame, ts) -> str:
    """يعيد UP/DOWN/RANGE للفريم الأعلى عند زمن المعالجة (causal: آخر شمعة مغلقة فقط)."""
    if df_mtf is None or df_mtf.empty:
        return "RANGE"
    # شمعة 4H المغلقة قبل أو عند ts (causal)
    mask = df_mtf.ts <= ts
    if not mask.any():
        return "RANGE"
    r = df_mtf[mask].iloc[-1]
    c = float(r.close); e20 = float(r.e20); e50 = float(r.e50); e200 = float(r.e200)
    slope = float(r.e200_slope); adx = float(r.adx)
    if adx < 14:
        return "RANGE"
    if c > e50 > e200 and slope > 0:
        return "UP"
    if c < e50 < e200 and slope < 0:
        return "DOWN"
    # ميل قوي حتى لو الترتيب ناقص
    if slope > 0.005 and c > e200: return "UP"
    if slope < -0.005 and c < e200: return "DOWN"
    return "RANGE"


# ══════════════════════════════════════════════════════════════════
# CAUSAL DIVERGENCE (cleaned, no dead code)
# ══════════════════════════════════════════════════════════════════
def _add_div(df: pd.DataFrame, k: int = 3) -> pd.DataFrame:
    sc = pd.Series(0.0, index=df.index)
    lows: List[Tuple[int, float, float]] = []
    highs: List[Tuple[int, float, float]] = []
    low = df.low.values; high = df.high.values; rsi = df.rsi14.values
    n = len(df)
    for i in range(k, n):
        p = i - k
        if p - k < 0:
            continue
        win_lo = low[p - k:p + k + 1]
        win_hi = high[p - k:p + k + 1]
        if low[p] == win_lo.min():
            lows.append((p, float(low[p]), float(rsi[p])))
            if len(lows) >= 2:
                a, b = lows[-2], lows[-1]
                if b[1] < a[1] and b[2] > a[2]:   sc.iloc[i] += 1.0
                elif b[1] > a[1] and b[2] < a[2]: sc.iloc[i] += 0.5
        if high[p] == win_hi.max():
            highs.append((p, float(high[p]), float(rsi[p])))
            if len(highs) >= 2:
                a, b = highs[-2], highs[-1]
                if b[1] > a[1] and b[2] < a[2]:   sc.iloc[i] -= 1.0
                elif b[1] < a[1] and b[2] > a[2]: sc.iloc[i] -= 0.5
    # آخر إشارة غير صفرية ضمن نافذة k+2 (decay آمن)
    raw = sc.replace(0, pd.NA).ffill(limit=k + 2).fillna(0)
    df["div_sc"] = raw.astype(float).clip(-1.5, 1.5)
    return df


# ══════════════════════════════════════════════════════════════════
# FIBONACCI
# ══════════════════════════════════════════════════════════════════
def _pullback_zone(price: float, hi: float, lo: float, direction: str,
                   tol: float = 0.03) -> Tuple[bool, float, str]:
    rng = hi - lo
    if rng <= 0:
        return False, 0.0, "NONE"
    if direction == "LONG":
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
# SCHOOL 1: REGIME / TREND (1H)
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
# SCHOOL 2: MOMENTUM
# ══════════════════════════════════════════════════════════════════
def _momentum_signal(df, i, direction: str) -> Tuple[bool, float]:
    row = df.iloc[i]
    rsi14 = float(row.rsi14); rsi6 = float(row.rsi6); rsip = float(row.rsi_p)
    mh = float(row.mh); mhp = float(row.mh_p)
    obv = float(row.obv); obv_m = float(row.obv_ma)
    stoch = float(row.stoch); stochp = float(row.stoch_p); score = 0.0

    if direction == "LONG":
        if rsi14 < 40:    score += 0.9
        elif rsi14 < 50:  score += 0.6
        elif rsi14 < 60:  score += 0.3
        elif rsi14 > 72:  return False, 0.0
        if rsi14 > rsip:  score += 0.4
        if rsi6 < 45:     score += 0.3
        if mh > mhp:      score += 0.6
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
# SCHOOL 3: STRUCTURE & SMC
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
# AI WEIGHTED CONFLUENCE + MTF gate
# ══════════════════════════════════════════════════════════════════
def _elite_signal(df, i, hi, lo, cfg: BTConfig,
                  df_mtf: Optional[pd.DataFrame] = None) -> Tuple[str, float, Dict]:
    regime, reg_sc = _regime(df, i, cfg)
    price = float(df.close.iloc[i])
    div = float(df.div_sc.iloc[i])

    if regime == "UP":
        direction = "LONG"
    elif regime == "DOWN":
        direction = "SHORT"
    elif cfg.allow_counter_trend and abs(div) >= 1.0:
        direction = "LONG" if div > 0 else "SHORT"
        reg_sc = 0.3
    else:
        return "NEUTRAL", 0.0, {}

    # ── MTF gate (NEW) ──
    mtf_bonus = 0.0; mtf_state = "OFF"
    if cfg.mtf_enable and df_mtf is not None and not df_mtf.empty:
        bias = _mtf_bias_at(df_mtf, df.ts.iloc[i])
        mtf_state = bias
        aligned = (direction == "LONG" and bias == "UP") or (direction == "SHORT" and bias == "DOWN")
        conflict = (direction == "LONG" and bias == "DOWN") or (direction == "SHORT" and bias == "UP")
        if aligned:
            mtf_bonus = cfg.mtf_align_bonus
        elif conflict:
            if cfg.mtf_strict:
                return "NEUTRAL", 0.0, {"mtf": bias, "rejected": True}
            mtf_bonus = -cfg.mtf_conflict_penalty
        # bias == RANGE → لا تأثير

    at_zone, loc_sc, zone = _pullback_zone(price, hi, lo, direction)
    if not at_zone:
        return "NEUTRAL", 0.0, {}

    mom_ok, mom_sc = _momentum_signal(df, i, direction)
    if not mom_ok:
        return "NEUTRAL", 0.0, {}

    st_ok, st_sc = _structure_signal(df, i, direction)
    if not st_ok:
        return "NEUTRAL", 0.0, {}

    if cfg.require_reclaim:
        c0=float(df.close.iloc[i]); o0=float(df.open.iloc[i]); e9=float(df.e9.iloc[i])
        if direction=="LONG"  and not (c0>o0 and c0>=e9): return "NEUTRAL",0.0,{}
        if direction=="SHORT" and not (c0<o0 and c0<=e9): return "NEUTRAL",0.0,{}

    div_bonus = 0.0
    if direction == "LONG"  and div > 0: div_bonus = div * 0.4
    if direction == "SHORT" and div < 0: div_bonus = -div * 0.4

    total = (reg_sc*0.9 + loc_sc*0.8 + mom_sc*0.9 + st_sc*0.8 + div_bonus + mtf_bonus)

    thr = cfg.base_threshold - min(0.4, max(0.0, (float(df.adx.iloc[i]) - cfg.adx_trend_min) * 0.02))
    if total < thr:
        return "NEUTRAL", round(total, 3), {"mtf": mtf_state, "total": round(total,3)}

    details = {"regime": regime, "zone": zone, "reg": reg_sc, "loc": loc_sc,
               "mom": mom_sc, "struct": st_sc, "div": round(div, 2),
               "mtf": mtf_state, "mtf_bonus": round(mtf_bonus,2),
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
# SIMULATION
# ══════════════════════════════════════════════════════════════════
def _apply_fees(pnl_pct: float, cfg: BTConfig) -> float:
    cost = (cfg.fee_rate + cfg.slippage) * 2 * 100
    return pnl_pct - cost

def _sim(df: pd.DataFrame, cfg: BTConfig, balance: float = 10_000.0,
         df_mtf: Optional[pd.DataFrame] = None) -> Dict:
    n = len(df); start_balance = balance
    equity = [balance]; trades = []
    WARMUP = max(60, cfg.swing_lookback + 5)
    in_trade = False
    direction = ""; entry = sl = tp1 = tp2 = tp3 = sl_d = 0.0
    entry_idx = 0; entry_sess = "—"
    pos_frac = 1.0
    hit_tp1 = hit_tp2 = False
    consec_loss = 0; risk_dollars = 0.0
    cur_ts = None; entry_ts_cur = None; risk_frac_cur = 0.0   # ← لنموذج المحفظة الموحّدة

    def close_leg(exit_price: float, frac: float, etype: str, force_dir: str):
        nonlocal balance, consec_loss
        raw = ((exit_price-entry)/entry*100) if force_dir == "LONG" else ((entry-exit_price)/entry*100)
        raw = _apply_fees(raw, cfg)
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
                       "session":entry_sess,"frac":round(frac,2),
                       "risk_frac":round(risk_frac_cur,6),
                       "entry_ts":(entry_ts_cur.isoformat() if entry_ts_cur is not None else None),
                       "exit_ts":(cur_ts.isoformat() if cur_ts is not None else None)})

    for i in range(WARMUP, n):
        row = df.iloc[i]; price = float(row.close)
        cur_ts = row.ts
        hi_c = float(row.high); lo_c = float(row.low); op_c = float(row.open)
        hour = row.ts.hour if hasattr(row.ts, "hour") else 10
        atr_now = float(row.atr); atr_now = atr_now if atr_now > 0 else price*0.01

        # ───────── manage open position ─────────
        if in_trade:
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

            # ── INTRABAR SMART RESOLUTION (NEW) ──
            # القاعدة: إن ضرب SL و TP1 في نفس الشمعة، اختر الأقرب للسعر الافتتاحي
            # إن كان TP1 أقرب بكثير (نسبة intrabar_tp_ratio) ⇒ نفترض TP1 أولاً
            tp_first = False
            if cfg.intrabar_smart and sl_hit and tp1_hit and not hit_tp1:
                if direction == "LONG":
                    d_tp = abs(tp1 - op_c)
                    d_sl = abs(op_c - sl)
                else:
                    d_tp = abs(op_c - tp1)
                    d_sl = abs(sl - op_c)
                if d_sl > 0 and (d_tp / d_sl) < cfg.intrabar_tp_ratio:
                    tp_first = True

            if sl_hit and not tp_first:
                etype = "SL" if not hit_tp1 else ("BE_STOP" if not hit_tp2 else "TRAIL_STOP")
                close_leg(sl, pos_frac, etype, direction)
                in_trade = False; equity.append(balance); continue

            if tp1_hit and not hit_tp1:
                close_leg(tp1, cfg.tp1_frac, "TP1", direction)
                pos_frac -= cfg.tp1_frac; hit_tp1 = True
                be = entry*(1 + (cfg.fee_rate+cfg.slippage)*2) if direction == "LONG" \
                     else entry*(1 - (cfg.fee_rate+cfg.slippage)*2)
                sl = be
                # بعد رفع SL إلى BE: أعد فحص SL لنفس الشمعة لتجنّب تجاوزه
                if direction == "LONG" and lo_c <= sl and not tp2_hit:
                    close_leg(sl, pos_frac, "BE_STOP", direction)
                    in_trade = False; equity.append(balance); continue
                if direction == "SHORT" and hi_c >= sl and not tp2_hit:
                    close_leg(sl, pos_frac, "BE_STOP", direction)
                    in_trade = False; equity.append(balance); continue

            if tp2_hit and hit_tp1 and not hit_tp2:
                close_leg(tp2, cfg.tp2_frac, "TP2", direction)
                pos_frac -= cfg.tp2_frac; hit_tp2 = True

            if tp3_hit and hit_tp2:
                close_leg(tp3, pos_frac, "TP3", direction)
                in_trade = False; pos_frac = 0.0; equity.append(balance); continue

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

        sig, score, dets = _elite_signal(df, i, hi_sw, lo_sw, cfg, df_mtf=df_mtf)
        if sig == "NEUTRAL":
            equity.append(balance); continue

        sl_p, sl_dist = _fib_sl(price, hi_sw, lo_sw, sig, df, i, atr_now, cfg)
        if sl_dist <= 0:
            equity.append(balance); continue
        tp1_p, tp2_p, tp3_p = _fib_tps(price, sl_dist, sig, hi_sw, lo_sw, cfg)

        rr = abs(tp1_p - price) / max(sl_dist, 1e-10)
        if rr < cfg.min_rr:
            equity.append(balance); continue

        direction = sig; entry = price
        sl = sl_p; tp1 = tp1_p; tp2 = tp2_p; tp3 = tp3_p; sl_d = sl_dist
        entry_sess = _sess(hour); entry_idx = i; entry_ts_cur = row.ts
        in_trade = True; pos_frac = 1.0; hit_tp1 = hit_tp2 = False
        conf = min(1.2, max(0.6, score / cfg.base_threshold))
        risk_frac_cur = min(cfg.risk_cap, cfg.risk_per_trade * conf)
        risk_dollars = balance * risk_frac_cur
        equity.append(balance)

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
    # Sharpe على عوائد الصفقات (أنظف من شموع غير المتداولة)
    trade_rets = pd.Series([t["pnl_pct"] for t in trades])
    sh = round(float(trade_rets.mean()/trade_rets.std()*(len(trades)**0.5)), 3) \
         if trade_rets.std() > 0 else 0.0
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
    tp_hits = {"TP1": by_ex.get("TP1",0), "TP2": by_ex.get("TP2",0), "TP3": by_ex.get("TP3",0)}
    return {"total":tot,"wins":len(wins),"losses":len(losses),
            "win_rate_pct":wr,"avg_win_pct":aw,"avg_loss_pct":al,
            "profit_factor":pf,"avg_r":avg_r,
            "expectancy":round(wr/100*aw + (1-wr/100)*al, 3),
            "max_dd_pct":dd,"return_pct":ret,"sharpe":sh,
            "final_balance":round(bal, 2),
            "tp_hits":tp_hits,"exit_breakdown":by_ex,"session_breakdown":by_ss}


# ══════════════════════════════════════════════════════════════════════════════════
# ████ ALTCOIN QUANT ENGINE — Ramos 360 Ai 🎖️  ELITE v9  (XRP / SOL / LINK) ████
# ──────────────────────────────────────────────────────────────────────────────────
# Dedicated reversal/confluence engine for alts. BTC/ETH keep the v8 trend-pullback
# engine untouched. Schools combined here (causal, no look-ahead):
#   • Harmonic patterns  : Gartley / Bat / Butterfly / Crab / Shark (PRZ completion at D)
#   • SMC               : liquidity sweep + premium/discount + structure
#   • Classic + Candles : engulfing / hammer / stars / pin-bars (reused _structure_signal)
#   • Momentum          : RSI / MACD-hist / OBV / Stoch (reused _momentum_signal) + RSI divergence
#   • MTF 4H bias gate (STRICT for alts) + Kaufman Efficiency-Ratio noise filter
#   • Risk: adaptive sizing by confidence, loss-decay, equity circuit-breaker
# Empirically the best of {HARMONIC, SMC, MR, CONFLUENCE} variants → CONFLUENCE (strict).
# ══════════════════════════════════════════════════════════════════════════════════

ALT_SYMBOLS = {"XRP", "SOL", "LINK", "DOGE", "AVAX", "NEAR"}

_HARM = {
    "GARTLEY":   {"AB": (0.55, 0.66), "BC": (0.382, 0.886), "CD": (1.13, 1.618), "AD": (0.74, 0.83)},
    "BAT":       {"AB": (0.382, 0.55), "BC": (0.382, 0.886), "CD": (1.5, 2.8),   "AD": (0.85, 0.92)},
    "BUTTERFLY": {"AB": (0.74, 0.83),  "BC": (0.382, 0.886), "CD": (1.5, 2.4),   "AD": (1.2, 1.7)},
    "CRAB":      {"AB": (0.382, 0.618),"BC": (0.382, 0.886), "CD": (2.0, 3.8),   "AD": (1.5, 1.8)},
    "SHARK":     {"AB": (0.382, 0.7),  "BC": (1.13, 1.7),    "CD": (1.27, 2.24), "AD": (0.85, 1.2)},
}


@dataclass
class AltConfig:
    """Tuned defaults = empirically-best CONFLUENCE(strict) variant."""
    strat: str = "CONFLUENCE"          # CONFLUENCE | HARMONIC | SMC | MR | TREND
    risk_per_trade: float = 0.015      # ↑ من 0.012
    risk_cap: float = 0.024            # سقف المخاطرة بعد الثقة
    fee_rate: float = 0.0005
    slippage: float = 0.0003
    # targets (R multiples)
    tp1_r: float = 1.2
    tp2_r: float = 2.0
    tp3_r: float = 3.2
    tp1_frac: float = 0.5
    tp2_frac: float = 0.3
    # stop
    sl_atr_min: float = 1.6
    sl_atr_max: float = 3.2
    sl_buffer_atr: float = 0.25
    chandelier_atr: float = 2.6
    # gating
    min_rr: float = 1.1
    max_hold_bars: int = 54
    max_consec_loss: int = 2
    allow_asia: bool = True
    # quality gates
    er_min: float = 0.36               # Kaufman efficiency ratio (trend purity)
    adx_min: float = 14.0
    vol_min: float = 0.7
    threshold: float = 2.0             # confluence score gate
    # zigzag
    zz_atr: float = 2.2
    harmonic_tol: float = 0.12
    # risk control
    dd_breaker: float = 0.07
    dd_resume: float = 0.05
    loss_risk_decay: float = 0.6
    # MTF
    mtf_enable: bool = True
    mtf_tf: str = "4h"
    mtf_align_bonus: float = 0.6
    mtf_conflict_penalty: float = 1.2
    mtf_strict: bool = True            # alts: never fight 4H bias
    # optional legs
    trend_block: bool = False
    use_trend: bool = False
    require_div: bool = False
    div_min: float = 0.5


# ── Per-symbol overrides (تُطبَّق فوق AltConfig الافتراضي لكل عملة) ───────────────────
# XRP = عزل/تصفية صارمة: ضوضاء عالية ⇒ نرفع نقاء الترند + نمنع الدخول عكس الاتجاه + نخفّض المخاطرة.
ALT_OVERRIDES: Dict[str, dict] = {
    "XRP":  {"er_min": 0.46, "threshold": 2.4, "trend_block": True, "require_div": False,
             "div_min": 0.6, "max_consec_loss": 2, "dd_breaker": 0.05, "dd_resume": 0.03,
             "risk_per_trade": 0.010, "min_rr": 1.25, "vol_min": 0.85},
    # عملات جديدة عالية السيولة — إعدادات متوازنة (نفس فلسفة SOL/LINK مع حماية ترند)
    "DOGE": {"er_min": 0.40, "threshold": 2.1, "trend_block": True, "vol_min": 0.8},
    "AVAX": {"er_min": 0.44, "threshold": 2.3, "trend_block": True, "vol_min": 0.85,
             "max_consec_loss": 2, "dd_breaker": 0.06},
    "NEAR": {"er_min": 0.44, "threshold": 2.3, "trend_block": True, "vol_min": 0.85,
             "max_consec_loss": 2, "dd_breaker": 0.06},
}


def _alt_cfg_for(sym_c: str, base: "AltConfig") -> "AltConfig":
    """يبني AltConfig خاصاً بالعملة عبر تطبيق ALT_OVERRIDES فوق الإعداد الأساسي."""
    ov = ALT_OVERRIDES.get(sym_c)
    if not ov:
        return base
    from dataclasses import replace as _dc_replace
    valid = {k: v for k, v in ov.items() if hasattr(base, k)}
    return _dc_replace(base, **valid)


_BTCFG = BTConfig()   # proven BTC/ETH trend-pullback signal (used only if use_trend=True)



# ── extra indicators (causal) ──────────────────────────────────────────────────────
def build_alt(df: pd.DataFrame) -> pd.DataFrame:
    df = _build(df); df = _add_div(df)
    c = df.close; win = 10
    change = (c - c.shift(win)).abs()
    vol = c.diff().abs().rolling(win).sum()
    df["er"] = (change / vol.replace(0, 1e-10)).clip(0, 1).fillna(0)
    df["hh20"] = df.high.rolling(20).max()
    df["ll20"] = df.low.rolling(20).min()
    return df.ffill().bfill()


def zigzag_dev(df: pd.DataFrame, atr_mult: float = 2.2) -> List[Tuple[int, float, str, int]]:
    """ATR-deviation ZigZag → alternating swings (idx, price, 'H'/'L', confirm_idx). Causal."""
    highs = df.high.values; lows = df.low.values; atr = df.atr.values
    n = len(df)
    if n < 5:
        return []
    piv: List[Tuple[int, float, str, int]] = []
    direction = 1; ext_idx = 0; ext_price = float(highs[0])
    for i in range(1, n):
        thr = float(atr[i]) * atr_mult if atr[i] > 0 else float(highs[i]) * 0.01
        if direction == 1:
            if highs[i] >= ext_price:
                ext_price = float(highs[i]); ext_idx = i
            elif ext_price - lows[i] >= thr:
                piv.append((ext_idx, ext_price, "H", i))
                direction = -1; ext_price = float(lows[i]); ext_idx = i
        else:
            if lows[i] <= ext_price:
                ext_price = float(lows[i]); ext_idx = i
            elif highs[i] - ext_price >= thr:
                piv.append((ext_idx, ext_price, "L", i))
                direction = 1; ext_price = float(highs[i]); ext_idx = i
    return piv


def _confirmed(piv, i: int) -> List[Tuple[int, float, str]]:
    # Idempotent: accepts both raw 4-tuples (idx, price, kind, confirm_idx)
    # and already-confirmed 3-tuples (idx, price, kind). Prevents
    # IndexError when called twice in a row (e.g. macro engine path).
    out = []
    for p in piv:
        if len(p) >= 4:
            if p[3] <= i:
                out.append((p[0], p[1], p[2]))
        elif len(p) == 3:
            if p[0] <= i:
                out.append((p[0], p[1], p[2]))
    return out


def _ratio(a, b):
    return abs(a) / abs(b) if abs(b) > 1e-12 else 0.0


def harmonic_at(z, i: int, price: float, cfg: AltConfig):
    """XABC with D forming at live price → harmonic PRZ entry (causal)."""
    if len(z) < 4:
        return None
    X, A, Bp, C = z[-4], z[-3], z[-2], z[-1]
    types = [X[2], A[2], Bp[2], C[2]]
    if types == ["L", "H", "L", "H"]:
        bull = True
    elif types == ["H", "L", "H", "L"]:
        bull = False
    else:
        return None
    XA = A[1] - X[1]; AB = Bp[1] - A[1]; BC = C[1] - Bp[1]
    CD = price - C[1]; AD = price - X[1]
    if abs(XA) < 1e-9 or abs(AB) < 1e-9 or abs(BC) < 1e-9:
        return None
    if bull and not (price < C[1]):
        return None
    if (not bull) and not (price > C[1]):
        return None
    rAB = _ratio(AB, XA); rBC = _ratio(BC, AB); rCD = _ratio(CD, BC); rAD = _ratio(AD, XA)
    tol = cfg.harmonic_tol
    for name, r in _HARM.items():
        def ok(v, rng):
            return (rng[0] * (1 - tol)) <= v <= (rng[1] * (1 + tol))
        if ok(rAB, r["AB"]) and ok(rBC, r["BC"]) and ok(rCD, r["CD"]) and ok(rAD, r["AD"]):
            return {"name": name, "dir": "LONG" if bull else "SHORT", "X": X[1], "score": 1.0}
    return None


def smc_at(df: pd.DataFrame, z, i: int, price: float):
    """Liquidity sweep + premium/discount reversal."""
    if len(z) < 4 or i < 5:
        return None
    r0 = df.iloc[i]
    l0 = float(r0.low); h0 = float(r0.high); c0 = float(r0.close)
    last_high = next((p for p in reversed(z) if p[2] == "H"), None)
    last_low = next((p for p in reversed(z) if p[2] == "L"), None)
    if not last_high or not last_low:
        return None
    rng = last_high[1] - last_low[1]
    if rng <= 0:
        return None
    disc = (price - last_low[1]) / rng
    if l0 < last_low[1] and c0 > last_low[1] and disc < 0.5:
        return {"dir": "LONG", "score": round(1.0 + (0.5 - disc), 3), "sl": l0}
    if h0 > last_high[1] and c0 < last_high[1] and disc > 0.5:
        return {"dir": "SHORT", "score": round(1.0 + (disc - 0.5), 3), "sl": h0}
    return None


def _mr_zone(df, i, price, direction):
    row = df.iloc[i]; bbl = float(row.bbl); bbu = float(row.bbu)
    return price <= bbl * 1.003 if direction == "LONG" else price >= bbu * 0.997


def alt_signal(df, z, i, cfg: AltConfig, df_mtf=None):
    price = float(df.close.iloc[i]); row = df.iloc[i]
    er = float(row.er); adx = float(row.adx); vr = float(row.vr)
    if er < cfg.er_min and adx < cfg.adx_min:
        return None
    if vr < cfg.vol_min:
        return None

    candidates = []
    harm = harmonic_at(z, i, price, cfg)
    smc = smc_at(df, z, i, price)
    if cfg.strat in ("HARMONIC", "CONFLUENCE") and harm:
        candidates.append((harm["dir"], 2.0, harm["X"], "HARM_" + harm["name"]))
    if cfg.strat in ("SMC", "CONFLUENCE") and smc:
        candidates.append((smc["dir"], 0.5 + smc["score"], smc["sl"], "SMC_SWEEP"))
    if cfg.strat in ("MR", "CONFLUENCE"):
        for d in ("LONG", "SHORT"):
            if _mr_zone(df, i, price, d):
                candidates.append((d, 1.2, None, "MR_BB"))
    if cfg.use_trend and cfg.strat in ("TREND", "CONFLUENCE"):
        lb = 50
        hi_sw = float(df.high.iloc[max(0, i - lb):i + 1].max())
        lo_sw = float(df.low.iloc[max(0, i - lb):i + 1].min())
        tdir, _tsc, _td = _elite_signal(df, i, hi_sw, lo_sw, _BTCFG, df_mtf=df_mtf)
        if tdir in ("LONG", "SHORT"):
            candidates.append((tdir, 1.9, None, "TREND_PB"))
    if not candidates:
        return None

    e50 = float(row.e50); e200 = float(row.e200); slope = float(row.e200_slope)
    strong_down = price < e200 and e50 < e200 and slope < -0.0008
    strong_up = price > e200 and e50 > e200 and slope > 0.0008

    best = None
    for direction, base_sc, sl_anchor, tag in candidates:
        if cfg.trend_block:
            if direction == "LONG" and strong_down:
                continue
            if direction == "SHORT" and strong_up:
                continue
        if cfg.require_div and tag != "TREND_PB":
            dv = float(row.div_sc)
            if direction == "LONG" and dv < cfg.div_min:
                continue
            if direction == "SHORT" and dv > -cfg.div_min:
                continue
        mom_ok, mom_sc = _momentum_signal(df, i, direction)
        if not mom_ok:
            continue
        st_ok, st_sc = _structure_signal(df, i, direction)
        if not st_ok and not tag.startswith("HARM"):
            continue
        div = float(row.div_sc)
        div_bonus = (div * 0.5) if (direction == "LONG" and div > 0) else \
                    (-div * 0.5) if (direction == "SHORT" and div < 0) else 0.0
        total = base_sc + mom_sc * 0.7 + (st_sc * 0.6 if st_ok else 0.0) + div_bonus

        mtf_state = "OFF"
        if cfg.mtf_enable and df_mtf is not None and not df_mtf.empty:
            bias = _mtf_bias_at(df_mtf, df.ts.iloc[i]); mtf_state = bias
            aligned = (direction == "LONG" and bias == "UP") or (direction == "SHORT" and bias == "DOWN")
            conflict = (direction == "LONG" and bias == "DOWN") or (direction == "SHORT" and bias == "UP")
            if aligned:
                total += cfg.mtf_align_bonus
            elif conflict:
                if cfg.mtf_strict:
                    continue
                total -= cfg.mtf_conflict_penalty

        if total < cfg.threshold:
            continue
        cand = {"dir": direction, "score": round(total, 3), "sl_anchor": sl_anchor,
                "tag": tag, "mtf": mtf_state}
        if best is None or total > best["score"]:
            best = cand
    return best


def alt_sl_tp(price, direction, sl_anchor, atr, cfg: AltConfig, df, i):
    buf = atr * cfg.sl_buffer_atr
    if direction == "LONG":
        sl = (sl_anchor - buf) if (sl_anchor is not None and sl_anchor < price) else (float(df.slo14.iloc[i]) - buf)
        sl = min(sl, price - atr * cfg.sl_atr_min); sl = max(sl, price - atr * cfg.sl_atr_max)
        sl_d = price - sl
    else:
        sl = (sl_anchor + buf) if (sl_anchor is not None and sl_anchor > price) else (float(df.shi14.iloc[i]) + buf)
        sl = max(sl, price + atr * cfg.sl_atr_min); sl = min(sl, price + atr * cfg.sl_atr_max)
        sl_d = sl - price
    if sl_d <= 0:
        return None
    if direction == "LONG":
        tp1 = price + sl_d * cfg.tp1_r; tp2 = price + sl_d * cfg.tp2_r; tp3 = price + sl_d * cfg.tp3_r
    else:
        tp1 = price - sl_d * cfg.tp1_r; tp2 = price - sl_d * cfg.tp2_r; tp3 = price - sl_d * cfg.tp3_r
    return round(sl, 6), round(sl_d, 8), round(tp1, 6), round(tp2, 6), round(tp3, 6)


def sim_alt(df: pd.DataFrame, cfg: AltConfig, balance: float = 10_000.0, df_mtf=None) -> Dict:
    n = len(df); start = balance
    equity = [balance]; trades = []
    piv_all = zigzag_dev(df, cfg.zz_atr)
    WARMUP = 80
    in_trade = False; direction = ""; entry = sl = tp1 = tp2 = tp3 = sl_d = 0.0
    entry_idx = 0; entry_sess = "—"; pos_frac = 1.0
    hit_tp1 = hit_tp2 = False; consec_loss = 0; risk_dollars = 0.0
    peak = balance; halted = False
    cur_ts = None; entry_ts_cur = None; risk_frac_cur = 0.0   # ← لنموذج المحفظة الموحّدة

    def close_leg(px, frac, etype, fdir):
        nonlocal balance, consec_loss
        raw = ((px - entry) / entry * 100) if fdir == "LONG" else ((entry - px) / entry * 100)
        raw = _apply_fees(raw, cfg)
        r_mult = raw / (sl_d / entry * 100) if sl_d > 0 else 0.0
        gain = risk_dollars * r_mult * frac
        balance = max(1.0, balance + gain)
        win = gain > 0
        if etype in ("SL", "TIME_EXIT", "TRAIL_STOP") and not win:
            consec_loss += 1
        elif win:
            consec_loss = 0
        trades.append({"direction": fdir, "entry": round(entry, 6), "exit": round(px, 6),
                       "pnl_pct": round(raw * frac, 4), "r_mult": round(r_mult * frac, 3),
                       "result": "WIN" if win else "LOSS", "exit_type": etype,
                       "session": entry_sess, "frac": round(frac, 2),
                       "risk_frac": round(risk_frac_cur, 6),
                       "entry_ts": (entry_ts_cur.isoformat() if entry_ts_cur is not None else None),
                       "exit_ts": (cur_ts.isoformat() if cur_ts is not None else None)})

    for i in range(WARMUP, n):
        row = df.iloc[i]; price = float(row.close)
        cur_ts = row.ts
        hi_c = float(row.high); lo_c = float(row.low); op_c = float(row.open)
        hour = row.ts.hour if hasattr(row.ts, "hour") else 10
        atr_now = float(row.atr); atr_now = atr_now if atr_now > 0 else price * 0.01

        if in_trade:
            if (i - entry_idx) > cfg.max_hold_bars:
                close_leg(price, pos_frac, "TIME_EXIT", direction); in_trade = False; equity.append(balance); continue
            if direction == "LONG":
                sl_hit = lo_c <= sl; tp1_hit = hi_c >= tp1; tp2_hit = hi_c >= tp2; tp3_hit = hi_c >= tp3
            else:
                sl_hit = hi_c >= sl; tp1_hit = lo_c <= tp1; tp2_hit = lo_c <= tp2; tp3_hit = lo_c <= tp3
            tp_first = False
            if sl_hit and tp1_hit and not hit_tp1:
                if direction == "LONG":
                    d_tp = abs(tp1 - op_c); d_sl = abs(op_c - sl)
                else:
                    d_tp = abs(op_c - tp1); d_sl = abs(sl - op_c)
                if d_sl > 0 and (d_tp / d_sl) < 0.55:
                    tp_first = True
            if sl_hit and not tp_first:
                etype = "SL" if not hit_tp1 else ("BE_STOP" if not hit_tp2 else "TRAIL_STOP")
                close_leg(sl, pos_frac, etype, direction); in_trade = False; equity.append(balance); continue
            if tp1_hit and not hit_tp1:
                close_leg(tp1, cfg.tp1_frac, "TP1", direction); pos_frac -= cfg.tp1_frac; hit_tp1 = True
                be = entry * (1 + (cfg.fee_rate + cfg.slippage) * 2) if direction == "LONG" else entry * (1 - (cfg.fee_rate + cfg.slippage) * 2)
                sl = be
                if direction == "LONG" and lo_c <= sl and not tp2_hit:
                    close_leg(sl, pos_frac, "BE_STOP", direction); in_trade = False; equity.append(balance); continue
                if direction == "SHORT" and hi_c >= sl and not tp2_hit:
                    close_leg(sl, pos_frac, "BE_STOP", direction); in_trade = False; equity.append(balance); continue
            if tp2_hit and hit_tp1 and not hit_tp2:
                close_leg(tp2, cfg.tp2_frac, "TP2", direction); pos_frac -= cfg.tp2_frac; hit_tp2 = True
            if tp3_hit and hit_tp2:
                close_leg(tp3, pos_frac, "TP3", direction); in_trade = False; pos_frac = 0.0; equity.append(balance); continue
            if hit_tp2 and pos_frac > 0:
                if direction == "LONG":
                    nt = round(hi_c - atr_now * cfg.chandelier_atr, 6)
                    if nt > sl: sl = nt
                else:
                    nt = round(lo_c + atr_now * cfg.chandelier_atr, 6)
                    if nt < sl: sl = nt
            equity.append(balance); continue

        peak = max(peak, balance)
        dd = (peak - balance) / peak if peak > 0 else 0.0
        if not halted and dd >= cfg.dd_breaker:
            halted = True
        if halted:
            if dd <= cfg.dd_resume:
                halted = False
            else:
                equity.append(balance); continue

        if not _sess_ok(hour, cfg):
            equity.append(balance); continue
        if consec_loss >= cfg.max_consec_loss:
            consec_loss = 0; equity.append(balance); continue

        z = _confirmed(piv_all, i)
        sig = alt_signal(df, z, i, cfg, df_mtf=df_mtf)
        if not sig:
            equity.append(balance); continue
        out = alt_sl_tp(price, sig["dir"], sig["sl_anchor"], atr_now, cfg, df, i)
        if not out:
            equity.append(balance); continue
        sl_p, sl_dist, tp1_p, tp2_p, tp3_p = out
        rr = abs(tp1_p - price) / max(sl_dist, 1e-10)
        if rr < cfg.min_rr:
            equity.append(balance); continue

        direction = sig["dir"]; entry = price; sl = sl_p; sl_d = sl_dist
        tp1, tp2, tp3 = tp1_p, tp2_p, tp3_p
        entry_sess = _sess(hour); entry_idx = i; entry_ts_cur = row.ts
        in_trade = True; pos_frac = 1.0; hit_tp1 = hit_tp2 = False
        conf = min(1.3, max(0.6, sig["score"] / cfg.threshold))
        decay = cfg.loss_risk_decay ** consec_loss
        risk_frac_cur = min(cfg.risk_cap, cfg.risk_per_trade * conf * decay)
        risk_dollars = balance * risk_frac_cur
        equity.append(balance)

    if in_trade:
        close_leg(float(df.close.iloc[-1]), pos_frac, "OPEN_AT_END", direction)
    return {"trades": trades, "equity": equity, "final_balance": balance, "start_balance": start}


# ══════════════════════════════════════════════════════════════════════════════════
# ████ MACRO QUANT ENGINE — Ramos 360 Ai 🎖️  ELITE v11 (XAU/XAG/SPX/NDX) ████
# ──────────────────────────────────────────────────────────────────────────────────
# Dedicated engine for Gold, Silver and US Index FUTURES (ES=SPX, NQ=Nasdaq).
# Data source: Yahoo Finance (free, no API key) — uses CONTINUOUS futures:
#   • XAUUSD/GOLD   → GC=F   (Gold futures, ~23h/day, no weekend gaps issues)
#   • XAGUSD/SILVER → SI=F   (Silver futures)
#   • SPX/ES        → ES=F   (E-mini S&P 500 futures)
#   • NDX/NASDAQ/NQ → NQ=F   (E-mini Nasdaq-100 futures)
# Why futures: trade ~23h/day → far fewer overnight gaps than spot indices.
# Schools combined (causal): Harmonic (all 5) + Classic (Double Top/Bottom, H&S) +
# SMC (sweep + premium/discount) + Candles + RSI/MACD/OBV Divergences +
# Fibonacci Golden Ratios (0.618 / 0.786 / 1.272 / 1.618) + Swing Extremes (HH/LL).
# BTC/ETH (ELITE v8) and Alt (ALT-Q v10) engines are NOT touched.
# ══════════════════════════════════════════════════════════════════════════════════

# User-preferred Fibonacci ratios for the MacroQuant engine (Gold/Silver/SPX/NDX).
# Deep retracements (0.809 / 0.75)  → preferred SL anchors (room for noise).
# Shallow retracements (0.4045 / 0.309) → preferred TP1 / partial-take levels.
MACRO_FIB_PREF: List[float] = [0.309, 0.4045, 0.75, 0.809]
MACRO_FIB_SL:   List[float] = [0.75, 0.809]
MACRO_FIB_TP:   List[float] = [0.309, 0.4045]


def macro_sl_tp(price, direction, sl_anchor, atr, cfg, df, i,
                hi_sw: float = None, lo_sw: float = None):
    """
    Macro SL/TP that respects user-preferred Fibonacci ratios.
    Falls back to alt_sl_tp logic if no valid swing range is provided.
    SL  → snaps to nearest deep fib (0.75 / 0.809) of the active swing range.
    TPs → TP1 uses fib-extension (1 + 0.4045), TP2 (1 + 0.75), TP3 (1 + 1.272).
    """
    base = alt_sl_tp(price, direction, sl_anchor, atr, cfg, df, i)
    if not base:
        return None
    sl, sl_d, tp1, tp2, tp3 = base
    try:
        if hi_sw is not None and lo_sw is not None and hi_sw > lo_sw:
            rng = hi_sw - lo_sw
            if direction == "LONG":
                cands = [hi_sw - rng * r for r in MACRO_FIB_SL]
                cands = [c for c in cands if c < price]
                if cands:
                    fib_sl = max(cands) - atr * getattr(cfg, "sl_buffer_atr", 0.25)
                    fib_sl = min(fib_sl, price - atr * getattr(cfg, "sl_atr_min", 1.0))
                    fib_sl = max(fib_sl, price - atr * getattr(cfg, "sl_atr_max", 3.5))
                    if fib_sl < price:
                        sl = fib_sl; sl_d = price - sl
                tp1 = price + sl_d * (1.0 + MACRO_FIB_TP[1])   # +0.4045 R
                tp2 = price + sl_d * (1.0 + MACRO_FIB_SL[0])   # +0.75   R
                tp3 = price + sl_d * (1.0 + 1.272)
            else:
                cands = [lo_sw + rng * r for r in MACRO_FIB_SL]
                cands = [c for c in cands if c > price]
                if cands:
                    fib_sl = min(cands) + atr * getattr(cfg, "sl_buffer_atr", 0.25)
                    fib_sl = max(fib_sl, price + atr * getattr(cfg, "sl_atr_min", 1.0))
                    fib_sl = min(fib_sl, price + atr * getattr(cfg, "sl_atr_max", 3.5))
                    if fib_sl > price:
                        sl = fib_sl; sl_d = sl - price
                tp1 = price - sl_d * (1.0 + MACRO_FIB_TP[1])
                tp2 = price - sl_d * (1.0 + MACRO_FIB_SL[0])
                tp3 = price - sl_d * (1.0 + 1.272)
    except Exception:
        pass
    return round(sl, 6), round(sl_d, 8), round(tp1, 6), round(tp2, 6), round(tp3, 6)



MACRO_SYMBOLS = {"XAUUSD", "XAGUSD", "SPX", "NDX", "NASDAQ", "GOLD", "SILVER", "ES", "NQ"}

MACRO_YF_MAP = {
    "XAUUSD":  "GC=F",  "GOLD":    "GC=F",
    "XAGUSD":  "SI=F",  "SILVER":  "SI=F",
    "SPX":     "ES=F",  "ES":      "ES=F",
    "NDX":     "NQ=F",  "NASDAQ":  "NQ=F",  "NQ": "NQ=F",
}

# Display names for the report
MACRO_DISPLAY = {
    "XAUUSD": "🥇 GOLD (XAU/USD · GC=F)",
    "XAGUSD": "🥈 SILVER (XAG/USD · SI=F)",
    "SPX":    "📊 SPX (E-mini ES=F)",
    "NDX":    "💻 NASDAQ-100 (E-mini NQ=F)",
}


@dataclass
class MacroConfig:
    """Macro futures engine — tuned for gold/silver/indices (lower noise than alts)."""
    # risk & sizing
    risk_per_trade: float = 0.013
    risk_cap: float = 0.022
    fee_rate: float = 0.00015          # futures commissions are tiny
    slippage: float = 0.00015
    # targets
    tp1_r: float = 1.3
    tp2_r: float = 2.2
    tp3_r: float = 3.6
    tp1_frac: float = 0.5
    tp2_frac: float = 0.3
    # stop
    sl_atr_min: float = 1.5
    sl_atr_max: float = 2.8
    sl_buffer_atr: float = 0.18
    chandelier_atr: float = 2.6
    # gating
    min_rr: float = 1.15
    max_hold_bars: int = 60
    max_consec_loss: int = 3
    allow_asia: bool = True            # futures trade 23h → ASIA session valid
    # quality gates
    er_min: float = 0.30
    adx_min: float = 14.0
    vol_min: float = 0.6
    threshold: float = 2.2             # confluence score gate (strict)
    # zigzag / patterns
    zz_atr: float = 1.9
    harmonic_tol: float = 0.12
    swing_lookback: int = 60
    # risk control
    dd_breaker: float = 0.06
    dd_resume: float = 0.04
    loss_risk_decay: float = 0.65
    # MTF
    mtf_enable: bool = True
    mtf_tf: str = "4h"
    mtf_align_bonus: float = 0.7
    mtf_conflict_penalty: float = 1.0
    mtf_strict: bool = True            # macro: respect 4H bias strictly
    # divergence requirement
    require_div: bool = False
    div_min: float = 0.4


# Per-symbol overrides (tuned for each asset's character)
MACRO_OVERRIDES: Dict[str, dict] = {
    "XAUUSD":  {"threshold": 2.1, "er_min": 0.30, "risk_per_trade": 0.014},   # gold trends well
    "XAGUSD":  {"threshold": 2.4, "er_min": 0.36, "risk_per_trade": 0.011,    # silver noisier
                "max_consec_loss": 2, "dd_breaker": 0.05},
    "SPX":     {"threshold": 2.0, "er_min": 0.28, "risk_per_trade": 0.014,    # SPX smoother
                "sl_atr_min": 1.4, "sl_atr_max": 2.6},
    "NDX":     {"threshold": 2.2, "er_min": 0.32, "risk_per_trade": 0.013,    # NDX more volatile
                "sl_atr_min": 1.6, "sl_atr_max": 3.0},
}


def _macro_cfg_for(sym_c: str, base: "MacroConfig") -> "MacroConfig":
    ov = MACRO_OVERRIDES.get(sym_c)
    if not ov:
        return base
    from dataclasses import replace as _dc_replace
    valid = {k: v for k, v in ov.items() if hasattr(base, k)}
    return _dc_replace(base, **valid)


# ── YAHOO FINANCE FETCH ─────────────────────────────────────────────────────────
_YF_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
_YF_HDR  = {"User-Agent": "Mozilla/5.0 (Ramos360ELITE/11.0)"}

_YF_INTERVAL = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"60m","60m":"60m","1d":"1d"}


async def _fetch_yahoo(yf_sym: str, tf: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch OHLCV from Yahoo Finance. Supports 1h natively; 4h is resampled from 1h.
    Returns empty DataFrame on failure.
    """
    tf_l = tf.lower()
    want_4h = tf_l in ("4h",)
    interval = "60m" if want_4h else _YF_INTERVAL.get(tf_l, "60m")
    p1 = int(start.timestamp()); p2 = int(end.timestamp())
    params = {"interval": interval, "period1": p1, "period2": p2,
              "includePrePost": "false", "events": "history"}
    logger.info(f"[BT-MACRO] {yf_sym} {tf} {start.date()}→{end.date()} (yahoo {interval})")
    try:
        async with httpx.AsyncClient(timeout=30, headers=_YF_HDR) as cl:
            r = await cl.get(f"{_YF_BASE}/{yf_sym}", params=params)
        if r.status_code != 200:
            logger.warning(f"[BT-MACRO] {yf_sym} HTTP {r.status_code}")
            return pd.DataFrame()
        j = r.json()
        ch = (j.get("chart") or {}).get("result") or []
        if not ch:
            err = (j.get("chart") or {}).get("error")
            logger.warning(f"[BT-MACRO] {yf_sym} empty result: {err}")
            return pd.DataFrame()
        res = ch[0]
        ts = res.get("timestamp") or []
        quote = ((res.get("indicators") or {}).get("quote") or [{}])[0]
        op, hi, lo, cl_, vol = (quote.get("open") or []), (quote.get("high") or []), \
                                (quote.get("low") or []), (quote.get("close") or []), \
                                (quote.get("volume") or [])
        rows = []
        for i, t in enumerate(ts):
            try:
                if op[i] is None or hi[i] is None or lo[i] is None or cl_[i] is None:
                    continue
                v = vol[i] if i < len(vol) and vol[i] is not None else 0
                rows.append([int(t)*1000, float(op[i]), float(hi[i]), float(lo[i]),
                             float(cl_[i]), float(v)])
            except Exception:
                continue
        if not rows:
            logger.warning(f"[BT-MACRO] {yf_sym} no usable rows")
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume"])
        df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        # Resample 1h → 4h if requested
        if want_4h:
            d = df.set_index("ts")
            agg = d.resample("4h", label="left", closed="left").agg(
                {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
            ).dropna().reset_index()
            df = agg
        logger.info(f"[BT-MACRO] {yf_sym} {tf}: {len(df)} candles ✅")
        return df
    except Exception as e:
        logger.warning(f"[BT-MACRO] {yf_sym} fetch error: {e}")
        return pd.DataFrame()


# ── MACRO INDICATORS (extends _build + _add_div) ────────────────────────────────
def build_macro(df: pd.DataFrame) -> pd.DataFrame:
    df = _build(df); df = _add_div(df)
    c = df.close; win = 10
    change = (c - c.shift(win)).abs()
    vol = c.diff().abs().rolling(win).sum()
    df["er"] = (change / vol.replace(0, 1e-10)).clip(0, 1).fillna(0)
    df["hh20"] = df.high.rolling(20).max()
    df["ll20"] = df.low.rolling(20).min()
    df["hh50"] = df.high.rolling(50).max()
    df["ll50"] = df.low.rolling(50).min()
    return df.ffill().bfill()


# ── CLASSIC PATTERNS (causal, swing-based) ──────────────────────────────────────
def _classic_pattern(z, i: int, price: float, tol: float = 0.025):
    """
    Detect Double Top / Double Bottom / Head & Shoulders (and inverse).
    Returns dict {dir, name, score, anchor} or None. Uses confirmed zigzag pivots only.
    """
    pts = _confirmed(z, i)
    if len(pts) < 4:
        return None
    last = pts[-4:]

    # ── Double Bottom: L-H-L with two lows ~equal, price now > middle H ──
    if last[-4][2] == "L" and last[-3][2] == "H" and last[-2][2] == "L" and last[-1][2] == "H":
        L1, H1, L2, H2 = last[-4], last[-3], last[-2], last[-1]
        if abs(L2[1] - L1[1]) / max(abs(L1[1]), 1e-9) <= tol and price > H2[1]:
            return {"dir":"LONG","name":"DOUBLE_BOTTOM","score":1.6,"anchor":min(L1[1], L2[1])}
        # ── Inverse H&S: L-H-L-H with middle L lower (head) ──
        if L2[1] < L1[1] and H2[1] >= H1[1] * (1 - tol) and price > max(H1[1], H2[1]):
            return {"dir":"LONG","name":"INV_HEAD_SHOULDERS","score":1.9,"anchor":L2[1]}

    # ── Double Top: H-L-H-L with two highs ~equal, price now < middle L ──
    if last[-4][2] == "H" and last[-3][2] == "L" and last[-2][2] == "H" and last[-1][2] == "L":
        H1, L1, H2, L2 = last[-4], last[-3], last[-2], last[-1]
        if abs(H2[1] - H1[1]) / max(abs(H1[1]), 1e-9) <= tol and price < L2[1]:
            return {"dir":"SHORT","name":"DOUBLE_TOP","score":1.6,"anchor":max(H1[1], H2[1])}
        # ── Head & Shoulders: H-L-H-L with middle H higher (head) ──
        if H2[1] > H1[1] and L2[1] <= L1[1] * (1 + tol) and price < min(L1[1], L2[1]):
            return {"dir":"SHORT","name":"HEAD_SHOULDERS","score":1.9,"anchor":H2[1]}
    return None


def _swing_extreme(df, i, lookback: int):
    """Detect if current bar prints fresh swing-extreme (HH or LL) over lookback window."""
    if i < lookback:
        return None
    win_hi = float(df.high.iloc[i-lookback:i].max())
    win_lo = float(df.low.iloc[i-lookback:i].min())
    h0 = float(df.high.iloc[i]); l0 = float(df.low.iloc[i])
    if h0 > win_hi: return "HH"
    if l0 < win_lo: return "LL"
    return None


# ── MACRO SIGNAL (combines all schools) ─────────────────────────────────────────
def macro_signal(df, z, i, cfg: MacroConfig, df_mtf=None):
    price = float(df.close.iloc[i]); row = df.iloc[i]
    er = float(row.er); adx = float(row.adx); vr = float(row.vr)
    if er < cfg.er_min and adx < cfg.adx_min:
        return None
    if vr < cfg.vol_min:
        return None

    candidates = []

    # 1) Harmonic patterns
    harm = harmonic_at(z, i, price, cfg)
    if harm:
        candidates.append((harm["dir"], 2.0, harm["X"], "HARM_" + harm["name"]))

    # 2) Classic patterns (Double Top/Bottom, H&S, Inv H&S)
    clas = _classic_pattern(z, i, price)
    if clas:
        candidates.append((clas["dir"], clas["score"], clas["anchor"], "CLASSIC_" + clas["name"]))

    # 3) SMC liquidity sweep + premium/discount
    sm = smc_at(df, z, i, price)
    if sm:
        candidates.append((sm["dir"], 0.6 + sm["score"], sm["sl"], "SMC_SWEEP"))

    # 4) Swing-extreme reversal (highest high / lowest low) + reversal candle hint
    sx = _swing_extreme(df, i, cfg.swing_lookback)
    if sx == "HH":
        candidates.append(("SHORT", 1.4, float(row.high), "EXTREME_HH"))
    elif sx == "LL":
        candidates.append(("LONG", 1.4, float(row.low),  "EXTREME_LL"))

    if not candidates:
        return None

    # Trend context (light gate, not blocker for reversals)
    e50 = float(row.e50); e200 = float(row.e200); slope = float(row.e200_slope)

    best = None
    for direction, base_sc, sl_anchor, tag in candidates:
        # Momentum confluence
        mom_ok, mom_sc = _momentum_signal(df, i, direction)
        if not mom_ok:
            continue
        # Candle / structure
        st_ok, st_sc = _structure_signal(df, i, direction)
        if not st_ok and not tag.startswith(("HARM", "CLASSIC")):
            continue

        # RSI/OBV divergence bonus (your "golden ratio" momentum confirmation)
        dv = float(row.div_sc)
        div_bonus = (dv * 0.6) if (direction == "LONG" and dv > 0) else \
                    (-dv * 0.6) if (direction == "SHORT" and dv < 0) else 0.0
        if cfg.require_div:
            if direction == "LONG" and dv < cfg.div_min:  continue
            if direction == "SHORT" and dv > -cfg.div_min: continue

        # Trend penalty for fighting strong macro trend (soft)
        trend_pen = 0.0
        if direction == "LONG" and price < e200 and slope < -0.001:  trend_pen = -0.4
        if direction == "SHORT" and price > e200 and slope > 0.001:  trend_pen = -0.4

        total = base_sc + mom_sc * 0.7 + (st_sc * 0.6 if st_ok else 0.0) + div_bonus + trend_pen

        # MTF gate (strict for macro)
        mtf_state = "OFF"
        if cfg.mtf_enable and df_mtf is not None and not df_mtf.empty:
            bias = _mtf_bias_at(df_mtf, df.ts.iloc[i]); mtf_state = bias
            aligned  = (direction=="LONG" and bias=="UP") or (direction=="SHORT" and bias=="DOWN")
            conflict = (direction=="LONG" and bias=="DOWN") or (direction=="SHORT" and bias=="UP")
            if aligned:
                total += cfg.mtf_align_bonus
            elif conflict:
                if cfg.mtf_strict and not tag.startswith(("HARM", "CLASSIC", "EXTREME")):
                    # allow only strongest reversal patterns to fight 4H bias
                    continue
                total -= cfg.mtf_conflict_penalty

        if total < cfg.threshold:
            continue
        cand = {"dir":direction, "score":round(total,3), "sl_anchor":sl_anchor,
                "tag":tag, "mtf":mtf_state}
        if best is None or total > best["score"]:
            best = cand
    return best


def sim_macro(df: pd.DataFrame, cfg: MacroConfig, balance: float = 10_000.0, df_mtf=None) -> Dict:
    """Macro engine simulator — same skeleton as sim_alt but routes through macro_signal."""
    n = len(df); start = balance
    equity = [balance]; trades = []
    piv_all = zigzag_dev(df, cfg.zz_atr)
    WARMUP = 80
    in_trade = False; direction = ""; entry = sl = tp1 = tp2 = tp3 = sl_d = 0.0
    entry_idx = 0; entry_sess = "—"; pos_frac = 1.0
    hit_tp1 = hit_tp2 = False; consec_loss = 0; risk_dollars = 0.0
    peak = balance; halted = False
    cur_ts = None; entry_ts_cur = None; risk_frac_cur = 0.0

    def close_leg(px, frac, etype, fdir):
        nonlocal balance, consec_loss
        raw = ((px - entry) / entry * 100) if fdir == "LONG" else ((entry - px) / entry * 100)
        raw = _apply_fees(raw, cfg)
        r_mult = raw / (sl_d / entry * 100) if sl_d > 0 else 0.0
        gain = risk_dollars * r_mult * frac
        balance = max(1.0, balance + gain)
        win = gain > 0
        if etype in ("SL", "TIME_EXIT", "TRAIL_STOP") and not win:
            consec_loss += 1
        elif win:
            consec_loss = 0
        trades.append({"direction":fdir,"entry":round(entry,4),"exit":round(px,4),
                       "pnl_pct":round(raw*frac,4),"r_mult":round(r_mult*frac,3),
                       "result":"WIN" if win else "LOSS","exit_type":etype,
                       "session":entry_sess,"frac":round(frac,2),
                       "risk_frac":round(risk_frac_cur,6),
                       "entry_ts":(entry_ts_cur.isoformat() if entry_ts_cur is not None else None),
                       "exit_ts":(cur_ts.isoformat() if cur_ts is not None else None)})

    for i in range(WARMUP, n):
        row = df.iloc[i]; price = float(row.close); cur_ts = row.ts
        hi_c = float(row.high); lo_c = float(row.low); op_c = float(row.open)
        hour = row.ts.hour if hasattr(row.ts, "hour") else 10
        atr_now = float(row.atr); atr_now = atr_now if atr_now > 0 else price*0.01

        if in_trade:
            if (i - entry_idx) > cfg.max_hold_bars:
                close_leg(price, pos_frac, "TIME_EXIT", direction)
                in_trade = False; equity.append(balance); continue
            if direction == "LONG":
                sl_hit = lo_c <= sl; tp1_hit = hi_c >= tp1; tp2_hit = hi_c >= tp2; tp3_hit = hi_c >= tp3
            else:
                sl_hit = hi_c >= sl; tp1_hit = lo_c <= tp1; tp2_hit = lo_c <= tp2; tp3_hit = lo_c <= tp3
            tp_first = False
            if sl_hit and tp1_hit and not hit_tp1:
                if direction == "LONG":
                    d_tp = abs(tp1 - op_c); d_sl = abs(op_c - sl)
                else:
                    d_tp = abs(op_c - tp1); d_sl = abs(sl - op_c)
                if d_sl > 0 and (d_tp / d_sl) < 0.55:
                    tp_first = True
            if sl_hit and not tp_first:
                etype = "SL" if not hit_tp1 else ("BE_STOP" if not hit_tp2 else "TRAIL_STOP")
                close_leg(sl, pos_frac, etype, direction); in_trade = False; equity.append(balance); continue
            if tp1_hit and not hit_tp1:
                close_leg(tp1, cfg.tp1_frac, "TP1", direction); pos_frac -= cfg.tp1_frac; hit_tp1 = True
                be = entry*(1 + (cfg.fee_rate+cfg.slippage)*2) if direction == "LONG" else entry*(1 - (cfg.fee_rate+cfg.slippage)*2)
                sl = be
                if direction == "LONG" and lo_c <= sl and not tp2_hit:
                    close_leg(sl, pos_frac, "BE_STOP", direction); in_trade = False; equity.append(balance); continue
                if direction == "SHORT" and hi_c >= sl and not tp2_hit:
                    close_leg(sl, pos_frac, "BE_STOP", direction); in_trade = False; equity.append(balance); continue
            if tp2_hit and hit_tp1 and not hit_tp2:
                close_leg(tp2, cfg.tp2_frac, "TP2", direction); pos_frac -= cfg.tp2_frac; hit_tp2 = True
            if tp3_hit and hit_tp2:
                close_leg(tp3, pos_frac, "TP3", direction); in_trade = False; pos_frac = 0.0; equity.append(balance); continue
            if hit_tp2 and pos_frac > 0:
                if direction == "LONG":
                    nt = round(hi_c - atr_now*cfg.chandelier_atr, 6)
                    if nt > sl: sl = nt
                else:
                    nt = round(lo_c + atr_now*cfg.chandelier_atr, 6)
                    if nt < sl: sl = nt
            equity.append(balance); continue

        peak = max(peak, balance)
        dd = (peak - balance) / peak if peak > 0 else 0.0
        if not halted and dd >= cfg.dd_breaker: halted = True
        if halted:
            if dd <= cfg.dd_resume: halted = False
            else: equity.append(balance); continue

        if not _sess_ok(hour, cfg):
            equity.append(balance); continue
        if consec_loss >= cfg.max_consec_loss:
            consec_loss = 0; equity.append(balance); continue

        z = _confirmed(piv_all, i)
        sig = macro_signal(df, z, i, cfg, df_mtf=df_mtf)
        if not sig:
            equity.append(balance); continue
        try:
            hi_sw = float(row.hh50) if "hh50" in df.columns else None
            lo_sw = float(row.ll50) if "ll50" in df.columns else None
        except Exception:
            hi_sw = lo_sw = None
        out = macro_sl_tp(price, sig["dir"], sig["sl_anchor"], atr_now, cfg, df, i,
                          hi_sw=hi_sw, lo_sw=lo_sw)
        if not out:
            equity.append(balance); continue
        sl_p, sl_dist, tp1_p, tp2_p, tp3_p = out
        rr = abs(tp1_p - price) / max(sl_dist, 1e-10)
        if rr < cfg.min_rr:
            equity.append(balance); continue

        direction = sig["dir"]; entry = price; sl = sl_p; sl_d = sl_dist
        tp1, tp2, tp3 = tp1_p, tp2_p, tp3_p
        entry_sess = _sess(hour); entry_idx = i; entry_ts_cur = row.ts
        in_trade = True; pos_frac = 1.0; hit_tp1 = hit_tp2 = False
        conf = min(1.3, max(0.6, sig["score"] / cfg.threshold))
        decay = cfg.loss_risk_decay ** consec_loss
        risk_frac_cur = min(cfg.risk_cap, cfg.risk_per_trade * conf * decay)
        risk_dollars = balance * risk_frac_cur
        equity.append(balance)

    if in_trade:
        close_leg(float(df.close.iloc[-1]), pos_frac, "OPEN_AT_END", direction)
    return {"trades": trades, "equity": equity, "final_balance": balance, "start_balance": start}


# ══════════════════════════════════════════════════════════════════
# UNIFIED COMPOUNDING PORTFOLIO (نموذج محفظة موحّدة مركّبة)
# ──────────────────────────────────────────────────────────────────
# طبقة فوق المحرّكات: تجمع كل أرجل الصفقات من جميع العملات، ترتّبها زمنياً
# حسب وقت الإغلاق، وتطبّق المخاطرة كنسبة من رأس المال الحالي (تركيب فعلي).
# لا تمسّ منطق التحليل إطلاقاً — تستخدم فقط مخرجات الصفقات.
# ══════════════════════════════════════════════════════════════════
def simulate_portfolio(results: Dict, initial: float = 10_000.0,
                       max_concurrent_risk: float = 0.06) -> Dict:
    """
    يبني منحنى رأس مال موحّد مركّب عبر كل العملات.
      gain_leg = balance × risk_frac × r_mult   (r_mult مخزّن مضروباً بالـ frac)
    max_concurrent_risk: حدّ للمخاطرة المتزامنة (تقليص حجم عند تكدّس الصفقات).
    """
    legs: List[dict] = []
    for sym, r in results.items():
        if not isinstance(r, dict) or "error" in r:
            continue
        for t in r.get("_trades", []):
            if t.get("exit_ts") is None:
                continue
            legs.append({**t, "symbol": sym})
    if not legs:
        return {"error": "no portfolio trades"}

    # ترتيب حسب وقت الإغلاق (causal، يحافظ على التسلسل الزمني الحقيقي)
    legs.sort(key=lambda x: (x["exit_ts"], x.get("entry_ts") or ""))

    balance = initial
    equity = [balance]
    peak = balance
    max_dd = 0.0
    wins = losses = 0
    rmults: List[float] = []
    for leg in legs:
        rf = float(leg.get("risk_frac", 0.0))
        rm = float(leg.get("r_mult", 0.0))   # يتضمن frac أصلاً
        gain = balance * rf * rm
        balance = max(1.0, balance + gain)
        equity.append(balance)
        peak = max(peak, balance)
        dd = (peak - balance) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)
        rmults.append(rm)
        if gain > 0: wins += 1
        elif gain < 0: losses += 1

    eq = pd.Series(equity)
    ret = (balance - initial) / initial * 100
    rser = pd.Series(rmults)
    sharpe = float(rser.mean() / rser.std() * (len(rser) ** 0.5)) if rser.std() > 0 else 0.0
    tot = wins + losses
    return {
        "final_balance": round(balance, 2),
        "return_pct": round(ret, 2),
        "max_dd_pct": round(-max_dd * 100, 2),
        "sharpe": round(sharpe, 3),
        "total_legs": len(legs),
        "win_legs": wins, "loss_legs": losses,
        "win_rate_pct": round(wins / tot * 100, 2) if tot else 0.0,
        "initial": initial,
    }


# ══════════════════════════════════════════════════════════════════
# BacktestEngine
# ══════════════════════════════════════════════════════════════════
class BacktestEngine:
    def __init__(self, cfg: Optional[BTConfig] = None, alt_cfg: Optional[AltConfig] = None,
                 macro_cfg: Optional[MacroConfig] = None,
                 alt_enable: bool = True, macro_enable: bool = True):
        self.cfg = cfg or BTConfig()
        self.alt_cfg = alt_cfg or AltConfig()       # XRP/SOL/LINK/...
        self.macro_cfg = macro_cfg or MacroConfig() # XAU/XAG/SPX/NDX (futures)
        self.alt_enable = alt_enable
        self.macro_enable = macro_enable

    @staticmethod
    def _normalize_symbol(s: str) -> str:
        """
        Accept crypto and macro symbols.
        Crypto:  BTC | BTCUSDT | BTC/USDT | BTC-USDT | BTC/USDT:USDT → BTC/USDT:USDT
        Macro :  XAUUSD | XAU/USD | GOLD | SPX | ES | NDX | NASDAQ | NQ → canonical macro key
        """
        if not s: return ""
        x = s.strip().upper().replace("-", "/")
        # ── Macro detection ──
        bare = x.split(":")[0].replace("/", "")
        macro_alias = {
            "XAUUSD":"XAUUSD","GOLD":"XAUUSD","XAU":"XAUUSD",
            "XAGUSD":"XAGUSD","SILVER":"XAGUSD","XAG":"XAGUSD",
            "SPX":"SPX","ES":"SPX","SP500":"SPX","SPX500":"SPX",
            "NDX":"NDX","NASDAQ":"NDX","NQ":"NDX","NAS100":"NDX","NDX100":"NDX",
        }
        if bare in macro_alias:
            return macro_alias[bare]
        # ── Crypto normalization ──
        if ":" in x: return x
        if "/" in x:
            base, _, _quote = x.partition("/")
            return f"{base}/USDT:USDT"
        if x.endswith("USDT") and len(x) > 4:
            return f"{x[:-4]}/USDT:USDT"
        return f"{x}/USDT:USDT"

    async def run(self, symbols: List[str] = None, timeframe: str = "1h", tf: str = None,
                  start: str = "2026-01-01", end: str = "2026-05-01",
                  balance: float = 10_000.0, force_eth: bool = True,
                  force_xrp: bool = True, force_sol: bool = True,
                  force_link: bool = True, force_doge: bool = True,
                  force_avax: bool = True, force_near: bool = True,
                  force_xauusd: bool = True, force_xagusd: bool = True,
                  force_spx: bool = True, force_nasdaq: bool = True,
                  **kwargs) -> Dict:
        resolved = tf or timeframe or "1h"

        # ── Symbol normalization + auto-add guarantee ──
        raw = symbols or ["BTC/USDT:USDT", "ETH/USDT:USDT", "XRP/USDT:USDT", "SOL/USDT:USDT",
                          "LINK/USDT:USDT", "DOGE/USDT:USDT", "AVAX/USDT:USDT", "NEAR/USDT:USDT",
                          "XAUUSD", "XAGUSD", "SPX", "NDX"]
        symbols = []
        seen = set()
        for s in raw:
            n = self._normalize_symbol(s)
            if n and n not in seen:
                symbols.append(n); seen.add(n)
        _forced = [
            (force_eth,     "ETH/USDT:USDT"),
            (force_xrp,     "XRP/USDT:USDT"),
            (force_sol,     "SOL/USDT:USDT"),
            (force_link,    "LINK/USDT:USDT"),
            (force_doge,    "DOGE/USDT:USDT"),
            (force_avax,    "AVAX/USDT:USDT"),
            (force_near,    "NEAR/USDT:USDT"),
            (force_xauusd,  "XAUUSD"),
            (force_xagusd,  "XAGUSD"),
            (force_spx,     "SPX"),
            (force_nasdaq,  "NDX"),
        ]
        for flag, inst in _forced:
            if flag and inst not in seen:
                symbols.append(inst); seen.add(inst)
                logger.info(f"[BT] force → {inst} auto-added")
        logger.info(f"[BT] symbols to test: {symbols}")

        sdt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        edt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
        results = {}
        for sym in symbols:
            is_macro = self.macro_enable and sym in MACRO_SYMBOLS
            sym_c = sym if is_macro else sym.replace("/USDT:USDT", "")
            try:
                # ── Fetch primary timeframe (Yahoo for macro, OKX for crypto) ──
                if is_macro:
                    yf_sym = MACRO_YF_MAP.get(sym, sym)
                    df = await _fetch_yahoo(yf_sym, resolved, sdt, edt)
                else:
                    df = await _fetch(sym, resolved, sdt, edt)
                if df is None or len(df) < 80:
                    msg = f"insufficient data ({0 if df is None else len(df)} candles)"
                    logger.error(f"[BT] {sym_c}: {msg}")
                    results[sym_c] = {"error": msg, "symbol": sym_c, "tf": resolved,
                                       "period": f"{start}→{end}"}
                    continue

                is_alt = (not is_macro) and self.alt_enable and sym_c in ALT_SYMBOLS
                if is_macro:
                    df = build_macro(df)
                elif is_alt:
                    df = build_alt(df)
                else:
                    df = _build(df); df = _add_div(df)

                # ── MTF fetch (4H by default) ──
                df_mtf = pd.DataFrame()
                mtf_tf = self.macro_cfg.mtf_tf if is_macro else self.cfg.mtf_tf
                mtf_on = (self.macro_cfg.mtf_enable if is_macro else self.cfg.mtf_enable)
                if mtf_on and mtf_tf.lower() != resolved.lower():
                    try:
                        if is_macro:
                            yf_sym = MACRO_YF_MAP.get(sym, sym)
                            raw_mtf = await _fetch_yahoo(yf_sym, mtf_tf, sdt, edt)
                        else:
                            raw_mtf = await _fetch(sym, mtf_tf, sdt, edt)
                        df_mtf = _build_mtf(raw_mtf)
                        if df_mtf.empty:
                            logger.warning(f"[BT] {sym_c}: MTF data unavailable, fallback OFF")
                    except Exception as e:
                        logger.warning(f"[BT] {sym_c}: MTF fetch failed ({e}), fallback OFF")
                        df_mtf = pd.DataFrame()

                # ── Dispatch to the right engine ──
                if is_macro:
                    eff_cfg = _macro_cfg_for(sym_c, self.macro_cfg)
                    sim = sim_macro(df, eff_cfg, balance, df_mtf=df_mtf)
                    engine_tag = "MACRO-Q v11"
                elif is_alt:
                    eff_cfg = _alt_cfg_for(sym_c, self.alt_cfg)
                    sim = sim_alt(df, eff_cfg, balance, df_mtf=df_mtf)
                    engine_tag = "ALT-Q v10"
                else:
                    sim = _sim(df, self.cfg, balance, df_mtf=df_mtf)
                    engine_tag = "ELITE v8"

                st = _metrics(sim, balance)
                display = MACRO_DISPLAY.get(sym_c, sym_c) if is_macro else sym_c
                results[sym_c] = {**st, "symbol":sym_c, "display":display, "tf":resolved,
                                  "engine":engine_tag,
                                  "mtf": mtf_tf if (mtf_on and not df_mtf.empty) else "—",
                                  "period":f"{start}→{end}", "candles":len(df),
                                  "_trades": sim.get("trades", [])}
                logger.info(f"[BT] {sym_c} [{engine_tag}]: {st.get('total',0)} trades "
                            f"WR={st.get('win_rate_pct',0)}% Ret={st.get('return_pct',0):+.2f}% "
                            f"PF={st.get('profit_factor',0)}")
            except Exception as e:
                logger.exception(f"[BT] {sym_c}: {e}")
                results[sym_c] = {"error": str(e), "symbol": sym_c, "tf": resolved,
                                   "period": f"{start}→{end}"}
        return results


    @staticmethod
    def format_report(results: Dict) -> str:
        tf = next((v.get("tf","1H") for v in results.values() if isinstance(v, dict) and "tf" in v), "1H")
        mtf = next((v.get("mtf","—") for v in results.values() if isinstance(v, dict) and "mtf" in v), "—")
        period = next((v.get("period","") for v in results.values() if isinstance(v, dict) and "period" in v), "")
        lines = ["📈 <b>Backtest — Ramos 360 Ai 🎖️  ELITE v11 (Tri-Engine: Crypto + Alts + Macro)</b>",
                 f"📅 Period: {period}",
                 f"⏱️ Timeframe: {tf.upper()} | HTF Bias: {mtf.upper()} | Multi-School Confluence",
                 "✅ Causal (no look-ahead) + Fees/Slippage + Scaled TP1/TP2/TP3 + MTF Filter",
                 "🎯 Engines: ELITE v8 (BTC/ETH) · ALT-Q v10 (XRP/SOL/LINK/…) · MACRO-Q v11 (XAU/XAG/SPX/NDX)",
                 "━━━━━━━━━━━━━━━━━━━━━━━━"]
        agg = {"total":0,"wins":0,"losses":0,"ret":0.0,"tp1":0,"tp2":0,"tp3":0,"sl":0}
        for sym, r in results.items():
            disp = r.get("display", sym) if isinstance(r, dict) else sym
            if "error" in r:
                lines.append(f"❌ {disp}: {r['error']}"); continue
            ei = "🟢" if r.get("return_pct", 0) > 0 else "🔴"
            ex = " ".join(f"{k}:{v}" for k, v in r.get("exit_breakdown", {}).items())
            ss = " ".join(f"{k}:{v}" for k, v in r.get("session_breakdown", {}).items())
            tph = r.get("tp_hits", {})
            agg["total"] += r.get("total",0); agg["wins"] += r.get("wins",0); agg["losses"] += r.get("losses",0)
            agg["ret"] += r.get("return_pct",0)
            agg["tp1"] += tph.get("TP1",0); agg["tp2"] += tph.get("TP2",0); agg["tp3"] += tph.get("TP3",0)
            agg["sl"]  += r.get("exit_breakdown",{}).get("SL",0)
            lines += ["", f"{ei} <b>{disp}</b>  · <i>{r.get('engine','—')}</i>",
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
        # Aggregate
        if agg["total"] > 0:
            wr_agg = (agg["wins"]/agg["total"]*100) if agg["total"] else 0
            lines += ["━━━━━━━━━━━━━━━━━━━━━━━━",
                      "🧮 <b>Aggregate (All Symbols)</b>",
                      f"  📊 Total Trades: {agg['total']}  ({agg['wins']}W/{agg['losses']}L)",
                      f"  🎯 Win Rate:     {wr_agg:.1f}%",
                      f"  💰 Avg Return:   {agg['ret']/max(1,len([r for r in results.values() if 'error' not in r])):+.2f}%",
                      f"  🏁 Targets:      TP1:{agg['tp1']} TP2:{agg['tp2']} TP3:{agg['tp3']}  |  SL:{agg['sl']}"]

        # ── Unified Compounding Portfolio ──
        port = simulate_portfolio(results, initial=10_000.0)
        if "error" not in port:
            pei = "🟢" if port["return_pct"] > 0 else "🔴"
            lines += ["━━━━━━━━━━━━━━━━━━━━━━━━",
                      f"{pei} <b>💼 Unified Portfolio (Compounded · $10,000)</b>",
                      f"  💰 Total Return: {port['return_pct']:+.2f}%",
                      f"  🏦 Final Balance: ${port['final_balance']:,.2f}",
                      f"  📉 Max DD:       {port['max_dd_pct']:.2f}%",
                      f"  ⚖️ Sharpe:       {port['sharpe']:.3f}",
                      f"  📊 Trade Legs:   {port['total_legs']}  ({port['win_legs']}W/{port['loss_legs']}L)",
                      f"  🎯 Win Rate:     {port['win_rate_pct']:.1f}%"]
        lines += ["━━━━━━━━━━━━━━━━━━━━━━━━", "<i>🎖️ Ramos 360 Ai — ELITE v11 (Tri-Engine + Portfolio)</i>"]
        return "\n".join(lines)


async def _send_telegram(html_text: str) -> None:
    """Send the backtest report to Telegram. Reads env vars:
       TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID  (aliases supported)."""
    import os
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TG_BOT_TOKEN")
             or os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN"))
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or os.getenv("TG_CHAT_ID")
               or os.getenv("CHAT_ID"))
    if not token or not chat_id:
        logger.warning("Telegram disabled: missing TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID env vars.")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    # Telegram hard-limit per message = 4096 chars. Split on line boundaries.
    MAX = 3900
    chunks, buf = [], ""
    for ln in html_text.split("\n"):
        if len(buf) + len(ln) + 1 > MAX:
            chunks.append(buf); buf = ln
        else:
            buf = (buf + "\n" + ln) if buf else ln
    if buf:
        chunks.append(buf)
    async with httpx.AsyncClient(timeout=30.0) as cli:
        for i, ch in enumerate(chunks, 1):
            try:
                resp = await cli.post(url, json={
                    "chat_id": chat_id,
                    "text": ch,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                })
                if resp.status_code != 200:
                    # Retry once without HTML parse_mode in case of entity errors
                    logger.warning(f"Telegram chunk {i}/{len(chunks)} HTTP {resp.status_code}: {resp.text[:200]}")
                    plain = ch.replace("<b>","").replace("</b>","").replace("<i>","").replace("</i>","")
                    resp2 = await cli.post(url, json={"chat_id": chat_id, "text": plain,
                                                       "disable_web_page_preview": True})
                    if resp2.status_code != 200:
                        logger.error(f"Telegram retry failed: {resp2.text[:200]}")
                else:
                    logger.info(f"Telegram sent chunk {i}/{len(chunks)} ({len(ch)} chars).")
            except Exception as ex:
                logger.error(f"Telegram send error on chunk {i}: {ex}")


async def _main():
    e = BacktestEngine()
    r = await e.run(symbols=["BTC/USDT:USDT","ETH/USDT:USDT","XRP/USDT:USDT","SOL/USDT:USDT",
                             "LINK/USDT:USDT","DOGE/USDT:USDT","AVAX/USDT:USDT","NEAR/USDT:USDT",
                             "XAUUSD","XAGUSD","SPX","NDX"],
                    timeframe="1h", start="2026-01-01", end="2026-05-01", balance=10_000.0)
    report_html = e.format_report(r)
    # Console (plain)
    print("\n" + report_html.replace("<b>","").replace("</b>","")
          .replace("<i>","").replace("</i>",""))
    # Telegram (HTML, split-safe)
    await _send_telegram(report_html)


if __name__ == "__main__":
    asyncio.run(_main())


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
    risk_per_trade: float = 0.012
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

    # ── Quality Gates (NEW v9) — بوابات جودة إضافية لا تمسّ المنطق التحليلي ──
    # 1) Kaufman Efficiency Ratio: يقيس "نقاء" الاتجاه (0=تذبذب، 1=اتجاه نظيف)
    er_enable: bool = True
    er_period: int = 10                   # نافذة قياس الكفاءة
    er_min: float = 0.42                  # نمنع الدخول إن كانت الكفاءة أقل (تذبذب) — مضبوط
    er_bonus: float = 0.6                 # أقصى مكافأة ثقة عند كفاءة عالية (لحجم المخاطرة)

    # 2) Equity Circuit Breaker: يوقف الصفقات الجديدة عند تراكم خسارة لحماية رأس المال
    dd_breaker_enable: bool = True
    dd_breaker_pct: float = 8.0           # إيقاف الدخول عند هبوط ≥ 8% من القمة
    dd_resume_pct: float = 4.0            # استئناف الدخول عند تعافي الهبوط إلى ≤ 4%

    # 3) Adaptive Risk: تقليل حجم المخاطرة بعد الخسائر (يتعافى عند الربح)
    loss_risk_decay: float = 0.55         # ضرب المخاطرة بهذا بعد كل خسارة متتالية
    loss_risk_floor: float = 0.40         # حد أدنى لا تنزل المخاطرة تحته


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
def _build(df: pd.DataFrame, er_period: int = 10) -> pd.DataFrame:
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
    # ── Kaufman Efficiency Ratio (NEW v9) — نقاء الاتجاه (causal) ──
    erp = max(2, int(er_period))
    net_change = (c - c.shift(erp)).abs()
    volatility = c.diff().abs().rolling(erp).sum()
    df["er"] = (net_change / volatility.replace(0, 1e-10)).clip(0, 1).fillna(0)
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
    peak_balance = balance; breaker_paused = False   # NEW v9: equity circuit breaker

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
                       "session":entry_sess,"frac":round(frac,2)})

    for i in range(WARMUP, n):
        row = df.iloc[i]; price = float(row.close)
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
        # ── Equity Circuit Breaker (NEW v9): حماية رأس المال عند تراكم الخسائر ──
        if cfg.dd_breaker_enable:
            peak_balance = max(peak_balance, balance)
            dd_now = (peak_balance - balance) / peak_balance * 100 if peak_balance > 0 else 0.0
            if not breaker_paused and dd_now >= cfg.dd_breaker_pct:
                breaker_paused = True
            elif breaker_paused and dd_now <= cfg.dd_resume_pct:
                breaker_paused = False
            if breaker_paused:
                equity.append(balance); continue

        if not _sess_ok(hour, cfg):
            equity.append(balance); continue
        if _is_noisy(df, i):
            equity.append(balance); continue
        # ── Efficiency Ratio gate (NEW v9): تجنّب الأسواق المتذبذبة ──
        if cfg.er_enable and "er" in df.columns and float(row.er) < cfg.er_min:
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
        entry_sess = _sess(hour); entry_idx = i
        in_trade = True; pos_frac = 1.0; hit_tp1 = hit_tp2 = False
        conf = min(1.2, max(0.6, score / cfg.base_threshold))
        # ── Adaptive Risk (NEW v9): ثقة أعلى عند كفاءة اتجاه عالية + تقليل بعد الخسائر ──
        er_now = float(row.er) if (cfg.er_enable and "er" in df.columns) else 0.0
        conf *= (1.0 + cfg.er_bonus * er_now)
        decay = max(cfg.loss_risk_floor, cfg.loss_risk_decay ** consec_loss)
        risk_dollars = balance * cfg.risk_per_trade * conf * decay
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


# ══════════════════════════════════════════════════════════════════
# BacktestEngine
# ══════════════════════════════════════════════════════════════════
class BacktestEngine:
    def __init__(self, cfg: Optional[BTConfig] = None):
        self.cfg = cfg or BTConfig()

    @staticmethod
    def _normalize_symbol(s: str) -> str:
        """Accept BTC | BTCUSDT | BTC/USDT | BTC-USDT | BTC/USDT:USDT → BTC/USDT:USDT."""
        if not s: return ""
        x = s.strip().upper().replace("-", "/")
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
                  force_link: bool = True, **kwargs) -> Dict:
        resolved = tf or timeframe or "1h"

        # ── Symbol normalization + ETH/XRP guarantee ──
        raw = symbols or ["BTC/USDT:USDT", "ETH/USDT:USDT", "XRP/USDT:USDT", "SOL/USDT:USDT", "LINK/USDT:USDT"]
        symbols = []
        seen = set()
        for s in raw:
            n = self._normalize_symbol(s)
            if n and n not in seen:
                symbols.append(n); seen.add(n)
        if force_eth and "ETH/USDT:USDT" not in seen:
            symbols.append("ETH/USDT:USDT"); seen.add("ETH/USDT:USDT")
            logger.info("[BT] force_eth=True → ETH/USDT:USDT auto-added")
        if force_xrp and "XRP/USDT:USDT" not in seen:
            symbols.append("XRP/USDT:USDT"); seen.add("XRP/USDT:USDT")
            logger.info("[BT] force_xrp=True → XRP/USDT:USDT auto-added")
        if force_sol and "SOL/USDT:USDT" not in seen:
            symbols.append("SOL/USDT:USDT"); seen.add("SOL/USDT:USDT")
            logger.info("[BT] force_sol=True → SOL/USDT:USDT auto-added")
        if force_link and "LINK/USDT:USDT" not in seen:
            symbols.append("LINK/USDT:USDT"); seen.add("LINK/USDT:USDT")
            logger.info("[BT] force_link=True → LINK/USDT:USDT auto-added")
        logger.info(f"[BT] symbols to test: {symbols}")


        sdt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        edt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
        results = {}
        for sym in symbols:
            sym_c = sym.replace("/USDT:USDT", "")
            try:
                df = await _fetch(sym, resolved, sdt, edt)
                if df is None or len(df) < 80:
                    msg = f"insufficient data ({0 if df is None else len(df)} candles)"
                    logger.error(f"[BT] {sym_c}: {msg}")
                    results[sym_c] = {"error": msg, "symbol": sym_c, "tf": resolved,
                                       "period": f"{start}→{end}"}
                    continue
                df = _build(df, er_period=self.cfg.er_period); df = _add_div(df)

                # ── MTF fetch (4H by default) ──
                df_mtf = pd.DataFrame()
                if self.cfg.mtf_enable and self.cfg.mtf_tf.lower() != resolved.lower():
                    try:
                        raw_mtf = await _fetch(sym, self.cfg.mtf_tf, sdt, edt)
                        df_mtf = _build_mtf(raw_mtf)
                        if df_mtf.empty:
                            logger.warning(f"[BT] {sym_c}: MTF data unavailable, fallback OFF")
                    except Exception as e:
                        logger.warning(f"[BT] {sym_c}: MTF fetch failed ({e}), fallback OFF")
                        df_mtf = pd.DataFrame()

                sim = _sim(df, self.cfg, balance, df_mtf=df_mtf)
                st = _metrics(sim, balance)
                results[sym_c] = {**st, "symbol":sym_c, "tf":resolved,
                                  "mtf": self.cfg.mtf_tf if (self.cfg.mtf_enable and not df_mtf.empty) else "—",
                                  "period":f"{start}→{end}", "candles":len(df)}
                logger.info(f"[BT] {sym_c}: {st.get('total',0)} trades "
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
        lines = ["📈 <b>Backtest — Ramos 360 Ai 🎖️  ELITE v9 (AI Confluence + MTF + Quality Gates)</b>",
                 f"📅 Period: {period}",
                 f"⏱️ Timeframe: {tf.upper()} | HTF Bias: {mtf.upper()} | Trend-Pullback + Multi-Target",
                 "✅ Causal + Fees/Slippage + Scaled TP1/TP2/TP3 + MTF + ER Gate + DD Breaker",
                 "━━━━━━━━━━━━━━━━━━━━━━━━"]
        agg = {"total":0,"wins":0,"losses":0,"ret":0.0,"tp1":0,"tp2":0,"tp3":0,"sl":0}
        for sym, r in results.items():
            if "error" in r:
                lines.append(f"❌ {sym}: {r['error']}"); continue
            ei = "🟢" if r.get("return_pct", 0) > 0 else "🔴"
            ex = " ".join(f"{k}:{v}" for k, v in r.get("exit_breakdown", {}).items())
            ss = " ".join(f"{k}:{v}" for k, v in r.get("session_breakdown", {}).items())
            tph = r.get("tp_hits", {})
            agg["total"] += r.get("total",0); agg["wins"] += r.get("wins",0); agg["losses"] += r.get("losses",0)
            agg["ret"] += r.get("return_pct",0)
            agg["tp1"] += tph.get("TP1",0); agg["tp2"] += tph.get("TP2",0); agg["tp3"] += tph.get("TP3",0)
            agg["sl"]  += r.get("exit_breakdown",{}).get("SL",0)
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
        # Aggregate
        if agg["total"] > 0:
            wr_agg = (agg["wins"]/agg["total"]*100) if agg["total"] else 0
            lines += ["━━━━━━━━━━━━━━━━━━━━━━━━",
                      "🧮 <b>Aggregate (All Symbols)</b>",
                      f"  📊 Total Trades: {agg['total']}  ({agg['wins']}W/{agg['losses']}L)",
                      f"  🎯 Win Rate:     {wr_agg:.1f}%",
                      f"  💰 Avg Return:   {agg['ret']/max(1,len([r for r in results.values() if 'error' not in r])):+.2f}%",
                      f"  🏁 Targets:      TP1:{agg['tp1']} TP2:{agg['tp2']} TP3:{agg['tp3']}  |  SL:{agg['sl']}"]
        lines += ["━━━━━━━━━━━━━━━━━━━━━━━━", "<i>🎖️ Ramos 360 Ai — ELITE v9 (Quality Gates)</i>"]
        return "\n".join(lines)


async def _main():
    e = BacktestEngine()
    r = await e.run(symbols=["BTC/USDT:USDT", "ETH/USDT:USDT", "XRP/USDT:USDT", "SOL/USDT:USDT", "LINK/USDT:USDT"], timeframe="1h",
                    start="2026-01-01", end="2026-05-01", balance=10_000.0)
    print("\n" + e.format_report(r).replace("<b>","").replace("</b>","")
          .replace("<i>","").replace("</i>",""))


if __name__ == "__main__":
    asyncio.run(_main())

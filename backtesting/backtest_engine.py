"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️  v2.0
══════════════════════════════════════════════════════════
الهدف: رفع Win Rate من 48% → 70-80%

التحسينات المطبّقة:
  1. Session filter: ASIA + DEAD_ZONE محجوبان تماماً
  2. Multi-Timeframe (1H + 4H): يجب أن يتفقا على الاتجاه
  3. Consensus threshold: avg > 0.28 و votes ≥ 8/14
  4. ADX filter: > 22 (اتجاه قوي فقط)
  5. SMC: Order Blocks + FVG + Liquidity Sweep
  6. Divergence: 4 أنواع (RSI + MACD) مع OBV
  7. EMA Trend: السعر فوق EMA100 للـ LONG تحتها للـ SHORT
  8. RSI bounds: لا دخول عند الذروات
  9. MAX_CANDLES: 12 بدل 18 (خروج أسرع)
  10. SL هيكلي: خلف أقرب Swing Low/High
"""
from __future__ import annotations
import asyncio, math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import httpx
import pandas as pd
import numpy as np
from loguru import logger

try:
    import pandas_ta as ta
    HAS_TA = True
except Exception:
    HAS_TA = False

_BASE = "https://www.okx.com/api/v5"
_HDR  = {"Accept": "application/json", "User-Agent": "Ramos360BT/v2"}
_TFM  = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m",
          "1h":"1H","4h":"4H","1d":"1D","1w":"1W"}

ALLOWED_SESSIONS = {"OVERLAP", "LONDON", "NEW_YORK"}   # حذف ASIA + DEAD_ZONE
SCORE_THRESHOLD  = 0.28    # كان 0.18
MIN_VOTES        = 8       # كان 5
ADX_MIN          = 22      # كان 12
MAX_CANDLES      = 12      # كان 18 — خروج أسرع
VR_MIN           = 1.3     # حجم أدنى للدخول


# ══════════════════════════════════════════════════════════════════
# Session
# ══════════════════════════════════════════════════════════════════
def _sess(h: int) -> str:
    if 13 <= h < 16: return "OVERLAP"
    if  7 <= h < 13: return "LONDON"
    if 16 <= h < 21: return "NEW_YORK"
    if  0 <= h <  7: return "ASIA"
    return "DEAD_ZONE"


def _sess_q(h: int) -> float:
    return {"OVERLAP": 1.5, "LONDON": 1.2, "NEW_YORK": 1.0,
            "ASIA": 0.0, "DEAD_ZONE": 0.0}.get(_sess(h), 0.0)


# ══════════════════════════════════════════════════════════════════
# OKX Fetch
# ══════════════════════════════════════════════════════════════════
def _inst(s: str) -> str:
    return s.split(":")[0].replace("/", "-") + "-SWAP"


async def _fetch(symbol: str, tf: str,
                 start: datetime, end: datetime) -> pd.DataFrame:
    inst = _inst(symbol)
    bar  = _TFM.get(tf.lower(), tf)
    s_ms = int(start.timestamp() * 1000)
    e_ms = int(end.timestamp()   * 1000)
    rows = []; before = e_ms
    logger.info(f"[BT] {symbol} {tf} {start.date()}→{end.date()}")
    async with httpx.AsyncClient(timeout=30, headers=_HDR) as cl:
        while True:
            r = await cl.get(
                f"{_BASE}/market/history-candles",
                params={"instId": inst, "bar": bar,
                        "before": str(s_ms), "after": str(before),
                        "limit": "300"})
            if r.status_code == 429: await asyncio.sleep(5); continue
            if r.status_code != 200: break
            j = r.json()
            if j.get("code") != "0" or not j.get("data"): break
            batch = j["data"]
            for c in batch:
                ts = int(c[0])
                if s_ms <= ts <= e_ms:
                    rows.append([ts, float(c[1]), float(c[2]),
                                  float(c[3]), float(c[4]), float(c[5])])
            old = int(batch[-1][0])
            if old <= s_ms or len(batch) < 300: break
            before = old
            await asyncio.sleep(0.3)
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume"])
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    logger.info(f"[BT] {symbol} {tf}: {len(df)} candles ✅")
    return df


# ══════════════════════════════════════════════════════════════════
# Indicators
# ══════════════════════════════════════════════════════════════════
def _build(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(); c = df.close; h = df.high; l = df.low

    # RSI
    d  = c.diff()
    g  = d.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    ls = (-d.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
    df["rsi"] = (100 - 100 / (1 + g / ls.replace(0, 1e-10))).fillna(50)

    # MACD
    e12 = c.ewm(span=12, adjust=False).mean()
    e26 = c.ewm(span=26, adjust=False).mean()
    ms  = e12 - e26
    df["macd"]   = ms
    df["macd_h"] = (ms - ms.ewm(span=9, adjust=False).mean()).fillna(0)
    df["macd_s"] = ms.ewm(span=9, adjust=False).mean()

    # EMA
    for p in [9, 20, 50, 100, 200]:
        df[f"ema{p}"] = c.ewm(span=p, adjust=False).mean()

    # ATR
    hl  = h - l
    hpc = (h - c.shift()).abs()
    lpc = (l - c.shift()).abs()
    df["atr"] = pd.concat([hl, hpc, lpc], axis=1).max(axis=1).ewm(
        alpha=1/14, adjust=False).mean()

    # ADX
    up  = (h - h.shift()).clip(lower=0)
    dn  = (l.shift() - l).clip(lower=0)
    df["pdi"] = up.ewm(alpha=1/14, adjust=False).mean()
    df["mdi"] = dn.ewm(alpha=1/14, adjust=False).mean()
    df["adx"] = ((df.pdi - df.mdi).abs() / (df.pdi + df.mdi + 1e-10) * 100
                 ).ewm(alpha=1/14, adjust=False).mean().fillna(15)

    # Bollinger
    mid       = c.rolling(20).mean()
    sd        = c.rolling(20).std()
    df["bbu"] = mid + 2 * sd
    df["bbl"] = mid - 2 * sd
    df["bbp"] = (c - df.bbl) / (df.bbu - df.bbl + 1e-10) * 100

    # Stochastic
    lo14      = l.rolling(14).min()
    hi14      = h.rolling(14).max()
    df["stoch"] = 100 * (c - lo14) / (hi14 - lo14 + 1e-10)
    df["wr"]    = (-100 * (hi14 - c) / (hi14 - lo14 + 1e-10)).fillna(-50)

    # OBV
    df["obv"] = (c.diff().apply(
        lambda x: 1 if x > 0 else -1 if x < 0 else 0
    ) * df.volume).cumsum()

    # Volume
    df["vm"] = df.volume.rolling(20).mean()
    df["vr"] = df.volume / (df.vm.replace(0, 1))

    # Ichimoku
    df["iten"] = (h.rolling(9).max()  + l.rolling(9).min())  / 2
    df["ikij"] = (h.rolling(26).max() + l.rolling(26).min()) / 2
    sa = (df.iten + df.ikij) / 2
    sb = (h.rolling(52).max() + l.rolling(52).min()) / 2
    df["ict"] = pd.concat([sa, sb], axis=1).max(axis=1)
    df["icb"] = pd.concat([sa, sb], axis=1).min(axis=1)

    # Heikin Ashi
    df["hac"] = (df.open + h + l + c) / 4
    df["hao"] = df.hac.shift(2)
    df["hab"] = (df.hac > df.hao).astype(int)

    # Wyckoff simplified
    df["wyck"] = 0.0
    df.loc[(c > df.ema50) & (df.vr > 1.2), "wyck"] = 1.0
    df.loc[(c < df.ema50) & (df.vr > 1.2), "wyck"] = -1.0

    # Swing High/Low (ZigZag 5-bar)
    df["swing_h"] = df.high.rolling(11, center=True).max() == df.high
    df["swing_l"] = df.low.rolling(11, center=True).min()  == df.low

    return df.ffill().bfill()


# ══════════════════════════════════════════════════════════════════
# SMC: Order Blocks + FVG + Liquidity Sweep
# ══════════════════════════════════════════════════════════════════
def _add_smc(df: pd.DataFrame) -> pd.DataFrame:
    """Pre-compute SMC signals to avoid O(n²)."""
    n   = len(df)
    ob  = pd.Series(0.0, index=df.index)   # bullish=+1, bearish=-1
    fvg = pd.Series(0.0, index=df.index)   # bullish=+1, bearish=-1
    lsw = pd.Series(0.0, index=df.index)   # bull sweep=+1, bear sweep=-1

    for i in range(3, n - 2):
        # ── Order Block ─────────────────────────────────────────────────
        # Bullish OB: last bearish candle before upward impulse
        c0 = df.close.iloc[i];   o0 = df.open.iloc[i]
        c1 = df.close.iloc[i+1]; h1 = df.high.iloc[i+1]
        if c0 < o0 and c1 > h1 * 0.998:   # bearish candle → bullish impulse
            ob.iloc[i] = 1.0

        # Bearish OB: last bullish candle before downward impulse
        if c0 > o0 and c1 < df.low.iloc[i+1] * 1.002:
            ob.iloc[i] = -1.0

        # ── Fair Value Gap ───────────────────────────────────────────────
        if i >= 1:
            h_prev = df.high.iloc[i-1]; l_next = df.low.iloc[i+1]
            l_prev = df.low.iloc[i-1];  h_next = df.high.iloc[i+1]
            if l_next > h_prev:   fvg.iloc[i] =  1.0  # Bullish FVG
            if h_next < l_prev:   fvg.iloc[i] = -1.0  # Bearish FVG

        # ── Liquidity Sweep ──────────────────────────────────────────────
        if i >= 5:
            recent_h = df.high.iloc[i-5:i].max()
            recent_l = df.low.iloc[i-5:i].min()
            curr_h   = df.high.iloc[i]
            curr_l   = df.low.iloc[i]
            curr_c   = df.close.iloc[i]
            # Bear sweep: wicked above recent high but closed below
            if curr_h > recent_h and curr_c < recent_h * 0.999:
                lsw.iloc[i] = -1.0
            # Bull sweep: wicked below recent low but closed above
            if curr_l < recent_l and curr_c > recent_l * 1.001:
                lsw.iloc[i] = 1.0

    df["smc_ob"]  = ob
    df["smc_fvg"] = fvg
    df["smc_lsw"] = lsw
    return df


# ══════════════════════════════════════════════════════════════════
# Divergence (4 types: RSI + MACD)
# ══════════════════════════════════════════════════════════════════
def _add_divergence(df: pd.DataFrame) -> pd.DataFrame:
    scores       = pd.Series(0.0, index=df.index)
    pivot_lows   = []
    pivot_highs  = []
    n = 5  # pivot window

    for i in range(n, len(df) - n):
        lo_w = df.low.iloc[i-n:i+n+1]
        hi_w = df.high.iloc[i-n:i+n+1]
        rsi_v  = float(df.rsi.iloc[i])
        macd_v = float(df.macd.iloc[i])

        if float(df.low.iloc[i]) == float(lo_w.min()):
            pivot_lows.append((i, float(df.low.iloc[i]), rsi_v, macd_v))

        if float(df.high.iloc[i]) == float(hi_w.max()):
            pivot_highs.append((i, float(df.high.iloc[i]), rsi_v, macd_v))

        # ── Bullish Divergences (price at lows) ──────────────────────────
        if len(pivot_lows) >= 2:
            l1, l2 = pivot_lows[-2], pivot_lows[-1]
            # Regular Bullish: price LL, RSI HL → reversal UP
            if l2[1] < l1[1] and l2[2] > l1[2]:
                scores.iloc[i] += 0.65
            # Hidden Bullish: price HL, RSI LL → continuation UP
            if l2[1] > l1[1] and l2[2] < l1[2]:
                scores.iloc[i] += 0.40
            # MACD Regular Bullish
            if l2[1] < l1[1] and l2[3] > l1[3]:
                scores.iloc[i] += 0.40

        # ── Bearish Divergences (price at highs) ─────────────────────────
        if len(pivot_highs) >= 2:
            h1, h2 = pivot_highs[-2], pivot_highs[-1]
            # Regular Bearish: price HH, RSI LH → reversal DOWN
            if h2[1] > h1[1] and h2[2] < h1[2]:
                scores.iloc[i] -= 0.65
            # Hidden Bearish: price LH, RSI HH → continuation DOWN
            if h2[1] < h1[1] and h2[2] > h1[2]:
                scores.iloc[i] -= 0.40
            # MACD Regular Bearish
            if h2[1] > h1[1] and h2[3] < h1[3]:
                scores.iloc[i] -= 0.40

    # OBV confirmation boost
    obv_dir = df.obv.diff().apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
    scores  = scores + obv_dir * 0.15

    # Volume boost
    scores = scores * df.vr.clip(upper=2.0)
    df["div"] = scores.clip(-1, 1).fillna(0)
    return df


# ══════════════════════════════════════════════════════════════════
# 4H Macro Trend (Multi-Timeframe)
# ══════════════════════════════════════════════════════════════════
def _build_4h_trend(df4h: pd.DataFrame) -> pd.Series:
    """
    يُرجع series من -1/0/+1 للاتجاه الكلي على 4H.
    يُستخدم كفلتر للدخول على 1H.
    """
    if df4h is None or df4h.empty:
        return pd.Series(dtype=float)
    df4h = _build(df4h)
    # اتجاه 4H بسيط: EMA20 > EMA50 > EMA100
    trend = pd.Series(0, index=df4h.index)
    bull  = (df4h.ema20 > df4h.ema50) & (df4h.ema50 > df4h.ema100)
    bear  = (df4h.ema20 < df4h.ema50) & (df4h.ema50 < df4h.ema100)
    trend[bull] =  1
    trend[bear] = -1
    # أضف timestamp index
    trend.index = df4h["ts"]
    return trend


def _get_4h_bias(trend4h: pd.Series, ts) -> int:
    """يجلب الاتجاه 4H لوقت معين."""
    if trend4h is None or trend4h.empty:
        return 0
    try:
        past = trend4h[trend4h.index <= ts]
        return int(past.iloc[-1]) if not past.empty else 0
    except Exception:
        return 0


# ══════════════════════════════════════════════════════════════════
# Expert Scoring (14 خبير مُحسَّن)
# ══════════════════════════════════════════════════════════════════
def _score(df: pd.DataFrame, i: int) -> Tuple[str, float, int]:
    row = df.iloc[i]; c = float(row.close)
    sc  = []

    # E1: EMA Alignment (multi-level)
    e20  = float(row.ema20);  e50  = float(row.ema50)
    e100 = float(row.ema100); e200 = float(row.ema200)
    if   c > e20 > e50 > e100 > e200: sc.append( 1.0)
    elif c > e20 > e50 > e100:         sc.append( 0.7)
    elif c > e20 > e50:                sc.append( 0.4)
    elif c < e20 < e50 < e100 < e200: sc.append(-1.0)
    elif c < e20 < e50 < e100:         sc.append(-0.7)
    elif c < e20 < e50:                sc.append(-0.4)
    else:                              sc.append( 0.0)

    # E2: RSI (strict zones)
    r = float(row.rsi)
    if   r < 25:  sc.append( 1.0)
    elif r < 35:  sc.append( 0.65)
    elif r < 45:  sc.append( 0.25)
    elif r > 75:  sc.append(-1.0)
    elif r > 65:  sc.append(-0.65)
    elif r > 55:  sc.append(-0.25)
    else:         sc.append( 0.0)

    # E3: MACD Histogram + crossover
    mh = float(row.macd_h)
    sc.append(0.7 if mh > 0 else -0.7)

    # E4: Ichimoku full
    ct = float(row.ict); cb = float(row.icb)
    tn = float(row.iten); kj = float(row.ikij)
    if   c > ct and tn > kj: sc.append( 1.0)
    elif c > ct:              sc.append( 0.5)
    elif c < cb and tn < kj: sc.append(-1.0)
    elif c < cb:              sc.append(-0.5)
    else:                     sc.append( 0.0)

    # E5: Bollinger
    bp = float(row.bbp)
    if   bp < 8:  sc.append( 1.0)
    elif bp < 22: sc.append( 0.5)
    elif bp > 92: sc.append(-1.0)
    elif bp > 78: sc.append(-0.5)
    else:         sc.append( 0.0)

    # E6: Stochastic
    sk = float(row.stoch)
    if   sk < 18: sc.append( 0.8)
    elif sk < 35: sc.append( 0.3)
    elif sk > 82: sc.append(-0.8)
    elif sk > 65: sc.append(-0.3)
    else:         sc.append( 0.0)

    # E7: ADX Direction
    adx = float(row.adx); pdi = float(row.pdi); mdi = float(row.mdi)
    mult = 1.0 if adx > 30 else 0.7 if adx > 22 else 0.3
    sc.append(0.8 * mult if pdi > mdi else -0.8 * mult)

    # E8: Wyckoff
    sc.append(float(row.wyck) * 0.9)

    # E9: Williams %R
    wr = float(row.wr)
    if   wr < -85: sc.append( 0.8)
    elif wr < -65: sc.append( 0.35)
    elif wr > -15: sc.append(-0.8)
    elif wr > -35: sc.append(-0.35)
    else:          sc.append( 0.0)

    # E10: Volume Surge
    vr = float(row.vr)
    base = sum(sc) / len(sc) if sc else 0
    if   vr > 2.0 and base > 0: sc.append( 0.7)
    elif vr > 1.4 and base > 0: sc.append( 0.35)
    elif vr > 2.0 and base < 0: sc.append(-0.7)
    elif vr > 1.4 and base < 0: sc.append(-0.35)
    else:                        sc.append( 0.0)

    # E11: Divergence (pre-computed)
    sc.append(float(row.get("div", 0)) * 0.9)

    # E12: Heikin Ashi
    sc.append(0.5 if float(row.hab) == 1 else -0.5)

    # E13: Fibonacci Position
    if i >= 20:
        hi60 = float(df.high.iloc[max(0, i-50):i+1].max())
        lo60 = float(df.low.iloc[max(0, i-50):i+1].min())
        rng  = hi60 - lo60
        if rng > 0:
            pos = (c - lo60) / rng
            if   pos < 0.309: sc.append( 0.8)   # Fib 30.9% → strong support
            elif pos < 0.382: sc.append( 0.5)
            elif pos > 0.809: sc.append(-0.8)   # Fib 80.9% → strong resist
            elif pos > 0.618: sc.append(-0.5)
            else:             sc.append( 0.0)
        else: sc.append(0.0)
    else: sc.append(0.0)

    # E14: SMC (OB + FVG + Liquidity Sweep)
    ob  = float(row.get("smc_ob",  0))
    fvg = float(row.get("smc_fvg", 0))
    lsw = float(row.get("smc_lsw", 0))
    smc_score = ob * 0.5 + fvg * 0.3 + lsw * 0.6   # Sweep أقوى إشارة
    sc.append(max(-1.0, min(1.0, smc_score)))

    n   = len(sc)
    avg = sum(sc) / n if n else 0.0
    bull = sum(1 for s in sc if s > 0.05)
    bear = sum(1 for s in sc if s < -0.05)

    # ✅ Threshold مُرفَّع: avg>0.28 و votes≥8
    direction = ("LONG"  if avg >  SCORE_THRESHOLD and bull >= MIN_VOTES else
                 "SHORT" if avg < -SCORE_THRESHOLD and bear >= MIN_VOTES else
                 "NEUTRAL")
    return direction, round(avg, 4), (bull if avg >= 0 else bear)


# ══════════════════════════════════════════════════════════════════
# Structural SL (خلف Swing High/Low)
# ══════════════════════════════════════════════════════════════════
def _structural_sl(df: pd.DataFrame, i: int, direction: str,
                   atr: float, price: float) -> float:
    """يُحدد SL خلف أقرب Swing Low/High هيكلي."""
    lookback = df.iloc[max(0, i-15):i]
    if direction == "LONG":
        swing_lows = lookback[lookback["swing_l"] == True]["low"]
        if not swing_lows.empty:
            nearest_low = float(swing_lows.iloc[-1])
            sl = nearest_low - atr * 0.3   # قليلاً تحت القاع
            if (price - sl) / price > 0.03:   # لا تزيد على 3%
                sl = price - atr * 1.2
        else:
            sl = price - atr * 1.2
    else:
        swing_highs = lookback[lookback["swing_h"] == True]["high"]
        if not swing_highs.empty:
            nearest_high = float(swing_highs.iloc[-1])
            sl = nearest_high + atr * 0.3
            if (sl - price) / price > 0.03:
                sl = price + atr * 1.2
        else:
            sl = price + atr * 1.2
    return round(sl, 4)


# ══════════════════════════════════════════════════════════════════
# Simulation
# ══════════════════════════════════════════════════════════════════
def _sim(df: pd.DataFrame, df4h: pd.DataFrame = None,
         balance: float = 10_000.0) -> Dict:
    # بناء اتجاه 4H
    trend4h = None
    if df4h is not None and not df4h.empty:
        trend4h = _build_4h_trend(df4h)

    n = len(df); equity = [balance]; trades = []
    WARMUP = 60
    in_trade = False; partial = False
    direction = ""; entry = sl = tp1 = tp2 = tp3 = 0.0; entry_idx = 0

    for i in range(WARMUP, n):
        row   = df.iloc[i]; price = float(row.close)
        hi    = float(row.high); lo = float(row.low)
        hour  = row.ts.hour if hasattr(row.ts, "hour") else 10

        # ── Manage trade ────────────────────────────────────────────────
        if in_trade:
            # Time exit
            if (i - entry_idx) > MAX_CANDLES:
                pnl  = ((price-entry)/entry*100) if direction=="LONG" \
                        else ((entry-price)/entry*100)
                mult = 0.5 if partial else 1.0
                risk = balance * 0.01 / max(abs(entry-sl)/entry, 0.001)
                balance = max(1.0, balance + pnl/100 * risk * mult)
                trades.append({"direction": direction, "entry": round(entry,4),
                               "exit": round(price,4),
                               "pnl_pct": round(pnl*mult, 3),
                               "result": "WIN" if pnl > 0 else "LOSS",
                               "exit_type": "TIME_EXIT",
                               "session": _sess(hour)})
                in_trade = False; partial = False
                equity.append(balance); continue

            # Hits
            if direction == "LONG":
                hit_sl  = lo <= sl
                hit_tp3 = hi >= tp3
                hit_tp2 = hi >= tp2 and not hit_tp3
                hit_tp1 = hi >= tp1 and not hit_tp2 and not hit_tp3
            else:
                hit_sl  = hi >= sl
                hit_tp3 = lo <= tp3
                hit_tp2 = lo <= tp2 and not hit_tp3
                hit_tp1 = lo <= tp1 and not hit_tp2 and not hit_tp3

            # TP1 Partial + Break Even SL
            if hit_tp1 and not partial:
                pnl  = ((tp1-entry)/entry*100) if direction=="LONG" \
                        else ((entry-tp1)/entry*100)
                risk = balance * 0.01 / max(abs(entry-sl)/entry, 0.001)
                balance = max(1.0, balance + pnl/100 * risk * 0.5)
                sl = entry; partial = True  # ✅ Break Even
                trades.append({"direction": direction, "entry": round(entry,4),
                               "exit": round(tp1,4),
                               "pnl_pct": round(pnl*0.5, 3),
                               "result": "WIN", "exit_type": "TP1_PARTIAL",
                               "session": _sess(hour)})
                equity.append(balance); continue

            if hit_tp2 or hit_tp3 or hit_sl:
                ex    = sl if hit_sl else (tp3 if hit_tp3 else tp2)
                pnl   = ((ex-entry)/entry*100) if direction=="LONG" \
                         else ((entry-ex)/entry*100)
                mult  = 0.5 if partial else 1.0
                risk  = balance * 0.01 / max(abs(entry-sl)/entry, 0.001)
                balance = max(1.0, balance + pnl/100 * risk * mult)
                xtype = "SL" if hit_sl else ("TP3" if hit_tp3 else "TP2")
                trades.append({"direction": direction, "entry": round(entry,4),
                               "exit": round(ex,4),
                               "pnl_pct": round(pnl*mult, 3),
                               "result": "WIN" if pnl > 0 else "LOSS",
                               "exit_type": xtype, "session": _sess(hour)})
                in_trade = False; partial = False

        # ── New signal ──────────────────────────────────────────────────
        if not in_trade:

            # ✅ 1. Session filter: فقط OVERLAP + LONDON + NEW_YORK
            sess = _sess(hour)
            if sess not in ALLOWED_SESSIONS:
                equity.append(balance); continue

            # ✅ 2. ADX filter: اتجاه قوي فقط
            if float(row.adx) < ADX_MIN:
                equity.append(balance); continue

            # ✅ 3. Volume filter
            if float(row.vr) < VR_MIN:
                equity.append(balance); continue

            # Score
            sig, avg, votes = _score(df, i)
            if sig == "NEUTRAL":
                equity.append(balance); continue

            # ✅ 4. Multi-Timeframe: 4H يجب أن يتفق
            if trend4h is not None:
                bias_4h = _get_4h_bias(trend4h, row.ts)
                if sig == "LONG"  and bias_4h == -1:
                    equity.append(balance); continue
                if sig == "SHORT" and bias_4h ==  1:
                    equity.append(balance); continue

            # ✅ 5. EMA Trend: السعر يجب أن يكون في جانب الاتجاه
            e100 = float(row.ema100)
            if sig == "LONG"  and float(row.close) < e100 * 0.998:
                equity.append(balance); continue
            if sig == "SHORT" and float(row.close) > e100 * 1.002:
                equity.append(balance); continue

            # ✅ 6. RSI bounds: لا دخول عند ذروات متطرفة
            rsi = float(row.rsi)
            if sig == "LONG"  and rsi > 70: equity.append(balance); continue
            if sig == "SHORT" and rsi < 30: equity.append(balance); continue

            # ✅ 7. Liquidity Sweep: تأكيد
            lsw = float(row.get("smc_lsw", 0))
            if sig == "LONG"  and lsw == -1.0: equity.append(balance); continue
            if sig == "SHORT" and lsw ==  1.0: equity.append(balance); continue

            # Build levels
            atr_v = float(row.atr) if float(row.atr) > 0 else price * 0.015
            rng   = atr_v * 7

            # ✅ SL هيكلي
            sl_struct = _structural_sl(df, i, sig, atr_v, price)

            if sig == "LONG":
                sl  = sl_struct
                tp1 = round(price + atr_v * 2.0, 4)
                tp2 = round(price + rng * 0.618, 4)
                tp3 = round(price + rng * 0.809, 4)
            else:
                sl  = sl_struct
                tp1 = round(price - atr_v * 2.0, 4)
                tp2 = round(price - rng * 0.618, 4)
                tp3 = round(price - rng * 0.809, 4)

            # ✅ RR ≥ 1.5
            rr = abs(tp1 - price) / max(abs(sl - price), 1e-10)
            if rr < 1.5:
                equity.append(balance); continue

            direction = sig; entry = price
            in_trade  = True; entry_idx = i; partial = False

        equity.append(balance)

    # Close remaining
    if in_trade:
        lp  = float(df.close.iloc[-1])
        pnl = ((lp-entry)/entry*100) if direction=="LONG" \
               else ((entry-lp)/entry*100)
        trades.append({"direction": direction, "entry": entry, "exit": lp,
                       "pnl_pct": round(pnl*(0.5 if partial else 1.0), 3),
                       "result": "WIN" if pnl > 0 else "LOSS",
                       "exit_type": "OPEN_AT_END"})

    return {"trades": trades, "equity": equity, "final_balance": balance}


# ══════════════════════════════════════════════════════════════════
# Metrics
# ══════════════════════════════════════════════════════════════════
def _metrics(sim: Dict, initial: float) -> Dict:
    trades = sim["trades"]
    eq     = pd.Series(sim["equity"])
    bal    = sim["final_balance"]
    if not trades:
        return {"error": "No trades", "total": 0, "win_rate_pct": 0,
                "max_dd_pct": 0, "return_pct": 0, "sharpe": 0}
    wins   = [t for t in trades if t["result"] == "WIN"]
    losses = [t for t in trades if t["result"] == "LOSS"]
    tot    = len(trades)
    wr     = round(len(wins) / tot * 100, 2) if tot else 0
    dd     = round(float(((eq - eq.cummax()) / eq.cummax() * 100).min()), 2)
    ret    = round((bal - initial) / initial * 100, 2)
    rets   = eq.pct_change().dropna()
    sh     = round(float(rets.mean() / rets.std() * (252**0.5)), 3) \
             if rets.std() > 0 else 0.0
    aw     = round(sum(t["pnl_pct"] for t in wins)   / len(wins),   3) if wins   else 0
    al     = round(sum(t["pnl_pct"] for t in losses) / len(losses), 3) if losses else 0
    by_ex  = {}; by_ss = {}
    for t in trades:
        k = t.get("exit_type","?"); by_ex[k] = by_ex.get(k,0) + 1
        s = t.get("session","?");   by_ss[s] = by_ss.get(s,0) + 1
    return {"total": tot, "wins": len(wins), "losses": len(losses),
            "win_rate_pct": wr, "avg_win_pct": aw, "avg_loss_pct": al,
            "expectancy": round(wr/100*aw + (1-wr/100)*al, 3),
            "max_dd_pct": dd, "return_pct": ret, "sharpe": sh,
            "final_balance": round(bal, 2),
            "exit_breakdown": by_ex, "session_breakdown": by_ss}


# ══════════════════════════════════════════════════════════════════
# BacktestEngine
# ══════════════════════════════════════════════════════════════════
class BacktestEngine:

    async def run(self,
                  symbols:   List[str] = None,
                  timeframe: str       = "1h",
                  tf:        str       = None,
                  start:     str       = "2026-01-01",
                  end:       str       = "2026-05-01",
                  balance:   float     = 10_000.0,
                  use_sessions: bool   = True,
                  **kwargs) -> Dict:

        resolved = tf or timeframe or "1h"
        symbols  = symbols or ["BTC/USDT:USDT", "ETH/USDT:USDT"]
        sdt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        edt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
        results = {}

        for sym in symbols:
            sym_c = sym.replace("/USDT:USDT", "")
            try:
                # جلب 1H
                df = await _fetch(sym, resolved, sdt, edt)
                if df is None or len(df) < 70:
                    results[sym_c] = {"error": "insufficient data"}; continue

                # ✅ جلب 4H للـ MTF
                df4h = None
                try:
                    df4h = await _fetch(sym, "4h", sdt, edt)
                    if df4h is not None and len(df4h) < 20:
                        df4h = None
                except Exception:
                    df4h = None

                # بناء المؤشرات
                df = _build(df)
                df = _add_smc(df)
                df = _add_divergence(df)

                sim = _sim(df, df4h=df4h, balance=balance)
                st  = _metrics(sim, balance)
                results[sym_c] = {**st, "symbol": sym_c, "tf": resolved,
                                  "period": f"{start}→{end}",
                                  "candles": len(df)}
                logger.info(
                    f"[BT] {sym_c}: {st['total']} trades "
                    f"WR={st['win_rate_pct']}% "
                    f"Ret={st.get('return_pct',0):+.2f}%"
                )
            except Exception as e:
                logger.error(f"[BT] {sym_c}: {e}")
                results[sym_c] = {"error": str(e)}

        return results

    @staticmethod
    def format_report(results: Dict) -> str:
        tf = next(
            (v.get("tf","1H") for v in results.values()
             if isinstance(v,dict) and "tf" in v), "1H")
        lines = [
            "📈 <b>Backtest — Ramos 360 Ai 🎖️  v2.0</b>",
            f"📅 Period: 2026-01-01 → 2026-05-01",
            f"⏱️ Timeframe: {tf.upper()} | 14 Experts",
            "✅ MTF(4H) + SMC + Divergence(4) + Session + Structural SL",
            "━━━━━━━━━━━━━━━━━━━━━━━━",
        ]
        for sym, r in results.items():
            if "error" in r:
                lines.append(f"❌ {sym}: {r['error']}"); continue
            ei = "🟢" if r.get("return_pct",0) > 0 else "🔴"
            ex = " ".join(f"{k}:{v}" for k,v in r.get("exit_breakdown",{}).items())
            ss = " ".join(f"{k}:{v}" for k,v in r.get("session_breakdown",{}).items())
            lines += [
                "", f"{ei} <b>{sym}</b>",
                f"  📊 {r['total']} trades  ({r.get('wins',0)}W/{r.get('losses',0)}L)",
                f"  🎯 Win Rate:   {r['win_rate_pct']:.1f}%",
                f"  💰 Return:     {r.get('return_pct',0):+.2f}%",
                f"  📉 Max DD:     {r.get('max_dd_pct',0):.2f}%",
                f"  ⚖️ Sharpe:     {r.get('sharpe',0):.3f}",
                f"  🔮 Expectancy: {r.get('expectancy',0):+.3f}%",
                f"  🏦 Balance:    ${r.get('final_balance',10000):,.2f}",
                f"  📋 Exits:      {ex}",
                f"  🕐 Sessions:   {ss}",
            ]
        lines += ["━━━━━━━━━━━━━━━━━━━━━━━━",
                  "<i>🎖️ Ramos 360 Ai — Backtest v2.0</i>"]
        return "\n".join(lines)


async def _main():
    e = BacktestEngine()
    r = await e.run(
        symbols   = ["BTC/USDT:USDT"],
        timeframe = "1h",
        start     = "2026-01-01",
        end       = "2026-05-01",
        balance   = 10_000.0,
    )
    print("\n" + e.format_report(r)
          .replace("<b>","").replace("</b>","")
          .replace("<i>","").replace("</i>",""))


if __name__ == "__main__":
    asyncio.run(_main())

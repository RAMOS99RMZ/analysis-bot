
"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️
Real walk-forward backtester with all bot features:
  - Session Filter (London/NY only)
  - ATR Dynamic SL
  - Layer 1 Bias (Wyckoff + WR + EMA)
  - 14 Experts scoring
  - Fibonacci TP (0.309 / 0.618 / 0.809)
  - 4 Divergence types

Accepts both: run(timeframe="4h") AND run(tf="4h")
Period: 2026-01-01 → 2026-05-01
"""
from __future__ import annotations
import asyncio
import math
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

# ─── OKX public API ────────────────────────────────────────────────────────
_BASE = "https://www.okx.com/api/v5"
_HDR  = {"Accept": "application/json", "User-Agent": "Ramos360BT/2.0"}
_TF   = {
    "1m":"1m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1H","2h":"2H","4h":"4H","6h":"6H",
    "1d":"1D","1w":"1W","1M":"1M",
}


def _inst(symbol: str) -> str:
    """BTC/USDT:USDT → BTC-USDT-SWAP"""
    return symbol.split(":")[0].replace("/", "-") + "-SWAP"


# ═══════════════════════════════════════════════════════════════
# 1. SESSION FILTER
# ═══════════════════════════════════════════════════════════════

def _session(h: int) -> str:
    if 13 <= h < 16: return "OVERLAP"
    if  7 <= h < 12: return "LONDON"
    if 13 <= h < 17: return "NEW_YORK"
    if  0 <= h <  7: return "ASIA"
    return "DEAD_ZONE"


def _session_ok(h: int) -> bool:
    return _session(h) in ("OVERLAP", "LONDON", "NEW_YORK")


def _session_quality(h: int) -> float:
    return {"OVERLAP": 1.5, "LONDON": 1.2, "NEW_YORK": 1.1,
            "ASIA": 0.5, "DEAD_ZONE": 0.3}.get(_session(h), 1.0)


# ═══════════════════════════════════════════════════════════════
# 2. FETCH CANDLES WITH PAGINATION
# ═══════════════════════════════════════════════════════════════

async def _fetch(symbol: str, tf: str,
                  start: datetime, end: datetime) -> pd.DataFrame:
    inst      = _inst(symbol)
    bar       = _TF.get(tf.lower(), tf)
    start_ms  = int(start.timestamp() * 1000)
    end_ms    = int(end.timestamp() * 1000)
    rows: List[List] = []
    before_ms = end_ms

    logger.info(f"[BT] Fetching {symbol} {tf} {start.date()} → {end.date()} …")

    async with httpx.AsyncClient(timeout=30, headers=_HDR) as cl:
        while True:
            r = await cl.get(f"{_BASE}/market/history-candles", params={
                "instId": inst, "bar": bar,
                "before": str(start_ms),
                "after":  str(before_ms),
                "limit":  "300",
            })
            if r.status_code == 429:
                await asyncio.sleep(5)
                continue
            if r.status_code != 200:
                break
            j = r.json()
            if j.get("code") != "0" or not j.get("data"):
                break
            batch = j["data"]
            for c in batch:
                ts = int(c[0])
                if start_ms <= ts <= end_ms:
                    rows.append([ts, float(c[1]), float(c[2]),
                                  float(c[3]), float(c[4]), float(c[5])])
            oldest = int(batch[-1][0])
            if oldest <= start_ms or len(batch) < 300:
                break
            before_ms = oldest
            await asyncio.sleep(0.3)

    if not rows:
        logger.warning(f"[BT] No data for {symbol} {tf}")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume"])
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    logger.info(f"[BT] {symbol}: {len(df)} candles ✅")
    return df


# ═══════════════════════════════════════════════════════════════
# 3. INDICATORS
# ═══════════════════════════════════════════════════════════════

def _rsi(s: pd.Series, p: int = 14) -> pd.Series:
    if HAS_TA:
        try:
            v = ta.rsi(s, length=p)
            if v is not None:
                return v.fillna(50)
        except Exception:
            pass
    d = s.diff()
    g = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1/p, adjust=False).mean()
    return (100 - 100 / (1 + g / l.replace(0, 1e-10))).fillna(50)


def _ema(s: pd.Series, p: int) -> pd.Series:
    if HAS_TA:
        try:
            v = ta.ema(s, length=p)
            if v is not None:
                return v.ffill().bfill()
        except Exception:
            pass
    return s.ewm(span=p, adjust=False).mean()


def _macd_hist(s: pd.Series) -> pd.Series:
    if HAS_TA:
        try:
            v = ta.macd(s, fast=12, slow=26, signal=9)
            if v is not None and not v.empty:
                return v.iloc[:, 2].fillna(0)
        except Exception:
            pass
    e12 = s.ewm(span=12, adjust=False).mean()
    e26 = s.ewm(span=26, adjust=False).mean()
    ms  = e12 - e26
    return (ms - ms.ewm(span=9, adjust=False).mean()).fillna(0)


def _atr(df: pd.DataFrame, p: int = 14) -> pd.Series:
    if HAS_TA:
        try:
            v = ta.atr(df.high, df.low, df.close, length=p)
            if v is not None:
                return v.ffill().bfill()
        except Exception:
            pass
    hl  = df.high - df.low
    hpc = (df.high - df.close.shift()).abs()
    lpc = (df.low  - df.close.shift()).abs()
    return pd.concat([hl, hpc, lpc], axis=1).max(axis=1)\
             .ewm(alpha=1/p, adjust=False).mean()


def _williams_r(df: pd.DataFrame, p: int = 14) -> pd.Series:
    hi = df.high.rolling(p).max()
    lo = df.low.rolling(p).min()
    return (-100 * (hi - df.close) / (hi - lo + 1e-10)).fillna(-50)


def _obv(df: pd.DataFrame) -> pd.Series:
    sign = df.close.diff().apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
    return (sign * df.volume).cumsum()


def _build_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    c  = df.close

    df["rsi"]     = _rsi(c, 14)
    df["rsi_6"]   = _rsi(c, 6)
    df["macd_h"]  = _macd_hist(c)
    df["atr"]     = _atr(df, 14)
    df["wr"]      = _williams_r(df, 14)
    df["obv"]     = _obv(df)
    df["vol_ma"]  = df.volume.rolling(20).mean()
    df["vol_r"]   = df.volume / df.vol_ma.replace(0, 1)

    for p in [9, 20, 50, 100, 200]:
        df[f"ema{p}"] = _ema(c, p)

    # Ichimoku
    df["ichi_ten"] = (df.high.rolling(9).max()  + df.low.rolling(9).min())  / 2
    df["ichi_kij"] = (df.high.rolling(26).max() + df.low.rolling(26).min()) / 2
    df["ichi_ct"]  = pd.concat([
        (df.ichi_ten + df.ichi_kij) / 2,
        (df.high.rolling(52).max() + df.low.rolling(52).min()) / 2,
    ], axis=1).max(axis=1)
    df["ichi_cb"]  = pd.concat([
        (df.ichi_ten + df.ichi_kij) / 2,
        (df.high.rolling(52).max() + df.low.rolling(52).min()) / 2,
    ], axis=1).min(axis=1)

    # Bollinger
    mid          = c.rolling(20).mean()
    sd           = c.rolling(20).std()
    df["bb_up"]  = mid + 2 * sd
    df["bb_low"] = mid - 2 * sd
    df["bb_pct"] = (c - df.bb_low) / (df.bb_up - df.bb_low + 1e-10) * 100

    # Stochastic
    lo14         = df.low.rolling(14).min()
    hi14         = df.high.rolling(14).max()
    df["stoch"]  = 100 * (c - lo14) / (hi14 - lo14 + 1e-10)

    # ADX
    up   = (df.high - df.high.shift()).clip(lower=0)
    dn   = (df.low.shift() - df.low).clip(lower=0)
    df["pdi"] = up.ewm(alpha=1/14, adjust=False).mean()
    df["mdi"] = dn.ewm(alpha=1/14, adjust=False).mean()
    df["adx"] = ((df.pdi - df.mdi).abs() / (df.pdi + df.mdi + 1e-10) * 100)\
                  .ewm(alpha=1/14, adjust=False).mean()

    # Heikin Ashi
    df["ha_c"]   = (df.open + df.high + df.low + df.close) / 4
    df["ha_o"]   = df.ha_c.shift(2)
    df["ha_bull"] = (df.ha_c > df.ha_o).astype(int)

    # Wyckoff score
    df["wyck"] = 0.0
    df.loc[(c > df.ema50) & (df.vol_r > 1.2),  "wyck"] =  1.0
    df.loc[(c < df.ema50) & (df.vol_r > 1.2),  "wyck"] = -1.0

    return df.ffill().bfill()


# ═══════════════════════════════════════════════════════════════
# 4. LAYER 1 — Macro Bias per candle
# ═══════════════════════════════════════════════════════════════

def _layer1_bias(df: pd.DataFrame, idx: int) -> Tuple[str, float]:
    if idx < 50:
        return "NEUTRAL", 0.0
    row = df.iloc[idx]
    scores = []

    # Wyckoff
    scores.append(float(row.wyck))

    # EMA alignment
    c = float(row.close)
    e20, e50 = float(row.ema20), float(row.ema50)
    e200 = float(row.get("ema200", e50))
    if c > e20 > e50 > e200:   scores.append(1.0)
    elif c > e20 > e50:        scores.append(0.5)
    elif c < e20 < e50 < e200: scores.append(-1.0)
    elif c < e20 < e50:        scores.append(-0.5)
    else:                      scores.append(0.0)

    # Williams %R
    wr = float(row.wr)
    if wr < -80:   scores.append(0.8)
    elif wr > -20: scores.append(-0.8)
    else:          scores.append(0.0)

    avg  = sum(scores) / len(scores) if scores else 0.0
    bias = "BULL" if avg > 0.25 else "BEAR" if avg < -0.25 else "NEUTRAL"
    return bias, round(avg, 3)


# ═══════════════════════════════════════════════════════════════
# 5. DIVERGENCE DETECTION
# ═══════════════════════════════════════════════════════════════

def _find_pivots(series: pd.Series, idx: int, depth: int = 5) -> List[Tuple[int, float]]:
    """Find recent pivot lows/highs in a series up to idx."""
    pivots = []
    start  = max(depth, idx - 60)
    for i in range(start, idx - depth + 1):
        window = series.iloc[i-depth:i+depth+1]
        if len(window) < depth * 2 + 1:
            continue
        mid = series.iloc[i]
        if series.name and "high" in series.name.lower():
            if mid == window.max():
                pivots.append((i, float(mid)))
        else:
            if mid == window.min():
                pivots.append((i, float(mid)))
    return pivots[-3:]


def _divergence_score(df: pd.DataFrame, idx: int) -> float:
    """Returns divergence score: +0.6 (bull div) / -0.6 (bear div) / 0.0"""
    if idx < 30:
        return 0.0

    price_slice = df.close.iloc[max(0, idx-40):idx+1]
    rsi_slice   = df.rsi.iloc[max(0, idx-40):idx+1]

    # Find pivot lows and highs
    p_lows  = []
    p_highs = []
    for i in range(5, len(price_slice) - 5):
        w = price_slice.iloc[i-5:i+6]
        if len(w) < 11:
            continue
        v = price_slice.iloc[i]
        if v == w.min():
            p_lows.append((i, float(v), float(rsi_slice.iloc[i])))
        if v == w.max():
            p_highs.append((i, float(v), float(rsi_slice.iloc[i])))

    score = 0.0

    # Regular Bullish: price LL, RSI HL
    if len(p_lows) >= 2:
        l1, l2 = p_lows[-2], p_lows[-1]
        if l2[1] < l1[1] and l2[2] > l1[2]:
            score += 0.6

    # Regular Bearish: price HH, RSI LH
    if len(p_highs) >= 2:
        h1, h2 = p_highs[-2], p_highs[-1]
        if h2[1] > h1[1] and h2[2] < h1[2]:
            score -= 0.6

    # Hidden Bullish: price HL, RSI LL
    if len(p_lows) >= 2:
        l1, l2 = p_lows[-2], p_lows[-1]
        if l2[1] > l1[1] and l2[2] < l1[2]:
            score += 0.4

    # Hidden Bearish: price LH, RSI HH
    if len(p_highs) >= 2:
        h1, h2 = p_highs[-2], p_highs[-1]
        if h2[1] < h1[1] and h2[2] > h1[2]:
            score -= 0.4

    # Volume confirmation
    vol_r = float(df.vol_r.iloc[idx]) if idx < len(df) else 1.0
    if vol_r > 1.5 and abs(score) > 0:
        score *= 1.2

    return round(max(-1.0, min(1.0, score)), 3)


# ═══════════════════════════════════════════════════════════════
# 6. SMC SIMPLIFIED
# ═══════════════════════════════════════════════════════════════

def _smc_score(df: pd.DataFrame, idx: int) -> float:
    if idx < 12:
        return 0.0
    score = 0.0
    rh = float(df.high.iloc[max(0,idx-5):idx+1].max())
    rl = float(df.low.iloc[max(0,idx-5):idx+1].min())
    ph = float(df.high.iloc[max(0,idx-11):idx-5].max()) if idx >= 11 else rh
    pl = float(df.low.iloc[max(0,idx-11):idx-5].min())  if idx >= 11 else rl
    c  = float(df.close.iloc[idx])

    # BOS
    if rh > ph * 1.003 and c > ph: score += 0.7
    elif rl < pl * 0.997 and c < pl: score -= 0.7

    # Liquidity sweep + recovery
    prev_lo = float(df.low.iloc[max(0,idx-7):idx].min())
    prev_hi = float(df.high.iloc[max(0,idx-7):idx].max())
    if float(df.low.iloc[idx]) < prev_lo * 0.999 and c > prev_lo:
        score += 0.5
    if float(df.high.iloc[idx]) > prev_hi * 1.001 and c < prev_hi:
        score -= 0.5

    return round(max(-1.0, min(1.0, score)), 3)


# ═══════════════════════════════════════════════════════════════
# 7. FIBONACCI POSITION SCORE
# ═══════════════════════════════════════════════════════════════

def _fib_score(df: pd.DataFrame, idx: int) -> float:
    if idx < 30:
        return 0.0
    hi  = float(df.high.iloc[max(0,idx-60):idx+1].max())
    lo  = float(df.low.iloc[max(0,idx-60):idx+1].min())
    rng = hi - lo
    if rng <= 0:
        return 0.0
    c   = float(df.close.iloc[idx])
    pos = (c - lo) / rng

    # Special Fibonacci zones
    BULL_ZONES = [(0.309-0.02, 0.309+0.02),
                  (0.4045-0.02, 0.4045+0.02),
                  (0.618-0.02, 0.618+0.02)]
    BEAR_ZONES = [(0.750-0.02, 0.750+0.02),
                  (0.780-0.02, 0.780+0.02),
                  (0.809-0.02, 0.809+0.02)]

    for lo_z, hi_z in BULL_ZONES:
        if lo_z <= pos <= hi_z:
            return 0.7   # at key support fib
    for lo_z, hi_z in BEAR_ZONES:
        if lo_z <= pos <= hi_z:
            return -0.7  # at key resistance fib

    if pos < 0.35:   return 0.5
    elif pos > 0.65: return -0.5
    return 0.0


# ═══════════════════════════════════════════════════════════════
# 8. 14-EXPERT SCORING
# ═══════════════════════════════════════════════════════════════

def _score(df: pd.DataFrame, idx: int,
            l1_bias: str) -> Tuple[str, float, int]:
    """Returns (direction, avg_score, votes)"""
    row  = df.iloc[idx]
    c    = float(row.close)
    sc   = []

    # Layer 1 bias (double weight)
    if l1_bias == "BULL":    sc += [0.8, 0.8]
    elif l1_bias == "BEAR":  sc += [-0.8, -0.8]

    # E1: RSI
    rsi = float(row.rsi)
    sc.append(0.9 if rsi < 30 else 0.5 if rsi < 40 else
              -0.9 if rsi > 70 else -0.5 if rsi > 60 else 0.0)

    # E2: MACD histogram
    sc.append(0.6 if float(row.macd_h) > 0 else -0.6)

    # E3: EMA alignment
    e20 = float(row.ema20); e50 = float(row.ema50)
    e200 = float(row.get("ema200", e50))
    if c > e20 > e50 > e200:    sc.append(1.0)
    elif c > e20 > e50:         sc.append(0.5)
    elif c < e20 < e50 < e200:  sc.append(-1.0)
    elif c < e20 < e50:         sc.append(-0.5)
    else:                       sc.append(0.0)

    # E4: Ichimoku
    ct = float(row.ichi_ct); cb = float(row.ichi_cb)
    ten = float(row.ichi_ten); kij = float(row.ichi_kij)
    if c > ct and ten > kij:    sc.append(1.0)
    elif c > ct:                sc.append(0.5)
    elif c < cb and ten < kij:  sc.append(-1.0)
    elif c < cb:                sc.append(-0.5)
    else:                       sc.append(0.0)

    # E5: Bollinger Bands
    bbp = float(row.bb_pct)
    sc.append(0.8 if bbp < 10 else 0.3 if bbp < 25 else
              -0.8 if bbp > 90 else -0.3 if bbp > 75 else 0.0)

    # E6: Stochastic
    sk = float(row.stoch)
    sc.append(0.7 if sk < 20 else 0.3 if sk < 35 else
              -0.7 if sk > 80 else -0.3 if sk > 65 else 0.0)

    # E7: ADX
    adx = float(row.adx); pdi = float(row.pdi); mdi = float(row.mdi)
    if adx > 25:
        sc.append(0.7 if pdi > mdi else -0.7)
    else:
        sc.append(0.2 if pdi > mdi else -0.2)

    # E8: SMC
    sc.append(_smc_score(df, idx))

    # E9: Fibonacci
    sc.append(_fib_score(df, idx))

    # E10: Gann Square of 9
    root = math.sqrt(max(c, 0.001))
    g_lvls = sorted(set(round((root+s)**2, 2) for s in [-1,-.5,0,.5,1] if round((root+s)**2,2)>0))
    if g_lvls:
        near  = min(g_lvls, key=lambda x: abs(x - c))
        dist  = (near - c) / c
        sc.append(0.5 if abs(dist) < 0.003 else 0.3 if dist > 0 else -0.3)
    else:
        sc.append(0.0)

    # E11: Divergence (all 4 types)
    sc.append(_divergence_score(df, idx))

    # E12: Volume
    vr = float(row.vol_r)
    base = sum(sc) / len(sc) if sc else 0
    sc.append(0.4 * (1 if base > 0 else -1) if vr > 1.5 else 0.0)

    # E13: Wyckoff
    sc.append(float(row.wyck) * 0.8)

    # E14: Williams %R
    wr = float(row.wr)
    sc.append(0.7 if wr < -80 else -0.7 if wr > -20 else 0.0)

    # E14b: Heikin Ashi
    sc.append(0.4 if float(row.ha_bull) == 1 else -0.4)

    n    = len(sc)
    avg  = sum(sc) / n if n else 0.0
    bull = sum(1 for s in sc if s > 0)
    bear = sum(1 for s in sc if s < 0)

    direction = ("LONG"  if avg >  0.25 and bull >= 5 else
                 "SHORT" if avg < -0.25 and bear >= 5 else "NEUTRAL")
    votes = bull if avg > 0 else bear

    return direction, round(avg, 4), votes


# ═══════════════════════════════════════════════════════════════
# 9. SIMULATION
# ═══════════════════════════════════════════════════════════════

def _simulate(df: pd.DataFrame,
               balance: float = 10_000.0,
               risk_pct: float = 0.01,
               use_sessions: bool = True) -> Dict:
    n         = len(df)
    equity    = [balance]
    trades    = []
    in_trade  = False
    entry = sl = tp1 = tp2 = tp3 = atr_v = 0.0
    direction = ""; entry_idx = 0
    WARMUP    = 60

    for i in range(WARMUP, n):
        row   = df.iloc[i]
        price = float(row.close)
        hi    = float(row.high)
        lo    = float(row.low)
        hour  = row.ts.hour if hasattr(row.ts, "hour") else 10

        # ── Check open trade ─────────────────────────────────────────
        if in_trade:
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

            if hit_sl or hit_tp1 or hit_tp2 or hit_tp3:
                ex  = sl if hit_sl else (tp3 if hit_tp3 else tp2 if hit_tp2 else tp1)
                pnl = ((ex-entry)/entry*100) if direction == "LONG" \
                       else ((entry-ex)/entry*100)
                risk  = balance * risk_pct / max(abs(entry-sl)/entry, 0.001)
                gain  = pnl / 100 * risk
                balance = max(1.0, balance + gain)
                xtype = "SL" if hit_sl else ("TP3" if hit_tp3 else "TP2" if hit_tp2 else "TP1")
                trades.append({
                    "entry_idx":  entry_idx,
                    "exit_idx":   i,
                    "direction":  direction,
                    "entry":      round(entry, 4),
                    "exit":       round(ex, 4),
                    "pnl_pct":    round(pnl, 3),
                    "result":     "WIN" if pnl > 0 else "LOSS",
                    "exit_type":  xtype,
                    "atr":        round(atr_v, 4),
                    "session":    _session(hour),
                })
                in_trade = False

        # ── New signal ────────────────────────────────────────────────
        if not in_trade:
            # Session filter
            if use_sessions and not _session_ok(hour):
                equity.append(balance)
                continue

            # Layer 1 bias
            l1_bias, l1_score = _layer1_bias(df, i)
            if l1_bias == "NEUTRAL":
                equity.append(balance)
                continue

            # 14 experts
            sig, avg_score, votes = _score(df, i, l1_bias)
            if sig == "NEUTRAL":
                equity.append(balance)
                continue

            # Session quality adjustment
            sq = _session_quality(hour)
            if abs(avg_score) * sq < 0.28:
                equity.append(balance)
                continue
            if votes < 5:
                equity.append(balance)
                continue

            # ATR Dynamic SL (× 1.2) + Fibonacci TP
            atr_v = float(row.atr) if float(row.atr) > 0 else price * 0.015
            rng   = atr_v * 6  # projected range

            if sig == "LONG":
                sl  = round(price - atr_v * 1.2, 4)
                tp1 = round(price + rng * 0.309,  4)
                tp2 = round(price + rng * 0.618,  4)
                tp3 = round(price + rng * 0.809,  4)
            else:
                sl  = round(price + atr_v * 1.2, 4)
                tp1 = round(price - rng * 0.309,  4)
                tp2 = round(price - rng * 0.618,  4)
                tp3 = round(price - rng * 0.809,  4)

            # Validate RR ≥ 1.5
            rr = abs(tp1 - price) / max(abs(sl - price), 1e-10)
            if rr < 1.5:
                equity.append(balance)
                continue

            direction  = sig
            entry      = price
            in_trade   = True
            entry_idx  = i

        equity.append(balance)

    # Close any remaining open trade at end
    if in_trade:
        lp  = float(df.close.iloc[-1])
        pnl = ((lp-entry)/entry*100) if direction == "LONG" \
               else ((entry-lp)/entry*100)
        trades.append({
            "direction": direction, "entry": entry, "exit": lp,
            "pnl_pct": round(pnl, 3),
            "result":  "WIN" if pnl > 0 else "LOSS",
            "exit_type": "OPEN_AT_END", "atr": round(atr_v, 4),
        })

    return {"trades": trades, "equity": equity, "final_balance": balance}


# ═══════════════════════════════════════════════════════════════
# 10. METRICS
# ═══════════════════════════════════════════════════════════════

def _metrics(sim: Dict, initial: float) -> Dict:
    trades = sim["trades"]
    equity = pd.Series(sim["equity"])
    bal    = sim["final_balance"]

    if not trades:
        return {"error": "No trades generated", "total": 0,
                "win_rate_pct": 0, "max_dd_pct": 0, "return_pct": 0, "sharpe": 0}

    wins   = [t for t in trades if t["result"] == "WIN"]
    losses = [t for t in trades if t["result"] == "LOSS"]
    total  = len(trades)
    wr     = round(len(wins)/total*100, 2) if total else 0

    # Drawdown
    dd     = round(float(((equity - equity.cummax()) / equity.cummax() * 100).min()), 2)
    ret    = round((bal - initial) / initial * 100, 2)

    # Sharpe
    rets   = equity.pct_change().dropna()
    sharpe = round(float(rets.mean()/rets.std()*(252**0.5)), 3) \
             if rets.std() > 0 else 0.0

    aw = round(sum(t["pnl_pct"] for t in wins)  / len(wins),   3) if wins   else 0
    al = round(sum(t["pnl_pct"] for t in losses)/ len(losses), 3) if losses else 0

    by_exit = {}
    by_sess = {}
    for t in trades:
        k = t.get("exit_type", "?"); by_exit[k] = by_exit.get(k, 0) + 1
        s = t.get("session",    "?"); by_sess[s] = by_sess.get(s, 0) + 1

    return {
        "total":         total,
        "wins":          len(wins),
        "losses":        len(losses),
        "win_rate_pct":  wr,
        "avg_win_pct":   aw,
        "avg_loss_pct":  al,
        "expectancy":    round(wr/100*aw + (1-wr/100)*al, 3),
        "max_dd_pct":    dd,
        "return_pct":    ret,
        "sharpe":        sharpe,
        "final_balance": round(bal, 2),
        "exit_breakdown": by_exit,
        "session_breakdown": by_sess,
    }


# ═══════════════════════════════════════════════════════════════
# 11. BacktestEngine CLASS
# ═══════════════════════════════════════════════════════════════

class BacktestEngine:
    """
    Complete backtester for Ramos 360 Ai.
    Usage:
        engine = BacktestEngine()
        results = await engine.run(
            symbols=["BTC/USDT:USDT"],
            timeframe="4h",          # ✅ works
            tf="4h",                 # ✅ also works
            start="2026-01-01",
            end="2026-05-01",
        )
    """

    async def run(self,
                  symbols:      List[str] = None,
                  timeframe:    str       = "4h",
                  tf:           str       = None,
                  start:        str       = "2026-01-01",
                  end:          str       = "2026-05-01",
                  balance:      float     = 10_000.0,
                  use_sessions: bool      = True,
                  trade_type:   str       = "Scalp",
                  **kwargs) -> Dict:
        """
        Run backtest. Accepts 'timeframe' OR 'tf' (both work).
        """
        # ✅ Accept both 'timeframe' and 'tf'
        resolved_tf = tf or timeframe or kwargs.get("bar", "4h") or "4h"
        symbols     = symbols or ["BTC/USDT:USDT", "ETH/USDT:USDT"]
        start_dt    = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        end_dt      = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
        results     = {}

        for sym in symbols:
            sym_c = sym.replace("/USDT:USDT", "")
            logger.info(f"[BT] ── {sym_c} {resolved_tf} {start} → {end} ──")
            try:
                df = await _fetch(sym, resolved_tf, start_dt, end_dt)
                if df is None or len(df) < 70:
                    results[sym_c] = {"error": "insufficient data"}
                    continue

                df    = _build_indicators(df)
                sim   = _simulate(df, balance, use_sessions=use_sessions)
                stats = _metrics(sim, balance)
                results[sym_c] = {
                    **stats,
                    "symbol":    sym_c,
                    "tf":        resolved_tf,
                    "period":    f"{start} → {end}",
                    "candles":   len(df),
                }
                logger.info(
                    f"[BT] {sym_c}: {stats['total']} trades | "
                    f"WR={stats['win_rate_pct']}% | "
                    f"Return={stats['return_pct']:+.2f}% | "
                    f"DD={stats['max_dd_pct']:.2f}%"
                )

            except Exception as e:
                logger.error(f"[BT] {sym_c}: {e}")
                results[sym_c] = {"error": str(e)}

        return results

    @staticmethod
    def format_report(results: Dict) -> str:
        tf_used = next(
            (v.get("tf","4H") for v in results.values() if isinstance(v, dict) and "tf" in v),
            "4H"
        )
        lines = [
            "📈 <b>Backtest — Ramos 360 Ai 🎖️</b>",
            f"📅 Period: 2026-01-01 → 2026-05-01",
            f"⏱️ Timeframe: {tf_used.upper()} | 14 Experts",
            "✅ Session + ATR SL + Layer 1 Bias + Fib TP",
            "━━━━━━━━━━━━━━━━━━━━━━━━",
        ]
        for sym, r in results.items():
            if "error" in r:
                lines.append(f"❌ {sym}: {r['error']}")
                continue
            ei    = "🟢" if r.get("return_pct", 0) > 0 else "🔴"
            exits = "  ".join(f"{k}:{v}" for k, v in r.get("exit_breakdown",{}).items())
            sess  = "  ".join(f"{k}:{v}" for k, v in r.get("session_breakdown",{}).items())
            lines += [
                "",
                f"{ei} <b>{sym}</b>",
                f"  📊 {r['total']} trades  ({r.get('wins',0)}W / {r.get('losses',0)}L)",
                f"  🎯 Win Rate:   {r['win_rate_pct']:.1f}%",
                f"  💰 Return:     {r.get('return_pct',0):+.2f}%",
                f"  📉 Max DD:     {r.get('max_dd_pct',0):.2f}%",
                f"  ⚖️ Sharpe:     {r.get('sharpe',0):.3f}",
                f"  🔮 Expectancy: {r.get('expectancy',0):+.3f}%",
                f"  🏦 Balance:    ${r.get('final_balance',10000):,.2f}",
                f"  📋 Exits:      {exits}",
                f"  🕐 Sessions:   {sess}",
            ]
        lines += [
            "━━━━━━━━━━━━━━━━━━━━━━━━",
            "<i>🎖️ Ramos 360 Ai — Backtest Engine v2</i>",
        ]
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# CLI runner
# ═══════════════════════════════════════════════════════════════

async def _main():
    engine  = BacktestEngine()
    results = await engine.run(
        symbols=["BTC/USDT:USDT"],
        timeframe="4h",
        start="2026-01-01",
        end="2026-05-01",
        balance=10_000.0,
        use_sessions=True,
    )
    report = engine.format_report(results)
    print("\n" + "=" * 55)
    print(report
          .replace("<b>", "").replace("</b>", "")
          .replace("<i>", "").replace("</i>", ""))
    print("=" * 55)


if __name__ == "__main__":
    asyncio.run(_main())

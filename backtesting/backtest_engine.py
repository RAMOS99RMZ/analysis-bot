
"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️
Real backtester: BTC 4H | Jan 1 → May 1, 2026
All 14 experts + indicators + proper SL/TP simulation
"""
from __future__ import annotations
import asyncio, math, time
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

# ═══════════════════════════════════════════════════════════
# OKX FETCH (pagination for full date range)
# ═══════════════════════════════════════════════════════════
_OKX = "https://www.okx.com/api/v5"
_HDR = {"Accept": "application/json", "User-Agent": "Ramos360BT/1.0"}
_TF  = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m",
         "1h":"1H","4h":"4H","1d":"1D","1w":"1W"}

def _inst(symbol: str) -> str:
    return symbol.split(":")[0].replace("/","-") + "-SWAP"

async def fetch_full_range(symbol: str, tf: str,
                            start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch complete OHLCV for a date range using OKX history-candles
    with automatic pagination (max 300 per request).
    Returns DataFrame oldest-first with columns: ts,open,high,low,close,volume
    """
    inst      = _inst(symbol)
    bar       = _TF.get(tf, tf)
    start_ms  = int(start.timestamp() * 1000)
    end_ms    = int(end.timestamp() * 1000)
    all_rows: List[List] = []
    before_ms = end_ms           # OKX: fetch candles BEFORE this ts

    logger.info(f"[BT] Fetching {symbol} {tf} {start.date()} → {end.date()} …")

    async with httpx.AsyncClient(timeout=30, headers=_HDR) as cl:
        while True:
            r = await cl.get(f"{_OKX}/market/history-candles", params={
                "instId": inst,
                "bar":    bar,
                "before": str(start_ms),
                "after":  str(before_ms),
                "limit":  "300",
            })
            if r.status_code == 429:
                logger.warning("[BT] Rate limited — waiting 5s …")
                await asyncio.sleep(5); continue
            if r.status_code != 200:
                logger.warning(f"[BT] HTTP {r.status_code}"); break
            j = r.json()
            if j.get("code") != "0" or not j.get("data"):
                break

            batch = j["data"]  # newest first
            for c in batch:
                ts = int(c[0])
                if start_ms <= ts <= end_ms:
                    all_rows.append([ts,
                                     float(c[1]), float(c[2]),
                                     float(c[3]), float(c[4]),
                                     float(c[5])])

            # oldest ts in this batch → set as new ceiling for next page
            oldest_ts = int(batch[-1][0])
            if oldest_ts <= start_ms or len(batch) < 300:
                break
            before_ms = oldest_ts
            await asyncio.sleep(0.25)

    if not all_rows:
        logger.warning(f"[BT] No data fetched for {symbol} {tf}")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows, columns=["ts","open","high","low","close","volume"])
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    logger.info(f"[BT] {symbol}: {len(df)} candles fetched ✅")
    return df


# ═══════════════════════════════════════════════════════════
# INDICATORS — all computed from scratch, no external deps
# ═══════════════════════════════════════════════════════════

def _rsi(s: pd.Series, p: int = 14) -> pd.Series:
    if HAS_TA:
        try:
            v = ta.rsi(s, length=p)
            if v is not None: return v.fillna(50)
        except Exception: pass
    d  = s.diff()
    g  = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
    l  = (-d.clip(upper=0)).ewm(alpha=1/p, adjust=False).mean()
    rs = g / l.replace(0, 1e-10)
    return (100 - 100/(1+rs)).fillna(50)

def _ema(s: pd.Series, p: int) -> pd.Series:
    if HAS_TA:
        try:
            v = ta.ema(s, length=p)
            if v is not None: return v.fillna(method="bfill")
        except Exception: pass
    return s.ewm(span=p, adjust=False).mean()

def _macd(s: pd.Series) -> pd.DataFrame:
    if HAS_TA:
        try:
            v = ta.macd(s, fast=12, slow=26, signal=9)
            if v is not None and not v.empty: return v
        except Exception: pass
    e12 = s.ewm(span=12, adjust=False).mean()
    e26 = s.ewm(span=26, adjust=False).mean()
    ms  = e12 - e26
    sig = ms.ewm(span=9, adjust=False).mean()
    return pd.DataFrame({"MACD_12_26_9": ms,
                          "MACDs_12_26_9": sig,
                          "MACDh_12_26_9": ms - sig})

def _atr(df: pd.DataFrame, p: int = 14) -> pd.Series:
    if HAS_TA:
        try:
            v = ta.atr(df.high, df.low, df.close, length=p)
            if v is not None: return v.fillna(method="bfill")
        except Exception: pass
    hl  = df.high - df.low
    hpc = (df.high - df.close.shift()).abs()
    lpc = (df.low  - df.close.shift()).abs()
    tr  = pd.concat([hl,hpc,lpc], axis=1).max(axis=1)
    return tr.ewm(alpha=1/p, adjust=False).mean()

def _bbands(s: pd.Series, p: int = 20, std: float = 2.0) -> pd.DataFrame:
    if HAS_TA:
        try:
            v = ta.bbands(s, length=p, std=std)
            if v is not None and not v.empty: return v
        except Exception: pass
    mid = s.rolling(p).mean()
    sd  = s.rolling(p).std()
    return pd.DataFrame({f"BBU_{p}_{std}": mid + std*sd,
                          f"BBM_{p}_{std}": mid,
                          f"BBL_{p}_{std}": mid - std*sd})

def _stoch(df: pd.DataFrame, k: int = 14, d: int = 3) -> pd.DataFrame:
    if HAS_TA:
        try:
            v = ta.stoch(df.high, df.low, df.close, k=k, d=d)
            if v is not None and not v.empty: return v
        except Exception: pass
    lo  = df.low.rolling(k).min()
    hi  = df.high.rolling(k).max()
    pct = 100*(df.close - lo)/(hi - lo + 1e-10)
    return pd.DataFrame({f"STOCHk_{k}_{d}_3": pct,
                          f"STOCHd_{k}_{d}_3": pct.rolling(d).mean()})

def _obv(df: pd.DataFrame) -> pd.Series:
    if HAS_TA:
        try:
            v = ta.obv(df.close, df.volume)
            if v is not None: return v
        except Exception: pass
    sign = df.close.diff().apply(lambda x: 1 if x>0 else -1 if x<0 else 0)
    return (sign * df.volume).cumsum()

def _add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add all indicators to the DataFrame."""
    df = df.copy()
    c = df.close

    # RSI
    df["rsi"]    = _rsi(c, 14)
    df["rsi_6"]  = _rsi(c, 6)

    # EMA
    for p in [9, 20, 50, 100, 200]:
        df[f"ema{p}"] = _ema(c, p)

    # MACD
    mac = _macd(c)
    df["macd"]     = mac.iloc[:,0]
    df["macd_sig"] = mac.iloc[:,1]
    df["macd_h"]   = mac.iloc[:,2]

    # ATR
    df["atr"] = _atr(df, 14)
    df["atr_pct"] = df.atr / c * 100

    # Bollinger Bands
    bb = _bbands(c, 20)
    df["bb_up"]  = bb.iloc[:,0]
    df["bb_mid"] = bb.iloc[:,1]
    df["bb_low"] = bb.iloc[:,2]
    df["bb_pct"] = (c - df.bb_low) / (df.bb_up - df.bb_low + 1e-10) * 100

    # Stochastic
    st = _stoch(df)
    df["stoch_k"] = st.iloc[:,0]
    df["stoch_d"] = st.iloc[:,1]

    # OBV
    df["obv"] = _obv(df)

    # Volume
    df["vol_ma20"]  = df.volume.rolling(20).mean()
    df["vol_ratio"] = df.volume / df.vol_ma20.replace(0, 1)

    # Ichimoku
    df["ichi_ten"] = (df.high.rolling(9).max()  + df.low.rolling(9).min())  / 2
    df["ichi_kij"] = (df.high.rolling(26).max() + df.low.rolling(26).min()) / 2
    df["ichi_sa"]  = (df.ichi_ten + df.ichi_kij) / 2
    df["ichi_sb"]  = (df.high.rolling(52).max() + df.low.rolling(52).min()) / 2
    df["ichi_ct"]  = df[["ichi_sa","ichi_sb"]].max(axis=1)
    df["ichi_cb"]  = df[["ichi_sa","ichi_sb"]].min(axis=1)

    # Pivot Points (rolling daily)
    df["pp"]  = (df.high + df.low + df.close) / 3
    df["r1"]  = 2*df.pp - df.low
    df["s1"]  = 2*df.pp - df.high
    df["r2"]  = df.pp + (df.high - df.low)
    df["s2"]  = df.pp - (df.high - df.low)

    # Heikin Ashi
    df["ha_c"] = (df.open + df.high + df.low + df.close) / 4
    df["ha_o"] = df["ha_c"].shift(2)
    df["ha_bull"] = (df.ha_c > df.ha_o).astype(int)

    # Trend strength (ADX approximation)
    df["tr_up"]    = (df.high - df.high.shift()).clip(lower=0)
    df["tr_dn"]    = (df.low.shift() - df.low).clip(lower=0)
    df["plus_di"]  = df.tr_up.ewm(alpha=1/14, adjust=False).mean()
    df["minus_di"] = df.tr_dn.ewm(alpha=1/14, adjust=False).mean()
    di_sum         = (df.plus_di + df.minus_di).replace(0, 1e-10)
    df["adx"]      = ((df.plus_di - df.minus_di).abs() / di_sum * 100) \
                        .ewm(alpha=1/14, adjust=False).mean()

    return df.fillna(method="bfill").fillna(method="ffill")


# ═══════════════════════════════════════════════════════════
# FIBONACCI LEVELS
# ═══════════════════════════════════════════════════════════

def _fib_targets(price: float, swing_hi: float, swing_lo: float,
                 direction: str) -> Tuple[float, float, float]:
    """Returns (tp1, tp2, tp3) using custom Fibonacci ratios."""
    rng = swing_hi - swing_lo or price * 0.10
    RATIOS = [0.309, 0.618, 0.809]
    if direction == "LONG":
        return tuple(round(price + rng * r, 4) for r in RATIOS)
    else:
        return tuple(round(price - rng * r, 4) for r in RATIOS)


# ═══════════════════════════════════════════════════════════
# GANN SQUARE OF 9
# ═══════════════════════════════════════════════════════════

def _gann_levels(price: float) -> List[float]:
    root = math.sqrt(price)
    return sorted(set(
        round((root + s) ** 2, 2)
        for s in [-1.5,-1,-0.75,-0.5,-0.25,0,0.25,0.5,0.75,1,1.5]
        if round((root + s) ** 2, 2) > 0
    ))


# ═══════════════════════════════════════════════════════════
# ALL 14 EXPERTS — scoring on each candle
# ═══════════════════════════════════════════════════════════

def _score_candle(row: pd.Series, df_slice: pd.DataFrame) -> Dict:
    """Run all 14 experts on current candle row. Returns scores + direction."""
    c  = float(row.close)
    scores: List[float] = []

    # E1 — Classic TA
    try:
        rsi = float(row.get("rsi", 50))
        if rsi < 30:    scores.append(1.0)
        elif rsi < 45:  scores.append(0.4)
        elif rsi > 70:  scores.append(-1.0)
        elif rsi > 55:  scores.append(-0.4)
        else:           scores.append(0.0)

        macd_h = float(row.get("macd_h", 0))
        scores.append(0.6 if macd_h > 0 else -0.6)

        ha_b = float(row.get("ha_bull", 0.5))
        scores.append(0.5 if ha_b == 1 else -0.5)
    except Exception:
        scores += [0, 0, 0]

    # E2 — EMA Alignment
    try:
        e20  = float(row.get("ema20", c))
        e50  = float(row.get("ema50", c))
        e200 = float(row.get("ema200", c))
        if c > e20 > e50 > e200:   scores.append(1.0)
        elif c > e20 > e50:        scores.append(0.5)
        elif c < e20 < e50 < e200: scores.append(-1.0)
        elif c < e20 < e50:        scores.append(-0.5)
        else:                      scores.append(0.0)
    except Exception:
        scores.append(0.0)

    # E3 — Ichimoku
    try:
        ct = float(row.get("ichi_ct", c))
        cb = float(row.get("ichi_cb", c))
        ten= float(row.get("ichi_ten", c))
        kij= float(row.get("ichi_kij", c))
        if c > ct and ten > kij:   scores.append(1.0)
        elif c > ct:               scores.append(0.5)
        elif c < cb and ten < kij: scores.append(-1.0)
        elif c < cb:               scores.append(-0.5)
        else:                      scores.append(0.0)
    except Exception:
        scores.append(0.0)

    # E4 — Bollinger Bands
    try:
        bb_pct = float(row.get("bb_pct", 50))
        if bb_pct < 10:   scores.append(0.8)
        elif bb_pct < 25: scores.append(0.3)
        elif bb_pct > 90: scores.append(-0.8)
        elif bb_pct > 75: scores.append(-0.3)
        else:             scores.append(0.0)
    except Exception:
        scores.append(0.0)

    # E5 — Stochastic
    try:
        sk = float(row.get("stoch_k", 50))
        sd = float(row.get("stoch_d", 50))
        if sk < 20 and sk > sd:   scores.append(0.8)
        elif sk < 20:             scores.append(0.4)
        elif sk > 80 and sk < sd: scores.append(-0.8)
        elif sk > 80:             scores.append(-0.4)
        else:                     scores.append(0.0)
    except Exception:
        scores.append(0.0)

    # E6 — OBV trend
    try:
        if len(df_slice) >= 10:
            obv_now  = float(df_slice.obv.iloc[-5:].mean())
            obv_prev = float(df_slice.obv.iloc[-10:-5].mean())
            p_now    = float(df_slice.close.iloc[-1])
            p_prev   = float(df_slice.close.iloc[-6])
            if p_now < p_prev and obv_now > obv_prev:  scores.append(0.9)  # bull div
            elif obv_now > obv_prev:                    scores.append(0.4)
            elif p_now > p_prev and obv_now < obv_prev: scores.append(-0.9) # bear div
            else:                                        scores.append(-0.4)
        else:
            scores.append(0.0)
    except Exception:
        scores.append(0.0)

    # E7 — ADX Trend Strength
    try:
        adx    = float(row.get("adx", 20))
        pdi    = float(row.get("plus_di",  0))
        mdi    = float(row.get("minus_di", 0))
        if adx > 25 and pdi > mdi:    scores.append(0.7)
        elif adx > 25 and mdi > pdi:  scores.append(-0.7)
        elif adx > 20:                scores.append(0.2 if pdi > mdi else -0.2)
        else:                         scores.append(0.0)
    except Exception:
        scores.append(0.0)

    # E8 — SMC (Simplified: structural breaks)
    try:
        if len(df_slice) >= 10:
            recent_hi = float(df_slice.high.iloc[-6:].max())
            recent_lo = float(df_slice.low.iloc[-6:].min())
            prev_hi   = float(df_slice.high.iloc[-12:-6].max()) if len(df_slice)>=12 else recent_hi
            prev_lo   = float(df_slice.low.iloc[-12:-6].min())  if len(df_slice)>=12 else recent_lo
            if recent_hi > prev_hi * 1.003 and c > prev_hi:   scores.append(0.8)
            elif recent_lo < prev_lo * 0.997 and c < prev_lo: scores.append(-0.8)
            else:                                               scores.append(0.0)
        else:
            scores.append(0.0)
    except Exception:
        scores.append(0.0)

    # E9 — Fibonacci Position
    try:
        if len(df_slice) >= 30:
            hi  = float(df_slice.high.iloc[-60:].max() if len(df_slice)>=60 else df_slice.high.max())
            lo  = float(df_slice.low.iloc[-60:].min()  if len(df_slice)>=60 else df_slice.low.min())
            rng = hi - lo
            if rng > 0:
                pos = (c - lo) / rng
                if pos < 0.382:   scores.append(0.7)   # near bottom fib
                elif pos > 0.618: scores.append(-0.7)  # near top fib
                else:             scores.append(0.0)
            else: scores.append(0.0)
        else: scores.append(0.0)
    except Exception:
        scores.append(0.0)

    # E10 — Gann Sq9
    try:
        gann = _gann_levels(c)
        if gann:
            nearest = min(gann, key=lambda x: abs(x - c))
            dist    = (nearest - c) / c
            if abs(dist) < 0.003:  scores.append(0.5)   # at key level
            elif dist > 0:         scores.append(0.3)   # support below
            else:                  scores.append(-0.3)  # resistance above
        else:
            scores.append(0.0)
    except Exception:
        scores.append(0.0)

    # E11 — Candlestick Pattern
    try:
        if len(df_slice) >= 3:
            r0 = df_slice.iloc[-1]; r1 = df_slice.iloc[-2]
            o0,h0,l0,c0 = float(r0.open),float(r0.high),float(r0.low),float(r0.close)
            o1,h1,l1,c1 = float(r1.open),float(r1.high),float(r1.low),float(r1.close)
            b1 = abs(c1-o1); rng1 = h1-l1 or 1e-4
            # Hammer → bull
            if c1>o1 and (min(o1,c1)-l1)>b1*2 and (h1-max(o1,c1))<b1*0.4:
                scores.append(0.7)
            # Shooting star → bear
            elif c1<o1 and (h1-max(o1,c1))>b1*2 and (min(o1,c1)-l1)<b1*0.4:
                scores.append(-0.7)
            # Bullish engulf
            elif c0>o0 and c1<o1 and c0>o1 and o0<c1:
                scores.append(0.6)
            # Bearish engulf
            elif c0<o0 and c1>o1 and c0<o1 and o0>c1:
                scores.append(-0.6)
            # Doji
            elif abs(c0-o0) < (h0-l0)*0.1:
                scores.append(0.0)
            else:
                scores.append(0.2 if c0>o0 else -0.2)
        else:
            scores.append(0.0)
    except Exception:
        scores.append(0.0)

    # E12 — Volume Confirmation
    try:
        vr = float(row.get("vol_ratio", 1.0))
        if vr > 1.5:   scores.append(0.4 if sum(scores)>0 else -0.4)
        elif vr > 1.2: scores.append(0.2 if sum(scores)>0 else -0.2)
        else:          scores.append(0.0)
    except Exception:
        scores.append(0.0)

    # E13 — Pivot Support/Resistance
    try:
        pp  = float(row.get("pp",  c))
        r1  = float(row.get("r1",  c))
        s1  = float(row.get("s1",  c))
        r2  = float(row.get("r2",  c))
        s2  = float(row.get("s2",  c))
        if c > r1:   scores.append(-0.6)
        elif c > pp: scores.append(0.3)
        elif c < s1: scores.append(0.6)
        else:        scores.append(-0.3)
    except Exception:
        scores.append(0.0)

    # E14 — RSI + MACD Convergence
    try:
        rsi_6  = float(row.get("rsi_6", 50))
        rsi_14 = float(row.get("rsi",   50))
        macd_h_val = float(row.get("macd_h", 0))
        if rsi_6 < 35 and rsi_14 < 45 and macd_h_val > 0:
            scores.append(1.0)  # strong convergence bull
        elif rsi_6 > 65 and rsi_14 > 55 and macd_h_val < 0:
            scores.append(-1.0) # strong convergence bear
        else:
            scores.append(0.0)
    except Exception:
        scores.append(0.0)

    # Final decision
    n   = len(scores)
    avg = sum(scores) / n if n else 0
    conf = min(abs(avg), 1.0)
    votes_bull = sum(1 for s in scores if s > 0)
    votes_bear = sum(1 for s in scores if s < 0)

    direction = "LONG"  if avg >  0.20 else \
                "SHORT" if avg < -0.20 else "NEUTRAL"

    return {
        "direction":   direction,
        "avg_score":   round(avg, 4),
        "confidence":  round(conf, 3),
        "votes_bull":  votes_bull,
        "votes_bear":  votes_bear,
        "total_exp":   n,
    }


# ═══════════════════════════════════════════════════════════
# TRADE SIMULATOR (realistic TP/SL with ATR sizing)
# ═══════════════════════════════════════════════════════════

def _simulate(df: pd.DataFrame,
              initial_balance: float = 10_000.0,
              risk_pct: float = 0.01,
              atr_sl_mult: float = 1.5) -> Dict:
    """
    Walk-forward simulation on the indicator-enriched DataFrame.
    One trade at a time; uses real Fibonacci TP levels.
    """
    df = df.copy().reset_index(drop=True)
    n  = len(df)

    balance  = initial_balance
    equity   = [initial_balance]
    trades   = []
    in_trade = False
    entry_price = sl = tp1 = tp2 = tp3 = 0.0
    direction = ""; entry_idx = 0

    MIN_CANDLES = 60  # warmup for indicators

    for i in range(MIN_CANDLES, n):
        row      = df.iloc[i]
        price    = float(row.close)
        hi_price = float(row.high)
        lo_price = float(row.low)
        atr      = float(row.atr) if row.atr > 0 else price * 0.01

        # ── Check open trade ─────────────────────────────────────
        if in_trade:
            hit_sl = hit_tp1 = hit_tp2 = hit_tp3 = False
            if direction == "LONG":
                hit_sl  = lo_price <= sl
                hit_tp3 = hi_price >= tp3
                hit_tp2 = hi_price >= tp2 and not hit_tp3
                hit_tp1 = hi_price >= tp1 and not hit_tp2 and not hit_tp3
            else:
                hit_sl  = hi_price >= sl
                hit_tp3 = lo_price <= tp3
                hit_tp2 = lo_price <= tp2 and not hit_tp3
                hit_tp1 = lo_price <= tp1 and not hit_tp2 and not hit_tp3

            if hit_sl or hit_tp1 or hit_tp2 or hit_tp3:
                exit_px = sl if hit_sl else (tp3 if hit_tp3 else tp2 if hit_tp2 else tp1)
                pnl_pct = ((exit_px - entry_price)/entry_price*100) if direction=="LONG" \
                           else ((entry_price - exit_px)/entry_price*100)
                trade_pnl = pnl_pct / 100 * (balance * risk_pct / (abs(entry_price-sl)/entry_price) if sl != entry_price else balance * 0.01)
                balance   = max(1.0, balance + trade_pnl)
                result    = "WIN" if pnl_pct > 0 else "LOSS"
                exit_type = "SL" if hit_sl else ("TP3" if hit_tp3 else "TP2" if hit_tp2 else "TP1")
                trades.append({
                    "entry_idx":  entry_idx,
                    "exit_idx":   i,
                    "direction":  direction,
                    "entry":      round(entry_price, 4),
                    "exit":       round(exit_px, 4),
                    "pnl_pct":   round(pnl_pct, 3),
                    "trade_pnl": round(trade_pnl, 2),
                    "result":    result,
                    "exit_type": exit_type,
                    "experts_bull": 0,
                    "experts_bear": 0,
                })
                in_trade = False

        # ── Check for new signal ─────────────────────────────────
        if not in_trade and i >= MIN_CANDLES + 1:
            df_slice = df.iloc[:i+1]
            sig = _score_candle(row, df_slice)

            if sig["direction"] != "NEUTRAL":
                # Require minimum confidence + votes
                if sig["confidence"] < 0.30: continue
                if sig["votes_bull"] < 4 and sig["direction"] == "LONG":  continue
                if sig["votes_bear"] < 4 and sig["direction"] == "SHORT": continue

                direction   = sig["direction"]
                entry_price = price
                sl = price - atr * atr_sl_mult if direction == "LONG" \
                     else price + atr * atr_sl_mult

                # Fibonacci TP levels
                hi60  = float(df.high.iloc[max(0,i-60):i+1].max())
                lo60  = float(df.low.iloc[max(0,i-60):i+1].min())
                tp1, tp2, tp3 = _fib_targets(price, hi60, lo60, direction)

                # Ensure TP > SL distance
                rr_tp1 = abs(tp1 - price) / max(abs(sl - price), 1e-10)
                if rr_tp1 < 0.8:
                    continue  # bad RR

                in_trade  = True
                entry_idx = i

        equity.append(balance)

    # Close any open trade at end
    if in_trade:
        last_price = float(df.close.iloc[-1])
        pnl_pct    = ((last_price - entry_price)/entry_price*100) if direction=="LONG" \
                      else ((entry_price - last_price)/entry_price*100)
        trades.append({
            "direction": direction, "entry": entry_price,
            "exit": last_price, "pnl_pct": round(pnl_pct, 3),
            "result": "WIN" if pnl_pct > 0 else "LOSS",
            "exit_type": "OPEN_AT_END",
        })

    return {"trades": trades, "equity": equity, "final_balance": balance}


# ═══════════════════════════════════════════════════════════
# METRICS
# ═══════════════════════════════════════════════════════════

def _metrics(sim_result: Dict, initial: float) -> Dict:
    trades  = sim_result["trades"]
    equity  = sim_result["equity"]
    balance = sim_result["final_balance"]

    if not trades:
        return {"error": "No trades generated",
                "total": 0, "win_rate": 0,
                "max_dd": 0, "return_pct": 0, "sharpe": 0}

    wins   = [t for t in trades if t["result"] == "WIN"]
    losses = [t for t in trades if t["result"] == "LOSS"]
    total  = len(trades)
    wr     = round(len(wins)/total*100, 2)

    # Max Drawdown
    eq_s      = pd.Series(equity)
    peak      = eq_s.cummax()
    dd        = (eq_s - peak) / peak * 100
    max_dd    = round(float(dd.min()), 2)

    # Return
    ret_pct   = round((balance - initial)/initial*100, 2)

    # Sharpe
    rets = eq_s.pct_change().dropna()
    sharpe = round(float(rets.mean()/rets.std()*(252**0.5)), 3) \
             if rets.std() > 0 else 0.0

    # Average pnl
    avg_win  = round(sum(t["pnl_pct"] for t in wins)/len(wins), 3) if wins else 0
    avg_loss = round(sum(t["pnl_pct"] for t in losses)/len(losses), 3) if losses else 0
    expect   = round(wr/100*avg_win + (1-wr/100)*avg_loss, 3)

    # Exit breakdown
    by_exit = {}
    for t in trades:
        k = t.get("exit_type", "?")
        by_exit[k] = by_exit.get(k, 0) + 1

    return {
        "total":       total,
        "wins":        len(wins),
        "losses":      len(losses),
        "win_rate_pct": wr,
        "avg_win_pct":  avg_win,
        "avg_loss_pct": avg_loss,
        "expectancy":   expect,
        "max_dd_pct":   max_dd,
        "return_pct":   ret_pct,
        "sharpe":       sharpe,
        "final_balance": round(balance, 2),
        "exit_breakdown": by_exit,
    }


# ═══════════════════════════════════════════════════════════
# MAIN BACKTEST RUNNER
# ═══════════════════════════════════════════════════════════

class BacktestEngine:
    """
    Full backtester: BTC + ETH on 4H candles
    Period: 2026-01-01 → 2026-05-01
    All 14 experts + Fibonacci TP + ATR SL
    """

    async def run(self,
                  symbols:  List[str] = None,
                  tf:       str       = "4h",
                  start:    str       = "2026-01-01",
                  end:      str       = "2026-05-01",
                  balance:  float     = 10_000.0) -> Dict:

        symbols = symbols or ["BTC/USDT:USDT", "ETH/USDT:USDT"]
        start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        end_dt   = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
        results  = {}

        for sym in symbols:
            sym_c = sym.replace("/USDT:USDT","")
            logger.info(f"[BT] ── Starting {sym_c} {tf} {start} → {end} ──")
            try:
                df = await fetch_full_range(sym, tf, start_dt, end_dt)
                if df is None or df.empty or len(df) < 70:
                    results[sym_c] = {"error": "insufficient data"}
                    continue

                logger.info(f"[BT] {sym_c}: computing indicators …")
                df = _add_indicators(df)

                logger.info(f"[BT] {sym_c}: running walk-forward simulation …")
                sim   = _simulate(df, balance)
                stats = _metrics(sim, balance)
                results[sym_c] = {**stats,
                                   "symbol":    sym_c,
                                   "tf":        tf,
                                   "period":    f"{start} → {end}",
                                   "candles":   len(df)}

                logger.info(
                    f"[BT] {sym_c}: "
                    f"{stats['total']} trades | "
                    f"WR={stats['win_rate_pct']}% | "
                    f"Return={stats['return_pct']:+.2f}% | "
                    f"DD={stats['max_dd_pct']:.2f}% | "
                    f"Sharpe={stats['sharpe']}"
                )
            except Exception as e:
                logger.error(f"[BT] {sym_c}: {e}")
                results[sym_c] = {"error": str(e)}

        return results

    @staticmethod
    def format_report(results: Dict) -> str:
        lines = [
            "📈 <b>Backtest — Ramos 360 Ai 🎖️</b>",
            "📅 Period: 2026-01-01 → 2026-05-01",
            "⏱️ Timeframe: 4H | 14 Experts",
            "━━━━━━━━━━━━━━━━━━━━━━━━",
        ]
        for sym, r in results.items():
            if "error" in r:
                lines.append(f"❌ {sym}: {r['error']}"); continue
            wr   = r.get("win_rate_pct", 0)
            ret  = r.get("return_pct", 0)
            dd   = r.get("max_dd_pct", 0)
            shp  = r.get("sharpe", 0)
            exp  = r.get("expectancy", 0)
            tot  = r.get("total", 0)
            blnc = r.get("final_balance", 10000)
            e_i  = "🟢" if ret > 0 else "🔴"
            exits= r.get("exit_breakdown", {})
            exits_str = "  ".join(f"{k}:{v}" for k,v in exits.items())
            lines += [
                f"",
                f"{e_i} <b>{sym}</b>",
                f"  📊 {tot} trades  ({r.get('wins',0)}W / {r.get('losses',0)}L)",
                f"  🎯 Win Rate:   {wr:.1f}%",
                f"  💰 Return:     {ret:+.2f}%",
                f"  📉 Max DD:     {dd:.2f}%",
                f"  ⚖️ Sharpe:     {shp:.3f}",
                f"  🔮 Expectancy: {exp:+.3f}%",
                f"  🏦 Balance:    ${blnc:,.2f}",
                f"  📋 Exits:      {exits_str}",
            ]
        lines += ["━━━━━━━━━━━━━━━━━━━━━━━━",
                  "<i>🎖️ Ramos 360 Ai — Backtest Engine</i>"]
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════
# CLI Runner
# ═══════════════════════════════════════════════════════════
async def _main():
    engine  = BacktestEngine()
    results = await engine.run(
        symbols=["BTC/USDT:USDT"],
        tf="4h",
        start="2026-01-01",
        end="2026-05-01",
        balance=10_000.0,
    )
    report = engine.format_report(results)
    print("\n" + "="*55)
    print(report.replace("<b>","").replace("</b>","")
              .replace("<i>","").replace("</i>",""))
    print("="*55)


if __name__ == "__main__":
    asyncio.run(_main())

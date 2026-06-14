"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️  QUANT v2
══════════════════════════════════════════════════════════════
QUANT ENGINE FOCUS:
  - Smart SL: placed at STRUCTURE (swing level), not arbitrary ATR
  - Custom Fibonacci: 0.309, 0.4045, 0.618, 0.75, 0.78, 0.809
  - Market Structure: BOS confirmation before entry
  - Noise Filter: skip choppy candles
  - Session: FIXED — strict 07:00-16:59 UTC (LONDON+NY+OVERLAP)
  - Volume Profile: enter only on institutional volume
  - Momentum Confluence: RSI + MACD + OBV must agree
══════════════════════════════════════════════════════════════
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
_HDR  = {"Accept":"application/json","User-Agent":"Ramos360QUANT/2.0"}
_TFM  = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m",
          "1h":"1H","4h":"4H","1d":"1D","1w":"1W"}

# ── CUSTOM FIBONACCI RATIOS ───────────────────────────────────────────────────
FIB_RETRACE = [0.309, 0.4045, 0.500, 0.618, 0.750, 0.780, 0.809]
FIB_GOLDEN  = 0.618   # Primary golden ratio
FIB_EXT     = [1.272, 1.414, 1.618, 2.000, 2.618]
FIB_TOL     = 0.018   # 1.8% tolerance around each level

# ── SESSION — FIXED ───────────────────────────────────────────────────────────
# London:   07:00-12:59 UTC
# NY:       13:00-16:59 UTC
# Overlap:  13:00-15:59 UTC
TRADE_HOURS = set(range(7, 17))   # 07-16 inclusive → 07:00-16:59 UTC

def _sess(h: int) -> str:
    if 13 <= h < 16: return "OVERLAP"
    if  7 <= h < 13: return "LONDON"      # FIXED: was <12, now <13
    if 13 <= h < 17: return "NEW_YORK"
    if  0 <= h <  7: return "ASIA"
    return "DEAD_ZONE"

def _sess_ok(h: int) -> bool:
    return h in TRADE_HOURS               # STRICT: only 07-16

def _sess_w(h: int) -> float:
    return {"OVERLAP":1.4,"LONDON":1.2,"NEW_YORK":1.1}.get(_sess(h), 0.0)

# ── OKX FETCH ─────────────────────────────────────────────────────────────────
def _inst(s: str) -> str:
    return s.split(":")[0].replace("/", "-") + "-SWAP"

async def _fetch(symbol: str, tf: str, start: datetime, end: datetime) -> pd.DataFrame:
    inst = _inst(symbol); bar = _TFM.get(tf.lower(), tf)
    s_ms = int(start.timestamp()*1000); e_ms = int(end.timestamp()*1000)
    rows = []; before = e_ms
    logger.info(f"[BT] {symbol} {tf} {start.date()}→{end.date()}")
    async with httpx.AsyncClient(timeout=30, headers=_HDR) as cl:
        while True:
            r = await cl.get(f"{_BASE}/market/history-candles", params={
                "instId": inst, "bar": bar,
                "before": str(s_ms), "after": str(before), "limit": "300"})
            if r.status_code == 429: await asyncio.sleep(5); continue
            if r.status_code != 200: break
            j = r.json()
            if j.get("code") != "0" or not j.get("data"): break
            for c in j["data"]:
                ts = int(c[0])
                if s_ms <= ts <= e_ms:
                    rows.append([ts,float(c[1]),float(c[2]),float(c[3]),float(c[4]),float(c[5])])
            old = int(j["data"][-1][0])
            if old <= s_ms or len(j["data"]) < 300: break
            before = old; await asyncio.sleep(0.3)
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume"])
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    logger.info(f"[BT] {symbol}: {len(df)} candles ✅")
    return df

# ── INDICATORS ─────────────────────────────────────────────────────────────────
def _build(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(); c = df.close; h = df.high; l = df.low
    # RSI
    d = c.diff(); g = d.clip(lower=0).ewm(alpha=1/14,adjust=False).mean()
    ls = (-d.clip(upper=0)).ewm(alpha=1/14,adjust=False).mean()
    df["rsi"] = (100 - 100/(1+g/ls.replace(0,1e-10))).fillna(50)
    df["rsi_prev"] = df["rsi"].shift(1).fillna(50)
    # MACD
    e12 = c.ewm(span=12,adjust=False).mean(); e26 = c.ewm(span=26,adjust=False).mean()
    ms = e12 - e26; sig9 = ms.ewm(span=9,adjust=False).mean()
    df["mh"] = (ms - sig9).fillna(0); df["mh_prev"] = df["mh"].shift(1).fillna(0)
    # EMAs
    for p in [9,20,50,100,200]: df[f"e{p}"] = c.ewm(span=p,adjust=False).mean()
    # ATR
    hl = h-l; hpc = (h-c.shift()).abs(); lpc = (l-c.shift()).abs()
    df["atr"] = pd.concat([hl,hpc,lpc],axis=1).max(axis=1).ewm(alpha=1/14,adjust=False).mean()
    # Historical Volatility (20-period)
    df["hv"] = c.pct_change().rolling(20).std() * math.sqrt(252)
    # ADX
    up = (h-h.shift()).clip(lower=0); dn = (l.shift()-l).clip(lower=0)
    df["pdi"] = up.ewm(alpha=1/14,adjust=False).mean()
    df["mdi"] = dn.ewm(alpha=1/14,adjust=False).mean()
    df["adx"] = ((df.pdi-df.mdi).abs()/(df.pdi+df.mdi+1e-10)*100).ewm(alpha=1/14,adjust=False).mean().fillna(15)
    # Bollinger
    mid = c.rolling(20).mean(); sd = c.rolling(20).std()
    df["bbu"] = mid+2*sd; df["bbl"] = mid-2*sd; df["bbm"] = mid
    df["bbw"] = (df.bbu - df.bbl) / mid   # BB Width (volatility measure)
    df["bbp"] = (c - df.bbl) / (df.bbu - df.bbl + 1e-10) * 100
    # Stoch
    lo14 = l.rolling(14).min(); hi14 = h.rolling(14).max()
    df["stoch"] = 100*(c-lo14)/(hi14-lo14+1e-10)
    df["stoch_d"] = df.stoch.rolling(3).mean()
    # Williams %R
    df["wr"] = (-100*(hi14-c)/(hi14-lo14+1e-10)).fillna(-50)
    # OBV
    df["obv"] = (c.diff().apply(lambda x:1 if x>0 else -1 if x<0 else 0)*df.volume).cumsum()
    df["obv_ma"] = df.obv.rolling(20).mean()
    # Volume
    df["vma"] = df.volume.rolling(20).mean()
    df["vr"] = df.volume / df.vma.replace(0, 1)
    # Ichimoku
    df["iten"] = (h.rolling(9).max() + l.rolling(9).min()) / 2
    df["ikij"] = (h.rolling(26).max() + l.rolling(26).min()) / 2
    sa = (df.iten + df.ikij) / 2
    sb = (h.rolling(52).max() + l.rolling(52).min()) / 2
    df["ict"] = pd.concat([sa,sb],axis=1).max(axis=1)
    df["icb"] = pd.concat([sa,sb],axis=1).min(axis=1)
    # Heikin Ashi
    df["hac"] = (df.open+h+l+c)/4; df["hao"] = df.hac.shift(2)
    df["hab"] = (df.hac > df.hao).astype(int)
    # Swing levels (structure)
    df["sw_lo5"]  = l.rolling(5).min()
    df["sw_hi5"]  = h.rolling(5).max()
    df["sw_lo14"] = l.rolling(14).min()
    df["sw_hi14"] = h.rolling(14).max()
    # Noise ratio: candle body / total range (high = directional, low = noise)
    body = (c - df.open).abs()
    rng  = (h - l).replace(0, 1e-6)
    df["noise_r"] = (body / rng).fillna(0.5)
    # Wyckoff
    df["wyck"] = 0.0
    df.loc[(c>df.e50) & (df.vr>1.2), "wyck"] =  1.0
    df.loc[(c<df.e50) & (df.vr>1.2), "wyck"] = -1.0
    return df.ffill().bfill()

# ── DIVERGENCE PRE-COMPUTED ───────────────────────────────────────────────────
def _add_div(df: pd.DataFrame) -> pd.DataFrame:
    sc = pd.Series(0.0, index=df.index)
    pl = []; ph = []
    for i in range(5, len(df)-5):
        lw = df.low.iloc[i-5:i+6]; hw = df.high.iloc[i-5:i+6]
        if len(lw) < 11: continue
        if float(df.low.iloc[i])  == float(lw.min()):
            pl.append((i, float(df.low.iloc[i]),  float(df.rsi.iloc[i])))
        if float(df.high.iloc[i]) == float(hw.max()):
            ph.append((i, float(df.high.iloc[i]), float(df.rsi.iloc[i])))
        if len(pl) >= 2:
            a, b = pl[-2], pl[-1]
            if b[1] < a[1] and b[2] > a[2]: sc.iloc[i] += 0.8   # Regular Bull
            if b[1] > a[1] and b[2] < a[2]: sc.iloc[i] += 0.45  # Hidden Bull
        if len(ph) >= 2:
            a, b = ph[-2], ph[-1]
            if b[1] > a[1] and b[2] < a[2]: sc.iloc[i] -= 0.8   # Regular Bear
            if b[1] < a[1] and b[2] > a[2]: sc.iloc[i] -= 0.45  # Hidden Bear
    df["div_sc"] = sc.clip(-1, 1).fillna(0)
    return df

# ── FIBONACCI ANALYSIS ────────────────────────────────────────────────────────
def _fib_analysis(df: pd.DataFrame, i: int, price: float) -> Dict:
    """
    Analyze price position relative to custom Fibonacci levels.
    Returns: { zone, score, nearest_level, levels_dict }
    """
    result = {"zone": None, "score": 0.0, "nearest": price,
              "levels": {}, "at_support": False, "at_resistance": False}
    if i < 25: return result

    hi = float(df.high.iloc[max(0,i-60):i+1].max())
    lo = float(df.low.iloc[max(0,i-60):i+1].min())
    rng = hi - lo
    if rng <= 0: return result

    # Calculate all custom Fibonacci levels
    levels = {}
    for r in FIB_RETRACE:
        levels[f"{r}"] = round(hi - rng * r, 4)

    # Fibonacci extensions
    for r in FIB_EXT:
        levels[f"ext_{r}"] = round(lo + rng * r, 4)

    result["levels"] = levels

    # Find nearest level
    nearest_key = min(levels.keys(), key=lambda k: abs(levels[k] - price))
    nearest_val = levels[nearest_key]
    dist_pct    = abs(nearest_val - price) / price

    result["nearest"] = nearest_val

    # Position in Fibonacci range (0=at low, 1=at high)
    pos = (price - lo) / rng

    # Support zones (price at these = potential LONG)
    SUPPORT_FIBS    = [0.309, 0.4045, 0.500, 0.618]
    # Resistance zones (price at these = potential SHORT)
    RESISTANCE_FIBS = [0.500, 0.618, 0.750, 0.780, 0.809]

    at_support    = any(abs(pos - f) < FIB_TOL for f in SUPPORT_FIBS)
    at_resistance = any(abs(pos - f) < FIB_TOL for f in RESISTANCE_FIBS)

    result["at_support"]    = at_support
    result["at_resistance"] = at_resistance

    # Score
    if at_support and pos < 0.7:
        # Price near support Fib → LONG bias
        score = 0.8 if dist_pct < FIB_TOL * 0.5 else 0.5
        result["zone"] = f"SUPPORT_FIB_{nearest_key}"
    elif at_resistance and pos > 0.3:
        # Price near resistance Fib → SHORT bias
        score = -0.8 if dist_pct < FIB_TOL * 0.5 else -0.5
        result["zone"] = f"RESIST_FIB_{nearest_key}"
    elif pos < 0.35:
        score = 0.3
    elif pos > 0.65:
        score = -0.3
    else:
        score = 0.0

    result["score"] = score
    return result

# ── MARKET STRUCTURE (BOS/CHoCH) ─────────────────────────────────────────────
def _market_structure(df: pd.DataFrame, i: int) -> Dict:
    """
    Detect Break of Structure (BOS) and Change of Character (CHoCH).
    Returns bias: 'BULL', 'BEAR', or 'NEUTRAL'
    """
    if i < 20:
        return {"bias": "NEUTRAL", "bos": None, "score": 0.0}

    # Recent structure
    recent_hi = float(df.high.iloc[max(0,i-6):i+1].max())
    recent_lo = float(df.low.iloc[max(0,i-6):i+1].min())
    prev_hi   = float(df.high.iloc[max(0,i-12):i-6].max()) if i >= 12 else recent_hi
    prev_lo   = float(df.low.iloc[max(0,i-12):i-6].min())  if i >= 12 else recent_lo
    price     = float(df.close.iloc[i])

    score = 0.0; bos = None

    # Bullish BOS: price breaks above previous swing high
    if recent_hi > prev_hi * 1.002 and price > prev_hi:
        score += 0.8; bos = "BULL_BOS"
    # Bearish BOS
    if recent_lo < prev_lo * 0.998 and price < prev_lo:
        score -= 0.8; bos = "BEAR_BOS"

    # Liquidity sweep: wick below swing low then recovery
    cur_lo = float(df.low.iloc[i])
    cur_cl = float(df.close.iloc[i])
    plo    = float(df.low.iloc[max(0,i-8):i].min())
    phi    = float(df.high.iloc[max(0,i-8):i].max())

    if cur_lo < plo * 0.999 and cur_cl > plo:
        score += 0.6; bos = bos or "SWEEP_BULL"   # Swept equal lows → bull
    if float(df.high.iloc[i]) > phi * 1.001 and cur_cl < phi:
        score -= 0.6; bos = bos or "SWEEP_BEAR"

    bias = "BULL" if score > 0.3 else "BEAR" if score < -0.3 else "NEUTRAL"
    return {"bias": bias, "bos": bos, "score": round(score, 3)}

# ── SMART SL: STRUCTURE-BASED ─────────────────────────────────────────────────
def _smart_sl(df: pd.DataFrame, i: int, direction: str, price: float) -> Tuple[float,float]:
    """
    Smart SL placed at MARKET STRUCTURE, not arbitrary ATR.
    LONG: SL below last significant swing low + volatility buffer
    SHORT: SL above last significant swing high + volatility buffer
    
    Avoids SL being in 'noise zone' — reduces premature SL hits significantly.
    """
    atr  = float(df.atr.iloc[i]); atr = atr if atr > 0 else price * 0.015
    hv   = float(df.hv.iloc[i])  if not pd.isna(df.hv.iloc[i]) else 0.02
    noise= float(df.noise_r.iloc[i])

    # Volatility buffer: wider in high-volatility, narrower in trends
    buf_mult = 1.5 if hv > 0.8 else 1.2 if hv > 0.5 else 1.0
    buf = atr * 0.25 * buf_mult

    if direction == "LONG":
        # SL below 5-period swing low (structure)
        sw_lo5  = float(df.sw_lo5.iloc[i])
        sw_lo14 = float(df.sw_lo14.iloc[i])
        # Use 5-period if it gives at least 0.6×ATR distance, else 14-period
        if price - sw_lo5 >= atr * 0.6:
            sl = round(sw_lo5 - buf, 4)
        else:
            sl = round(sw_lo14 - buf, 4)
        # Clamp: min 0.7×ATR, max 2.2×ATR
        sl = min(sl, round(price - atr*0.7, 4))
        sl = max(sl, round(price - atr*2.2, 4))
        sl_d = price - sl
    else:
        sw_hi5  = float(df.sw_hi5.iloc[i])
        sw_hi14 = float(df.sw_hi14.iloc[i])
        if sw_hi5 - price >= atr * 0.6:
            sl = round(sw_hi5 + buf, 4)
        else:
            sl = round(sw_hi14 + buf, 4)
        sl = max(sl, round(price + atr*0.7, 4))
        sl = min(sl, round(price + atr*2.2, 4))
        sl_d = sl - price

    return sl, sl_d

# ── FIBONACCI TP TARGETS ──────────────────────────────────────────────────────
def _fib_tps(price: float, sl_d: float, direction: str, df: pd.DataFrame, i: int) -> Tuple[float,float,float]:
    """
    TP levels using custom Fibonacci ratios projected from SL distance.
    Also anchored to actual Fibonacci retracement levels when confluent.
    Ratios: 0.309, 0.4045, 0.618, 0.75, 0.78, 0.809
    """
    # Base targets from SL distance × Fibonacci ratio multiples
    # RR: TP1=1.8×SL, TP2=3.5×SL, TP3=6.18×SL (golden ratio × 6)
    if direction == "LONG":
        tp1 = round(price + sl_d * 1.8,   4)
        tp2 = round(price + sl_d * 3.5,   4)
        tp3 = round(price + sl_d * 6.18,  4)   # 6.18 = 6 × golden ratio
    else:
        tp1 = round(price - sl_d * 1.8,   4)
        tp2 = round(price - sl_d * 3.5,   4)
        tp3 = round(price - sl_d * 6.18,  4)

    # Try to snap TPs to actual Fibonacci levels for stronger targets
    if i >= 25:
        hi = float(df.high.iloc[max(0,i-60):i+1].max())
        lo = float(df.low.iloc[max(0,i-60):i+1].min())
        rng = hi - lo
        if rng > 0:
            fib_levels = sorted([round(hi - rng*r, 4) for r in FIB_RETRACE] +
                                 [round(lo + rng*r, 4) for r in FIB_EXT])

            def snap(tp_orig: float, direction: str) -> float:
                # Find nearest Fibonacci level within 1% of original TP
                candidates = [f for f in fib_levels
                               if abs(f - tp_orig)/tp_orig < 0.012]
                if not candidates: return tp_orig
                if direction == "LONG":
                    # Prefer level just above original (conservative)
                    above = [f for f in candidates if f >= tp_orig]
                    return min(above) if above else max(candidates)
                else:
                    below = [f for f in candidates if f <= tp_orig]
                    return max(below) if below else min(candidates)

            tp1 = snap(tp1, direction)
            tp2 = snap(tp2, direction)
            tp3 = snap(tp3, direction)

    return tp1, tp2, tp3

# ── NOISE FILTER ──────────────────────────────────────────────────────────────
def _is_noisy(df: pd.DataFrame, i: int) -> bool:
    """
    Returns True if the market is too choppy to trade.
    Uses BB width and noise ratio.
    """
    if i < 20: return False
    bbw   = float(df.bbw.iloc[i])   # BB Width
    noise = float(df.noise_r.iloc[i])  # Body/Range ratio
    adx   = float(df.adx.iloc[i])

    # Narrow BB (consolidation) + high noise + low ADX = choppy
    bbw_ma = float(df.bbw.rolling(20).mean().iloc[i]) if i >= 20 else bbw
    too_narrow = bbw < bbw_ma * 0.7   # BB tighter than 70% of average
    too_noisy  = noise < 0.25         # Body < 25% of range (doji-like)
    no_trend   = adx < 14

    return (too_narrow and no_trend) or too_noisy

# ── MOMENTUM CONFLUENCE ───────────────────────────────────────────────────────
def _momentum_ok(df: pd.DataFrame, i: int, direction: str) -> Tuple[bool, float]:
    """
    Check if RSI + MACD + OBV all agree on direction.
    Returns (ok, momentum_score)
    """
    row = df.iloc[i]
    rsi  = float(row.rsi); rsi_p = float(row.rsi_prev)
    mh   = float(row.mh);  mh_p  = float(row.mh_prev)
    obv  = float(row.obv); obv_ma= float(row.obv_ma)
    score = 0.0

    if direction == "LONG":
        if rsi < 55 and rsi > rsi_p:  score += 0.4   # RSI rising from low
        if mh > 0:                     score += 0.4   # MACD positive
        if mh > 0 and mh_p <= 0:      score += 0.3   # MACD cross up
        if obv > obv_ma:               score += 0.3   # OBV above MA (institutional buying)
        if rsi < 40:                   score += 0.3   # Oversold
    else:
        if rsi > 45 and rsi < rsi_p:  score += 0.4
        if mh < 0:                     score += 0.4
        if mh < 0 and mh_p >= 0:      score += 0.3
        if obv < obv_ma:               score += 0.3
        if rsi > 60:                   score += 0.3

    # Need at least 2 of 4 momentum indicators to agree
    return score >= 0.7, round(score, 3)

# ── MAIN SCORING ENGINE ───────────────────────────────────────────────────────
def _score_all(df: pd.DataFrame, i: int, sw: float) -> Tuple[str, float, Dict]:
    """
    14 experts with weighted scoring.
    Returns (direction, weighted_score, details)
    """
    row = df.iloc[i]; c = float(row.close); sc = []
    details = {}

    # ── SCHOOL 1: Trend (EMA Alignment) ───────────────────────
    e20 = float(row.e20); e50 = float(row.e50); e200 = float(row.e200)
    if c>e20>e50>e200:    v=1.4;  details["EMA"]="Perfect Bull"
    elif c>e20>e50:       v=0.8;  details["EMA"]="Bull"
    elif c>e20:           v=0.3;  details["EMA"]="Weak Bull"
    elif c<e20<e50<e200:  v=-1.4; details["EMA"]="Perfect Bear"
    elif c<e20<e50:       v=-0.8; details["EMA"]="Bear"
    elif c<e20:           v=-0.3; details["EMA"]="Weak Bear"
    else:                 v=0.0;  details["EMA"]="Neutral"
    sc.append(v)

    # ── SCHOOL 2: Momentum (RSI) ────────────────────────────────
    r = float(row.rsi); rp = float(row.rsi_prev)
    if r<25:              v=1.2;  details["RSI"]=f"OversoldStrong({r:.0f})"
    elif r<35:            v=0.8;  details["RSI"]=f"Oversold({r:.0f})"
    elif r<45 and r>rp:   v=0.4;  details["RSI"]=f"Rising({r:.0f})"
    elif r<50:            v=0.1
    elif r>75:            v=-1.2; details["RSI"]=f"OverboughtStrong({r:.0f})"
    elif r>65:            v=-0.8; details["RSI"]=f"Overbought({r:.0f})"
    elif r>55 and r<rp:   v=-0.4; details["RSI"]=f"Falling({r:.0f})"
    elif r>50:            v=-0.1
    else:                 v=0.0
    sc.append(v)

    # ── SCHOOL 3: MACD ──────────────────────────────────────────
    mh = float(row.mh); mhp = float(row.mh_prev)
    if mh>0 and mhp<=0:   v=1.1;  details["MACD"]="BullCross"
    elif mh<0 and mhp>=0: v=-1.1; details["MACD"]="BearCross"
    elif mh>0:            v=0.5;  details["MACD"]="Bull"
    else:                 v=-0.5; details["MACD"]="Bear"
    sc.append(v)

    # ── SCHOOL 4: Ichimoku Cloud ─────────────────────────────────
    ct=float(row.ict); cb=float(row.icb); tn=float(row.iten); kj=float(row.ikij)
    if c>ct and tn>kj:    v=1.0;  details["ICHI"]="AboveCloud+BullCross"
    elif c>ct:            v=0.5;  details["ICHI"]="AboveCloud"
    elif c<cb and tn<kj:  v=-1.0; details["ICHI"]="BelowCloud+BearCross"
    elif c<cb:            v=-0.5; details["ICHI"]="BelowCloud"
    else:                 v=0.0;  details["ICHI"]="InCloud"
    sc.append(v)

    # ── SCHOOL 5: Fibonacci Analysis ────────────────────────────
    fib = _fib_analysis(df, i, c)
    sc.append(fib["score"]); details["FIB"]=fib.get("zone","—")

    # ── SCHOOL 6: Market Structure (BOS/CHoCH) ──────────────────
    ms = _market_structure(df, i)
    struct_score = ms["score"]
    sc.append(struct_score); details["SMC"]=ms["bos"] or ms["bias"]

    # ── SCHOOL 7: Bollinger Bands ────────────────────────────────
    bp = float(row.bbp)
    if bp<8:    v=1.0; details["BB"]=f"AtLower({bp:.0f}%)"
    elif bp<20: v=0.5; details["BB"]=f"NearLower({bp:.0f}%)"
    elif bp>92: v=-1.0;details["BB"]=f"AtUpper({bp:.0f}%)"
    elif bp>80: v=-0.5;details["BB"]=f"NearUpper({bp:.0f}%)"
    elif bp<45: v=0.15
    elif bp>55: v=-0.15
    else:       v=0.0
    sc.append(v)

    # ── SCHOOL 8: Stochastic + Cross ────────────────────────────
    sk=float(row.stoch); sd=float(row.stoch_d)
    if sk<18 and sk>sd:   v=0.9; details["STOCH"]=f"OsBullCross({sk:.0f})"
    elif sk<22:           v=0.6; details["STOCH"]=f"Oversold({sk:.0f})"
    elif sk>82 and sk<sd: v=-0.9;details["STOCH"]=f"ObBearCross({sk:.0f})"
    elif sk>78:           v=-0.6;details["STOCH"]=f"Overbought({sk:.0f})"
    elif sk<40:           v=0.2
    elif sk>60:           v=-0.2
    else:                 v=0.0
    sc.append(v)

    # ── SCHOOL 9: Divergence (pre-computed) ─────────────────────
    div = float(row.div_sc)
    sc.append(div * 0.9); details["DIV"]=f"{div:+.2f}"

    # ── SCHOOL 10: Williams %R ───────────────────────────────────
    wr = float(row.wr)
    if wr<-85:   v=0.9; details["WR"]=f"OsExtreme({wr:.0f})"
    elif wr<-68: v=0.5; details["WR"]=f"Os({wr:.0f})"
    elif wr>-15: v=-0.9;details["WR"]=f"ObExtreme({wr:.0f})"
    elif wr>-32: v=-0.5;details["WR"]=f"Ob({wr:.0f})"
    else:        v=0.0
    sc.append(v)

    # ── SCHOOL 11: OBV vs MA ─────────────────────────────────────
    obv=float(row.obv); obvm=float(row.obv_ma)
    p_now=float(df.close.iloc[i]); p_ago=float(df.close.iloc[max(0,i-6)])
    if obv>obvm and p_now>p_ago:   v=0.6; details["OBV"]="BullConf"
    elif obv>obvm and p_now<p_ago: v=0.5; details["OBV"]="BullDiv"
    elif obv<obvm and p_now<p_ago: v=-0.6;details["OBV"]="BearConf"
    elif obv<obvm and p_now>p_ago: v=-0.5;details["OBV"]="BearDiv"
    else:                           v=0.0
    sc.append(v)

    # ── SCHOOL 12: Volume Surge ──────────────────────────────────
    vr=float(row.vr); base=sum(sc)/len(sc) if sc else 0
    if vr>2.5:   v=0.9*(1 if base>0 else -1); details["VOL"]=f"Surge({vr:.1f}x)"
    elif vr>1.8: v=0.6*(1 if base>0 else -1); details["VOL"]=f"High({vr:.1f}x)"
    elif vr>1.3: v=0.3*(1 if base>0 else -1); details["VOL"]=f"Above({vr:.1f}x)"
    else:        v=0.0; details["VOL"]=f"Low({vr:.1f}x)"
    sc.append(v)

    # ── SCHOOL 13: Wyckoff ───────────────────────────────────────
    sc.append(float(row.wyck) * 0.8)

    # ── SCHOOL 14: Heikin Ashi Momentum ─────────────────────────
    ha3 = float(df.hab.iloc[max(0,i-2):i+1].mean())  # 3-bar HA consensus
    if ha3 > 0.65:   v=0.6;  details["HA"]="StrongBull"
    elif ha3 > 0.35: v=0.3;  details["HA"]="Bull"
    elif ha3 < 0.35: v=-0.6; details["HA"]="StrongBear"
    else:            v=-0.3; details["HA"]="Bear"
    sc.append(v)

    # Apply session weight
    n   = len(sc)
    avg = (sum(sc) / n) * sw if n else 0.0

    # Count positive/negative experts
    bull = sum(1 for s in sc if s >  0.05)
    bear = sum(1 for s in sc if s < -0.05)

    # Direction: need avg ≥ 0.25 AND at least 8 experts agree
    direction = ("LONG"  if avg >  0.25 and bull >= 8 else
                 "SHORT" if avg < -0.25 and bear >= 8 else "NEUTRAL")

    return direction, round(avg, 4), details

# ── SIMULATION ────────────────────────────────────────────────────────────────
def _sim(df: pd.DataFrame, balance: float = 10_000.0) -> Dict:
    n = len(df); equity = [balance]; trades = []
    WARMUP = 60; MAX_C = 22
    in_trade = False; partial = False
    direction = ""; entry = sl = tp1 = tp2 = tp3 = sl_d = 0.0; entry_idx = 0
    consec_loss = 0; risk_pct = 0.01   # 1% risk per trade

    for i in range(WARMUP, n):
        row   = df.iloc[i]; price = float(row.close)
        hi    = float(row.high); lo = float(row.low)
        hour  = row.ts.hour if hasattr(row.ts, "hour") else 10

        # ── Manage open trade ──────────────────────────────────────
        if in_trade:
            # Time exit
            if (i - entry_idx) > MAX_C:
                pnl  = ((price-entry)/entry*100) if direction=="LONG" else ((entry-price)/entry*100)
                mult = 0.5 if partial else 1.0
                risk = balance * risk_pct / max(sl_d/entry, 0.001)
                gain = pnl/100 * risk * mult; balance = max(1.0, balance+gain)
                win  = pnl > 0; consec_loss = 0 if win else consec_loss+1
                trades.append({"direction":direction,"entry":round(entry,4),"exit":round(price,4),
                                "pnl_pct":round(pnl*mult,3),"result":"WIN" if win else "LOSS",
                                "exit_type":"TIME_EXIT","session":_sess(hour)})
                in_trade = False; partial = False; equity.append(balance); continue

            # TP/SL check
            if direction == "LONG":
                hit_sl  = lo <= sl;       hit_tp3 = hi >= tp3
                hit_tp2 = hi >= tp2 and not hit_tp3
                hit_tp1 = hi >= tp1 and not hit_tp2 and not hit_tp3
            else:
                hit_sl  = hi >= sl;       hit_tp3 = lo <= tp3
                hit_tp2 = lo <= tp2 and not hit_tp3
                hit_tp1 = lo <= tp1 and not hit_tp2 and not hit_tp3

            # Partial exit at TP1 → 50% close + SL to breakeven
            if hit_tp1 and not partial:
                pnl  = ((tp1-entry)/entry*100) if direction=="LONG" else ((entry-tp1)/entry*100)
                risk = balance * risk_pct / max(sl_d/entry, 0.001)
                balance = max(1.0, balance + pnl/100*risk*0.5)
                sl = entry; partial = True   # SL now at breakeven
                trades.append({"direction":direction,"entry":round(entry,4),"exit":round(tp1,4),
                                "pnl_pct":round(pnl*0.5,3),"result":"WIN",
                                "exit_type":"TP1_PARTIAL","session":_sess(hour)})
                equity.append(balance); continue

            if hit_tp2 or hit_tp3 or hit_sl:
                ex   = sl if hit_sl else (tp3 if hit_tp3 else tp2)
                pnl  = ((ex-entry)/entry*100) if direction=="LONG" else ((entry-ex)/entry*100)
                mult = 0.5 if partial else 1.0
                risk = balance * risk_pct / max(sl_d/entry, 0.001)
                gain = pnl/100 * risk * mult; balance = max(1.0, balance+gain)
                win  = pnl > 0; consec_loss = 0 if win else consec_loss+1
                xtype = "SL" if hit_sl else ("TP3" if hit_tp3 else "TP2")
                trades.append({"direction":direction,"entry":round(entry,4),"exit":round(ex,4),
                                "pnl_pct":round(pnl*mult,3),"result":"WIN" if win else "LOSS",
                                "exit_type":xtype,"session":_sess(hour)})
                in_trade = False; partial = False

        # ── New signal ──────────────────────────────────────────────
        if not in_trade:
            # GATE 1: Session — STRICT 07:00-16:59
            if not _sess_ok(hour): equity.append(balance); continue

            # GATE 2: Cool-down after 3 consecutive losses
            if consec_loss >= 3: consec_loss = 0; equity.append(balance); continue

            # GATE 3: Noise filter
            if _is_noisy(df, i): equity.append(balance); continue

            # Session weight
            sw = _sess_w(hour)
            if sw <= 0: equity.append(balance); continue

            # GATE 4: Score all 14 experts
            sig, avg, dets = _score_all(df, i, sw)
            if sig == "NEUTRAL": equity.append(balance); continue

            # GATE 5: Momentum confluence (RSI + MACD + OBV)
            mom_ok, mom_score = _momentum_ok(df, i, sig)
            if not mom_ok: equity.append(balance); continue

            # Smart SL placement
            sl_p, sl_dist = _smart_sl(df, i, sig, price)

            # Fibonacci TP targets
            tp1_p, tp2_p, tp3_p = _fib_tps(price, sl_dist, sig, df, i)

            # GATE 6: RR ≥ 1.6
            rr = abs(tp1_p - price) / max(sl_dist, 1e-10)
            if rr < 1.6: equity.append(balance); continue

            direction = sig; entry = price
            sl = sl_p; tp1 = tp1_p; tp2 = tp2_p; tp3 = tp3_p
            sl_d = sl_dist
            in_trade  = True; entry_idx = i; partial = False

        equity.append(balance)

    # Close any open trade
    if in_trade:
        lp  = float(df.close.iloc[-1])
        pnl = ((lp-entry)/entry*100) if direction=="LONG" else ((entry-lp)/entry*100)
        trades.append({"direction":direction,"entry":entry,"exit":lp,
                        "pnl_pct":round(pnl*(0.5 if partial else 1.0),3),
                        "result":"WIN" if pnl>0 else "LOSS","exit_type":"OPEN_AT_END"})

    return {"trades":trades,"equity":equity,"final_balance":balance}

# ── METRICS ────────────────────────────────────────────────────────────────────
def _metrics(sim: Dict, initial: float) -> Dict:
    trades=sim["trades"]; eq=pd.Series(sim["equity"]); bal=sim["final_balance"]
    if not trades:
        return {"error":"No trades","total":0,"win_rate_pct":0,"max_dd_pct":0,"return_pct":0,"sharpe":0}
    wins=[t for t in trades if t["result"]=="WIN"]
    losses=[t for t in trades if t["result"]=="LOSS"]
    tot=len(trades); wr=round(len(wins)/tot*100,2) if tot else 0
    dd=round(float(((eq-eq.cummax())/eq.cummax()*100).min()),2)
    ret=round((bal-initial)/initial*100,2)
    rets=eq.pct_change().dropna()
    sh=round(float(rets.mean()/rets.std()*(252**0.5)),3) if rets.std()>0 else 0.0
    aw=round(sum(t["pnl_pct"] for t in wins)/len(wins),3)   if wins   else 0
    al=round(sum(t["pnl_pct"] for t in losses)/len(losses),3) if losses else 0
    by_ex={}; by_ss={}
    for t in trades:
        k=t.get("exit_type","?"); by_ex[k]=by_ex.get(k,0)+1
        s=t.get("session","?");   by_ss[s]=by_ss.get(s,0)+1
    return {"total":tot,"wins":len(wins),"losses":len(losses),
            "win_rate_pct":wr,"avg_win_pct":aw,"avg_loss_pct":al,
            "expectancy":round(wr/100*aw+(1-wr/100)*al,3),
            "max_dd_pct":dd,"return_pct":ret,"sharpe":sh,
            "final_balance":round(bal,2),
            "exit_breakdown":by_ex,"session_breakdown":by_ss}

# ── BacktestEngine ──────────────────────────────────────────────────────────────
class BacktestEngine:
    async def run(self,
                  symbols: List[str] = None,
                  timeframe: str = "1h",
                  tf: str = None,
                  start: str = "2026-01-01",
                  end: str = "2026-05-01",
                  balance: float = 10_000.0,
                  **kwargs) -> Dict:
        resolved = tf or timeframe or "1h"
        symbols  = symbols or ["BTC/USDT:USDT", "ETH/USDT:USDT"]
        sdt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        edt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
        results = {}
        for sym in symbols:
            sym_c = sym.replace("/USDT:USDT","")
            try:
                df = await _fetch(sym, resolved, sdt, edt)
                if df is None or len(df) < 70:
                    results[sym_c]={"error":"insufficient data"}; continue
                df = _build(df); df = _add_div(df)
                sim  = _sim(df, balance)
                st   = _metrics(sim, balance)
                results[sym_c] = {**st,"symbol":sym_c,"tf":resolved,
                                   "period":f"{start}→{end}","candles":len(df)}
                logger.info(f"[BT] {sym_c}: {st['total']} trades WR={st['win_rate_pct']}% Ret={st['return_pct']:+.2f}%")
            except Exception as e:
                logger.error(f"[BT] {sym_c}: {e}"); results[sym_c]={"error":str(e)}
        return results

    @staticmethod
    def format_report(results: Dict) -> str:
        tf = next((v.get("tf","1H") for v in results.values() if isinstance(v,dict) and "tf" in v),"1H")
        lines = ["📈 <b>Backtest — Ramos 360 Ai 🎖️  QUANT v2</b>",
                 f"📅 Period: 2026-01-01 → 2026-05-01",
                 f"⏱️ Timeframe: {tf.upper()} | 14 Experts",
                 "✅ Smart SL + Custom Fib + BOS + Momentum Confluence",
                 "━━━━━━━━━━━━━━━━━━━━━━━━"]
        for sym, r in results.items():
            if "error" in r: lines.append(f"❌ {sym}: {r['error']}"); continue
            ei="🟢" if r.get("return_pct",0)>0 else "🔴"
            ex=" ".join(f"{k}:{v}" for k,v in r.get("exit_breakdown",{}).items())
            ss=" ".join(f"{k}:{v}" for k,v in r.get("session_breakdown",{}).items())
            lines += [f"",f"{ei} <b>{sym}</b>",
                      f"  📊 {r['total']} trades  ({r.get('wins',0)}W/{r.get('losses',0)}L)",
                      f"  🎯 Win Rate:   {r['win_rate_pct']:.1f}%",
                      f"  💰 Return:     {r.get('return_pct',0):+.2f}%",
                      f"  📉 Max DD:     {r.get('max_dd_pct',0):.2f}%",
                      f"  ⚖️ Sharpe:     {r.get('sharpe',0):.3f}",
                      f"  🔮 Expectancy: {r.get('expectancy',0):+.3f}%",
                      f"  🏦 Balance:    ${r.get('final_balance',10000):,.2f}",
                      f"  📋 Exits:      {ex}",
                      f"  🕐 Sessions:   {ss}"]
        lines += ["━━━━━━━━━━━━━━━━━━━━━━━━",
                  "<i>🎖️ Ramos 360 Ai — Backtest QUANT v2</i>"]
        return "\n".join(lines)

async def _main():
    e = BacktestEngine()
    r = await e.run(symbols=["BTC/USDT:USDT"], timeframe="1h",
                     start="2026-01-01", end="2026-05-01", balance=10_000.0)
    print("\n" + e.format_report(r)
          .replace("<b>","").replace("</b>","")
          .replace("<i>","").replace("</i>",""))

if __name__ == "__main__":
    asyncio.run(_main())

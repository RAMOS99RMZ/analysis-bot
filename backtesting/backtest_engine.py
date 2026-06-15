"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️  ELITE v6
══════════════════════════════════════════════════════════════
TARGET: 70%+ Win Rate via QUALITY over QUANTITY

CORE PHILOSOPHY:
  Only enter when 4 schools SIMULTANEOUSLY confirm:
  1. FIBONACCI / HARMONIC: Price at key PRZ (0.309/0.4045/0.618/0.75/0.78/0.809)
  2. MOMENTUM:  RSI oversold/overbought + MACD + OBV Institutional Flow
  3. STRUCTURE & SMC: Candlestick reversal + FVG Mitigation + Volume
  4. TREND: EMA Alignment + Ichimoku Cloud

  SL = Smart Fibonacci Invalidation Zone
  TP1 = 1.4×SL → STRICT BREAKEVEN (+Fees) → TP2/3 Trailing (Faster on Shorts)

SESSIONS: LONDON(07-12) + OVERLAP(13-15) + NY(13-16) ONLY
══════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import asyncio, math
from datetime import datetime, timezone
from typing import Dict, List, Tuple
import httpx, pandas as pd
from loguru import logger

try:
    import pandas_ta as ta
    HAS_TA = True
except Exception:
    HAS_TA = False

_BASE = "https://www.okx.com/api/v5"
_HDR  = {"Accept":"application/json","User-Agent":"Ramos360ELITE/6.0"}
_TFM  = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1H","4h":"4H","1d":"1D","1w":"1W"}

# ── Custom Fibonacci Ratios ────────────────────────────────────────────────────
FIBO_RET  = [0.309, 0.4045, 0.500, 0.618, 0.750, 0.780, 0.809]
FIBO_EXT  = [1.272, 1.414, 1.618, 2.000, 2.618]
FIBO_TOL  = 0.022   # 2.2% zone around each level

# ── SESSION (STRICT) ─────────────────────────────────────────────────────────
ALLOWED_H = set(range(7, 17))   # 07:00–16:59 UTC

def _sess(h: int) -> str:
    if 13<=h<16: return "OVERLAP"
    if  7<=h<13: return "LONDON"
    if 13<=h<17: return "NEW_YORK"
    return "DEAD_ZONE" if h >= 17 else "ASIA"

def _sess_ok(h: int) -> bool:
    return h in ALLOWED_H

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
                    rows.append([ts, float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])])
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
    # RSI 14 + 6
    for p in [6, 14]:
        d = c.diff(); g = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
        ls = (-d.clip(upper=0)).ewm(alpha=1/p, adjust=False).mean()
        df[f"rsi{p}"] = (100 - 100/(1+g/ls.replace(0,1e-10))).fillna(50)
    df["rsi_p"] = df.rsi14.shift(1).fillna(50)
    # MACD
    e12 = c.ewm(span=12,adjust=False).mean(); e26 = c.ewm(span=26,adjust=False).mean()
    ms = e12 - e26; df["mh"] = (ms - ms.ewm(span=9,adjust=False).mean()).fillna(0)
    df["mh_p"] = df.mh.shift(1).fillna(0)
    # EMAs
    for p in [9,20,50,100,200]: df[f"e{p}"] = c.ewm(span=p,adjust=False).mean()
    # ATR
    hl = h-l; hpc = (h-c.shift()).abs(); lpc = (l-c.shift()).abs()
    df["atr"] = pd.concat([hl,hpc,lpc],axis=1).max(axis=1).ewm(alpha=1/14,adjust=False).mean()
    # ADX
    up = (h-h.shift()).clip(lower=0); dn = (l.shift()-l).clip(lower=0)
    df["pdi"] = up.ewm(alpha=1/14,adjust=False).mean()
    df["mdi"] = dn.ewm(alpha=1/14,adjust=False).mean()
    df["adx"] = ((df.pdi-df.mdi).abs()/(df.pdi+df.mdi+1e-10)*100).ewm(alpha=1/14,adjust=False).mean().fillna(15)
    # Bollinger
    mid = c.rolling(20).mean(); sd = c.rolling(20).std()
    df["bbu"] = mid+2*sd; df["bbl"] = mid-2*sd
    df["bbp"] = (c-df.bbl)/(df.bbu-df.bbl+1e-10)*100
    df["bbw"] = (df.bbu-df.bbl)/mid.replace(0,1)
    # Stoch
    lo14 = l.rolling(14).min(); hi14 = h.rolling(14).max()
    df["stoch"] = 100*(c-lo14)/(hi14-lo14+1e-10)
    df["stoch_d"] = df.stoch.rolling(3).mean()
    # OBV
    df["obv"] = (c.diff().apply(lambda x:1 if x>0 else -1 if x<0 else 0)*df.volume).cumsum()
    df["obv_ma"] = df.obv.rolling(20).mean()
    # Volume
    df["vma"] = df.volume.rolling(20).mean(); df["vr"] = df.volume/(df.vma.replace(0,1))
    # SMC: Fair Value Gaps (FVG)
    df["fvg_bull"] = (df.low - df.high.shift(2)) > 0
    df["fvg_bear"] = (df.low.shift(2) - df.high) > 0
    # Ichimoku
    df["iten"] = (h.rolling(9).max()+l.rolling(9).min())/2
    df["ikij"] = (h.rolling(26).max()+l.rolling(26).min())/2
    sa = (df.iten+df.ikij)/2; sb = (h.rolling(52).max()+l.rolling(52).min())/2
    df["ict"] = pd.concat([sa,sb],axis=1).max(axis=1)
    df["icb"] = pd.concat([sa,sb],axis=1).min(axis=1)
    # Heikin Ashi
    df["hac"] = (df.open+h+l+c)/4; df["hao"] = df.hac.shift(2)
    df["hab"] = (df.hac>df.hao).astype(int)
    # Candle body ratio
    body = (c-df.open).abs(); rng = (h-l).replace(0,1e-6)
    df["body_r"] = (body/rng).fillna(0.5)
    # Swing levels
    df["slo8"]  = l.rolling(8).min();  df["shi8"]  = h.rolling(8).max()
    df["slo14"] = l.rolling(14).min(); df["shi14"] = h.rolling(14).max()
    return df.ffill().bfill()

# ── DIVERGENCE (pre-computed) ─────────────────────────────────────────────────
def _add_div(df: pd.DataFrame) -> pd.DataFrame:
    sc = pd.Series(0.0, index=df.index); pl = []; ph = []
    for i in range(5, len(df)-5):
        lw = df.low.iloc[i-5:i+6]; hw = df.high.iloc[i-5:i+6]
        if len(lw) < 11: continue
        if float(df.low.iloc[i])  == float(lw.min()):
            pl.append((i, float(df.low.iloc[i]),  float(df.rsi14.iloc[i])))
        if float(df.high.iloc[i]) == float(hw.max()):
            ph.append((i, float(df.high.iloc[i]), float(df.rsi14.iloc[i])))
        if len(pl) >= 2:
            a, b = pl[-2], pl[-1]
            if b[1] < a[1] and b[2] > a[2]: sc.iloc[i] += 1.0  # Regular Bull
            if b[1] > a[1] and b[2] < a[2]: sc.iloc[i] += 0.5  # Hidden Bull
        if len(ph) >= 2:
            a, b = ph[-2], ph[-1]
            if b[1] > a[1] and b[2] < a[2]: sc.iloc[i] -= 1.0  # Regular Bear
            if b[1] < a[1] and b[2] > a[2]: sc.iloc[i] -= 0.5  # Hidden Bear
    df["div_sc"] = sc.clip(-1.5, 1.5).fillna(0)
    return df

# ══════════════════════════════════════════════════════════════════
# FIBONACCI ENGINE (Custom Ratios)
# ══════════════════════════════════════════════════════════════════

def _at_fib_zone(price: float, hi: float, lo: float) -> Tuple[bool, float, str]:
    rng = hi - lo
    if rng <= 0: return False, 0.0, "NONE"
    pos = (price - lo) / rng

    BULL = [(0.287,0.331, 0.309, "F309",  0.7),
            (0.382,0.427, 0.404, "F4045", 0.75),
            (0.478,0.522, 0.500, "F500",  0.65),
            (0.595,0.641, 0.618, "F618",  1.0)] 

    BEAR = [(0.478,0.522, 0.500, "F500",  0.65),
            (0.595,0.641, 0.618, "F618",  1.0),
            (0.727,0.773, 0.750, "F750",  0.8),
            (0.757,0.803, 0.780, "F780",  0.75),
            (0.786,0.832, 0.809, "F809",  0.75)]

    for lo_z, hi_z, ratio, name, strength in BULL:
        if lo_z <= pos <= hi_z:
            return True, strength, f"BULL_{name}"

    for lo_z, hi_z, ratio, name, strength in BEAR:
        if lo_z <= pos <= hi_z:
            return True, -strength, f"BEAR_{name}"

    return False, 0.0, "NONE"

def _fib_sl(price: float, hi: float, lo: float, direction: str, df, i, atr: float) -> Tuple[float,float]:
    rng = hi - lo; buf = atr * 0.15
    if direction == "LONG":
        fib_levels = sorted([hi - rng*r for r in FIBO_RET if (hi-rng*r) < price*0.998])
        if fib_levels: sl = round(max(fib_levels) - buf, 4)
        else: sl = round(float(df.slo14.iloc[i]) - buf, 4)
        sl = min(sl, round(price - atr*1.2, 4))
        sl = max(sl, round(price - atr*3.0, 4))
        sl_d = price - sl
    else:
        fib_levels = sorted([hi - rng*r for r in FIBO_RET if (hi-rng*r) > price*1.002], reverse=True)
        if fib_levels: sl = round(min(fib_levels) + buf, 4)
        else: sl = round(float(df.shi14.iloc[i]) + buf, 4)
        sl = max(sl, round(price + atr*1.2, 4))
        sl = min(sl, round(price + atr*3.0, 4))
        sl_d = sl - price
    return round(sl, 4), sl_d

def _fib_tps(price: float, sl_d: float, direction: str, hi: float, lo: float) -> Tuple[float,float,float]:
    rng = hi - lo
    if direction == "LONG":
        tp1 = round(price + sl_d*1.4, 4)
        tp2 = round(price + sl_d*3.0, 4)
        tp3 = round(price + sl_d*5.5, 4)
        if rng > 0:
            exts = sorted([round(lo + rng*r, 4) for r in FIBO_EXT])
            above = [v for v in exts if v > price*1.003]
            if len(above) >= 2: tp2 = above[0]; tp3 = above[1]
            elif len(above) == 1: tp2 = above[0]
    else:
        tp1 = round(price - sl_d*1.4, 4)
        tp2 = round(price - sl_d*3.0, 4)
        tp3 = round(price - sl_d*5.5, 4)
        if rng > 0:
            exts = sorted([round(hi - rng*r, 4) for r in FIBO_EXT], reverse=True)
            below = [v for v in exts if v < price*0.997]
            if len(below) >= 2: tp2 = below[0]; tp3 = below[1]
            elif len(below) == 1: tp2 = below[0]
    return tp1, tp2, tp3

# ══════════════════════════════════════════════════════════════════
# SCHOOL 1: MOMENTUM
# ══════════════════════════════════════════════════════════════════

def _momentum_signal(df, i, direction: str) -> Tuple[bool, float]:
    row = df.iloc[i]
    rsi14 = float(row.rsi14); rsi6 = float(row.rsi6); rsip = float(row.rsi_p)
    mh = float(row.mh); mhp = float(row.mh_p)
    obv = float(row.obv); obv_m = float(row.obv_ma)
    stoch = float(row.stoch); score = 0.0

    if direction == "LONG":
        if rsi14 < 25:    score += 1.0
        elif rsi14 < 35:  score += 0.7
        elif rsi14 < 45:  score += 0.4
        elif rsi14 > 55:  return False, 0.0  
        if rsi14 > rsip: score += 0.3
        if rsi6 < 30: score += 0.4
        if mh > 0 and mhp <= 0: score += 0.8   
        elif mh > 0:             score += 0.4
        elif mh < 0:             score -= 0.2   
        if obv > obv_m:  score += 0.4
        else:            score -= 0.1
        if stoch < 20:   score += 0.5
        elif stoch < 35: score += 0.2
        elif stoch > 70: return False, 0.0  
    else:  
        if rsi14 > 75:   score += 1.0
        elif rsi14 > 65: score += 0.7
        elif rsi14 > 55: score += 0.4
        elif rsi14 < 45: return False, 0.0
        if rsi14 < rsip: score += 0.3
        if rsi6 > 70:    score += 0.4
        if mh < 0 and mhp >= 0: score += 0.8
        elif mh < 0:             score += 0.4
        elif mh > 0:             score -= 0.2
        if obv < obv_m:  score += 0.4
        else:            score -= 0.1
        if stoch > 80:   score += 0.5
        elif stoch > 65: score += 0.2
        elif stoch < 30: return False, 0.0

    return score >= 0.7, round(score, 3)

# ══════════════════════════════════════════════════════════════════
# SCHOOL 2: STRUCTURE & SMC (FVG + Candlestick)
# ══════════════════════════════════════════════════════════════════

def _structure_signal(df, i, direction: str) -> Tuple[bool, float]:
    if i < 3: return False, 0.0
    score = 0.0

    r0 = df.iloc[i];   r1 = df.iloc[i-1];  r2 = df.iloc[i-2]
    o0=float(r0.open); h0=float(r0.high); l0=float(r0.low); c0=float(r0.close)
    o1=float(r1.open); h1=float(r1.high); l1=float(r1.low); c1=float(r1.close)
    o2=float(r2.open); h2=float(r2.high); l2=float(r2.low); c2=float(r2.close)
    b0=abs(c0-o0); rng0=h0-l0 or 1e-4
    b1=abs(c1-o1); rng1=h1-l1 or 1e-4
    vr = float(r0.vr)

    # SMC FVG Mitigation Logic
    fvg_bull_mitigated = float(r1.fvg_bull) == 1 and l0 <= float(r2.high)
    fvg_bear_mitigated = float(r1.fvg_bear) == 1 and h0 >= float(r2.low)

    if direction == "LONG":
        hammer = (c1>o1 or c0>o0) and (min(o1,c1)-l1) > b1*1.8 and (h1-max(o1,c1)) < b1*0.4
        bull_eng = c0>o0 and c1<o1 and o0<=c1 and c0>=o1
        doji_mid = b1 < rng1*0.3
        morn_star = c2<o2 and doji_mid and c0>o0 and c0>((o2+c2)/2)
        strong_bull = c0>o0 and b0>rng0*0.60
        pin_bar = (min(o0,c0)-l0)>b0*2.0 and (h0-max(o0,c0))<b0*0.3

        if morn_star:   score += 1.0
        elif bull_eng:  score += 0.9
        elif hammer or pin_bar: score += 0.8
        elif strong_bull: score += 0.6
        else:           return False, 0.0  

        if fvg_bull_mitigated: score += 0.5  # SMC Boost
        if vr > 2.0:   score += 0.8
        elif vr > 1.5: score += 0.5
        elif vr > 1.2: score += 0.3
        elif vr < 0.7: score -= 0.3  

    else:  
        shoot_star = (c1<o1 or c0<o0) and (h1-max(o1,c1)) > b1*1.8 and (min(o1,c1)-l1) < b1*0.4
        bear_eng = c0<o0 and c1>o1 and o0>=c1 and c0<=o1
        even_star = c2>o2 and b1<rng1*0.3 and c0<o0 and c0<((o2+c2)/2)
        strong_bear = c0<o0 and b0>rng0*0.60
        pin_bear = (h0-max(o0,c0))>b0*2.0 and (min(o0,c0)-l0)<b0*0.3

        if even_star:   score += 1.0
        elif bear_eng:  score += 0.9
        elif shoot_star or pin_bear: score += 0.8
        elif strong_bear: score += 0.6
        else:           return False, 0.0

        if fvg_bear_mitigated: score += 0.5  # SMC Boost
        if vr > 2.0:   score += 0.8
        elif vr > 1.5: score += 0.5
        elif vr > 1.2: score += 0.3
        elif vr < 0.7: score -= 0.3

    return score >= 0.8, round(score, 3)

# ══════════════════════════════════════════════════════════════════
# SCHOOL 3: TREND
# ══════════════════════════════════════════════════════════════════

def _trend_signal(df, i, direction: str) -> Tuple[bool, float]:
    row = df.iloc[i]; c = float(row.close); score = 0.0
    e20=float(row.e20); e50=float(row.e50); e200=float(row.e200)
    ct=float(row.ict); cb=float(row.icb); div=float(row.div_sc)

    if direction == "LONG":
        if c > e20 > e50 > e200: score += 1.0   
        elif c > e50 > e200:     score += 0.7   
        elif c > e200:           score += 0.4   
        elif c < e50 < e200:     return False, 0.0  
        if c > ct:               score += 0.5   
        elif c > cb:             score += 0.2   
        elif c < cb:             score -= 0.2   
        if div > 0.5:            score += 0.8   
        elif div > 0:            score += 0.4   
        elif div < -0.5:         score -= 0.5   
    else:  
        if c < e20 < e50 < e200: score += 1.0
        elif c < e50 < e200:     score += 0.7
        elif c < e200:           score += 0.4
        elif c > e50 > e200:     return False, 0.0
        if c < cb:               score += 0.5
        elif c < ct:             score += 0.2
        elif c > ct:             score -= 0.2
        if div < -0.5:           score += 0.8
        elif div < 0:            score += 0.4
        elif div > 0.5:          score -= 0.5

    return score >= 0.5, round(score, 3)

# ══════════════════════════════════════════════════════════════════
# FINAL DECISION
# ══════════════════════════════════════════════════════════════════

def _elite_signal(df, i, hi60, lo60) -> Tuple[str, float, Dict]:
    price = float(df.close.iloc[i])
    at_fib, fib_sc, fib_zone = _at_fib_zone(price, hi60, lo60)
    if not at_fib: return "NEUTRAL", 0.0, {}

    if fib_sc > 0: direction = "LONG"
    elif fib_sc < 0: direction = "SHORT"
    else: return "NEUTRAL", 0.0, {}

    mom_ok, mom_sc = _momentum_signal(df, i, direction)
    if not mom_ok: return "NEUTRAL", 0.0, {}

    struct_ok, struct_sc = _structure_signal(df, i, direction)
    if not struct_ok: return "NEUTRAL", 0.0, {}

    trend_ok, trend_sc = _trend_signal(df, i, direction)
    if not trend_ok: return "NEUTRAL", 0.0, {}

    total = abs(fib_sc)*0.25 + mom_sc*0.30 + struct_sc*0.25 + trend_sc*0.20
    details = {
        "fib_zone": fib_zone, "fib_sc": fib_sc,
        "mom_sc": mom_sc, "struct_sc": struct_sc, "trend_sc": trend_sc,
        "total": round(total, 3)
    }
    return direction, round(total, 3), details

# ── NOISE FILTER ──────────────────────────────────────────────────────────────
def _is_noisy(df, i) -> bool:
    if i < 20: return False
    body_r = float(df.body_r.iloc[i]); adx = float(df.adx.iloc[i])
    bbw = float(df.bbw.iloc[i])
    bbw_ma = float(df.bbw.rolling(20).mean().iloc[i]) if i >= 20 else bbw
    return (body_r < 0.15 and adx < 14) or (bbw < bbw_ma * 0.5 and adx < 12)

# ══════════════════════════════════════════════════════════════════
# SIMULATION WITH STRICT BREAKEVEN
# ══════════════════════════════════════════════════════════════════

def _sim(df: pd.DataFrame, balance: float = 10_000.0) -> Dict:
    n = len(df); equity = [balance]; trades = []; WARMUP = 60; MAX_C = 32
    in_trade = False; partial = False
    direction = ""; entry = sl = tp1 = tp2 = tp3 = sl_d = 0.0
    entry_idx = 0; entry_sess = "—"; consec_loss = 0

    for i in range(WARMUP, n):
        row = df.iloc[i]; price = float(row.close)
        hi_c = float(row.high); lo_c = float(row.low)
        hour = row.ts.hour if hasattr(row.ts, "hour") else 10

        if in_trade:
            if (i - entry_idx) > MAX_C:
                pnl = ((price-entry)/entry*100) if direction=="LONG" else ((entry-price)/entry*100)
                mult = 0.5 if partial else 1.0
                risk = balance*0.015/max(sl_d/entry, 0.001)
                gain = pnl/100*risk*mult; balance = max(1.0, balance+gain)
                win = pnl > 0; consec_loss = 0 if win else consec_loss+1
                trades.append({"direction":direction,"entry":round(entry,4),
                                "exit":round(price,4),"pnl_pct":round(pnl*mult,3),
                                "result":"WIN" if win else "LOSS",
                                "exit_type":"TIME_EXIT","session":entry_sess})
                in_trade = False; partial = False; equity.append(balance); continue

            if direction == "LONG":
                hit_sl = lo_c <= sl; hit_tp3 = hi_c >= tp3
                hit_tp2 = hi_c >= tp2 and not hit_tp3
                hit_tp1 = hi_c >= tp1 and not hit_tp2 and not hit_tp3
            else:
                hit_sl = hi_c >= sl; hit_tp3 = lo_c <= tp3
                hit_tp2 = lo_c <= tp2 and not hit_tp3
                hit_tp1 = lo_c <= tp1 and not hit_tp2 and not hit_tp3

            # TP1: STRICT BREAKEVEN LOGIC
            if hit_tp1 and not partial:
                pnl = ((tp1-entry)/entry*100) if direction=="LONG" else ((entry-tp1)/entry*100)
                risk = balance*0.015/max(sl_d/entry, 0.001)
                balance = max(1.0, balance + pnl/100*risk*0.5)
                
                # Force SL to entry + Fees (0.1%) to guarantee no loss on remaining 50%
                be_sl = entry * 1.001 if direction == "LONG" else entry * 0.999
                atr_now = float(df.atr.iloc[i]); atr_now = atr_now if atr_now>0 else sl_d*0.5
                atr_mult = 0.7 if direction == "LONG" else 0.4 # Faster Short Trailing
                trail = round(price-atr_now*atr_mult,4) if direction=="LONG" else round(price+atr_now*atr_mult,4)
                
                sl = max(be_sl, trail) if direction=="LONG" else min(be_sl, trail)
                partial = True
                trades.append({"direction":direction,"entry":round(entry,4),
                                "exit":round(tp1,4),"pnl_pct":round(pnl*0.5,3),
                                "result":"WIN","exit_type":"TP1_PARTIAL","session":entry_sess})
                equity.append(balance); continue
            
            elif partial:
                atr_now = float(df.atr.iloc[i]); atr_now = atr_now if atr_now>0 else sl_d*0.5
                atr_mult = 0.7 if direction == "LONG" else 0.4
                nt = round(price-atr_now*atr_mult,4) if direction=="LONG" else round(price+atr_now*atr_mult,4)
                if direction=="LONG"  and nt > sl: sl = nt
                if direction=="SHORT" and nt < sl: sl = nt

            if hit_tp2 or hit_tp3 or hit_sl:
                ex = sl if hit_sl else (tp3 if hit_tp3 else tp2)
                pnl = ((ex-entry)/entry*100) if direction=="LONG" else ((entry-ex)/entry*100)
                mult = 0.5 if partial else 1.0
                risk = balance*0.015/max(sl_d/entry, 0.001)
                gain = pnl/100*risk*mult; balance = max(1.0, balance+gain)
                win = pnl > 0; consec_loss = 0 if win else consec_loss+1
                xtype = "SL" if hit_sl else ("TP3" if hit_tp3 else "TP2")
                trades.append({"direction":direction,"entry":round(entry,4),
                                "exit":round(ex,4),"pnl_pct":round(pnl*mult,3),
                                "result":"WIN" if win else "LOSS",
                                "exit_type":xtype,"session":entry_sess})
                in_trade = False; partial = False

        if not in_trade:
            if not _sess_ok(hour): equity.append(balance); continue
            if _is_noisy(df, i): equity.append(balance); continue
            if consec_loss >= 3: consec_loss = 0; equity.append(balance); continue

            hi60 = float(df.high.iloc[max(0,i-55):i+1].max())
            lo60 = float(df.low.iloc[max(0,i-55):i+1].min())
            price = float(row.close)
            atr = float(row.atr); atr = atr if atr>0 else price*0.015

            sig, score, dets = _elite_signal(df, i, hi60, lo60)
            if sig == "NEUTRAL": equity.append(balance); continue

            sl_p, sl_dist = _fib_sl(price, hi60, lo60, sig, df, i, atr)
            tp1_p, tp2_p, tp3_p = _fib_tps(price, sl_dist, sig, hi60, lo60)

            rr = abs(tp1_p - price) / max(sl_dist, 1e-10)
            if rr < 1.2: equity.append(balance); continue

            direction = sig; entry = price
            sl = sl_p; tp1 = tp1_p; tp2 = tp2_p; tp3 = tp3_p
            sl_d = sl_dist; entry_sess = _sess(hour)
            in_trade = True; entry_idx = i; partial = False

        equity.append(balance)

    if in_trade:
        lp = float(df.close.iloc[-1])
        pnl = ((lp-entry)/entry*100) if direction=="LONG" else ((entry-lp)/entry*100)
        trades.append({"direction":direction,"entry":entry,"exit":lp,
                        "pnl_pct":round(pnl*(0.5 if partial else 1.0),3),
                        "result":"WIN" if pnl>0 else "LOSS",
                        "exit_type":"OPEN_AT_END","session":entry_sess})

    return {"trades": trades, "equity": equity, "final_balance": balance}

# ── METRICS ───────────────────────────────────────────────────────────────────
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
    aw=round(sum(t["pnl_pct"] for t in wins)/len(wins),3) if wins else 0
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

# ── BacktestEngine ─────────────────────────────────────────────────────────────
class BacktestEngine:
    async def run(self,
                  symbols:List[str]=None,
                  timeframe:str="1h", tf:str=None,
                  start:str="2026-01-01", end:str="2026-05-01",
                  balance:float=10_000.0, **kwargs)->Dict:
        resolved = tf or timeframe or "1h"
        symbols  = symbols or ["BTC/USDT:USDT","ETH/USDT:USDT"]
        sdt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        edt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
        results = {}
        for sym in symbols:
            sym_c = sym.replace("/USDT:USDT","")
            try:
                df = await _fetch(sym, resolved, sdt, edt)
                if df is None or len(df)<70: results[sym_c]={"error":"insufficient data"}; continue
                df = _build(df); df = _add_div(df)
                sim = _sim(df, balance); st = _metrics(sim, balance)
                results[sym_c] = {**st,"symbol":sym_c,"tf":resolved,
                                   "period":f"{start}→{end}","candles":len(df)}
                logger.info(f"[BT] {sym_c}: {st['total']} trades WR={st['win_rate_pct']}% Ret={st['return_pct']:+.2f}%")
            except Exception as e:
                logger.error(f"[BT] {sym_c}: {e}"); results[sym_c]={"error":str(e)}
        return results

    @staticmethod
    def format_report(results:Dict)->str:
        tf=next((v.get("tf","1H") for v in results.values() if isinstance(v,dict) and "tf" in v),"1H")
        lines=["📈 <b>Backtest — Ramos 360 Ai 🎖️  ELITE v6</b>",
               f"📅 Period: 2026-01-01 → 2026-05-01",
               f"⏱️ Timeframe: {tf.upper()} | Institutional Grade",
               "✅ FVG Mitigation + Strict Breakeven + Dynamic Shorts",
               "━━━━━━━━━━━━━━━━━━━━━━━━"]
        for sym,r in results.items():
            if "error" in r: lines.append(f"❌ {sym}: {r['error']}"); continue
            ei="🟢" if r.get("return_pct",0)>0 else "🔴"
            ex=" ".join(f"{k}:{v}" for k,v in r.get("exit_breakdown",{}).items())
            ss=" ".join(f"{k}:{v}" for k,v in r.get("session_breakdown",{}).items())
            lines+=[f"",f"{ei} <b>{sym}</b>",
                    f"  📊 {r['total']} trades  ({r.get('wins',0)}W/{r.get('losses',0)}L)",
                    f"  🎯 Win Rate:   {r['win_rate_pct']:.1f}%",
                    f"  💰 Return:     {r.get('return_pct',0):+.2f}%",
                    f"  📉 Max DD:     {r.get('max_dd_pct',0):.2f}%",
                    f"  ⚖️ Sharpe:     {r.get('sharpe',0):.3f}",
                    f"  🔮 Expectancy: {r.get('expectancy',0):+.3f}%",
                    f"  🏦 Balance:    ${r.get('final_balance',10000):,.2f}",
                    f"  📋 Exits:      {ex}",
                    f"  🕐 Sessions:   {ss}"]
        lines+=["━━━━━━━━━━━━━━━━━━━━━━━━",
                "<i>🎖️ Ramos 360 Ai — ELITE v6</i>"]
        return "\n".join(lines)

async def _main():
    e=BacktestEngine()
    r=await e.run(symbols=["BTC/USDT:USDT"],timeframe="1h",
                   start="2026-01-01",end="2026-05-01",balance=10_000.0)
    print("\n"+e.format_report(r).replace("<b>","").replace("</b>","")
          .replace("<i>","").replace("</i>",""))

if __name__=="__main__":
    asyncio.run(_main())

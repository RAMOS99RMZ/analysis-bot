
"""
engine/layer3_scalp_engine.py — Ramos 360 Ai 🎖️
═══════════════════════════════════════════════════════════════════════════
Layer 3: Independent Scalp Engine — runs every 5 minutes.

⚡ CRITICAL RULE: Layer 3 is FULLY INDEPENDENT.
   It does NOT wait for Layer 1/2 confirmation.
   macro_context from Supabase = confluence bonus only, NOT a gate.

Schools implemented:
  A. ZigZag Peak/Trough Detection
  B. SMC Specialist (BOS, CHoCH, OB, FVG, Liquidity Sweeps)
  C. Harmonic & Classic TA (all 6 harmonic patterns + 5 classic)
  D. Fibonacci Matrix (0.309, 0.4045, 0.618, 0.75, 0.78, 0.809)
  E. Momentum Divergence (all 4 types) + Volume Spread Analysis
  F. Auto-Reversal Circuit (Long ↔ Short)
  G. Trailing Stop + Breakeven at TP1
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import asyncio
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger

try:
    import pandas_ta as ta
    HAS_TA = True
except Exception:
    HAS_TA = False

# ── Fibonacci ratios used across all schools ───────────────────────────────
FIB_RATIOS = [0.309, 0.4045, 0.618, 0.750, 0.780, 0.809]
FIB_EXT    = [1.272, 1.414, 1.618, 2.000, 2.618]
FIB_GOLDEN = 0.618   # Primary golden ratio


# ═══════════════════════════════════════════════════════════════════════════
# SECTION A — ZigZag Peak/Trough Detection
# ═══════════════════════════════════════════════════════════════════════════

def detect_peaks_troughs(df: pd.DataFrame,
                          deviation: float = 0.005,
                          depth: int = 5) -> Dict:
    """
    ZigZag algorithm to find absolute swing highs and lows.

    Args:
        df:        OHLCV DataFrame (oldest first)
        deviation: Min % move to count as a new swing (0.5% default)
        depth:     Min candles between pivots

    Returns dict with:
        peaks:    List[(idx, price)] — swing highs
        troughs:  List[(idx, price)] — swing lows
        last_high: float
        last_low:  float
        trend:     'UP' | 'DOWN' | 'NEUTRAL'
    """
    highs  = df["high"].values
    lows   = df["low"].values
    n      = len(df)

    peaks:   List[Tuple[int, float]] = []
    troughs: List[Tuple[int, float]] = []

    # Find fractal pivots (left=depth, right=depth)
    for i in range(depth, n - depth):
        h = highs[i]
        l = lows[i]
        # Pivot High: highest in window
        if all(h >= highs[i-j] for j in range(1, depth+1)) and \
           all(h >= highs[i+j] for j in range(1, depth+1)):
            peaks.append((i, float(h)))
        # Pivot Low: lowest in window
        if all(l <= lows[i-j] for j in range(1, depth+1)) and \
           all(l <= lows[i+j] for j in range(1, depth+1)):
            troughs.append((i, float(l)))

    # Filter by minimum deviation
    def _filter(pts: List[Tuple[int, float]], is_high: bool) -> List[Tuple[int, float]]:
        if not pts: return pts
        filtered = [pts[0]]
        for idx, price in pts[1:]:
            last_p = filtered[-1][1]
            move   = abs(price - last_p) / (last_p or 1)
            if move >= deviation:
                filtered.append((idx, price))
        return filtered

    peaks   = _filter(peaks,   is_high=True)
    troughs = _filter(troughs, is_high=False)

    last_high = peaks[-1][1]   if peaks   else float(df["high"].max())
    last_low  = troughs[-1][1] if troughs else float(df["low"].min())

    # Determine current trend
    if len(peaks) >= 2 and len(troughs) >= 2:
        hh = peaks[-1][1]   > peaks[-2][1]
        hl = troughs[-1][1] > troughs[-2][1]
        lh = peaks[-1][1]   < peaks[-2][1]
        ll = troughs[-1][1] < troughs[-2][1]
        if hh and hl: trend = "UP"
        elif lh and ll: trend = "DOWN"
        else: trend = "NEUTRAL"
    else:
        trend = "NEUTRAL"

    return {
        "peaks":     peaks,
        "troughs":   troughs,
        "last_high": last_high,
        "last_low":  last_low,
        "trend":     trend,
    }


# ═══════════════════════════════════════════════════════════════════════════
# SECTION B — SMC Specialist
# ═══════════════════════════════════════════════════════════════════════════

def _smc_analysis(df: pd.DataFrame,
                   peaks_troughs: Dict) -> Dict:
    """
    Full SMC analysis:
      - BOS (Break of Structure)
      - CHoCH (Change of Character)
      - Unmitigated Order Blocks
      - Fair Value Gaps (FVG)
      - Equal Highs/Lows Liquidity Sweeps
    """
    result = {
        "bos":       None,   # 'BULL' | 'BEAR'
        "choch":     None,
        "ob_bull":   [],     # Bullish Order Blocks
        "ob_bear":   [],     # Bearish Order Blocks
        "fvg_bull":  [],     # Bullish FVGs
        "fvg_bear":  [],     # Bearish FVGs
        "sweep_hi":  False,
        "sweep_lo":  False,
        "bias":      0.0,    # -1.0 → +1.0
    }

    if df is None or len(df) < 20: return result

    highs   = df["high"].values
    lows    = df["low"].values
    opens   = df["open"].values
    closes  = df["close"].values
    n       = len(df)
    peaks   = peaks_troughs.get("peaks",   [])
    troughs = peaks_troughs.get("troughs", [])

    # ── BOS / CHoCH ───────────────────────────────────────────────────────
    if len(peaks) >= 2 and len(troughs) >= 2:
        last_hi_idx, last_hi = peaks[-1]
        prev_hi_idx, prev_hi = peaks[-2]
        last_lo_idx, last_lo = troughs[-1]
        prev_lo_idx, prev_lo = troughs[-2]
        price = float(closes[-1])

        # Bullish BOS: price breaks above previous swing high
        if price > prev_hi and last_lo > prev_lo:
            result["bos"] = "BULL"
        # Bearish BOS: price breaks below previous swing low
        elif price < prev_lo and last_hi < prev_hi:
            result["bos"] = "BEAR"

        # CHoCH: structure flips after sweep
        if last_lo > prev_lo and last_hi < prev_hi:
            result["choch"] = "BULL_CHOCH"
        elif last_hi < prev_hi and last_lo < prev_lo:
            result["choch"] = "BEAR_CHOCH"

    # ── Order Blocks ──────────────────────────────────────────────────────
    # Bullish OB: last bearish candle before a strong bullish move
    for i in range(1, min(n-1, 30)):
        c0 = closes[-(i+1)]; o0 = opens[-(i+1)]
        c1 = closes[-i];     h1 = highs[-i]
        c2 = closes[max(0,-(i-1))] if i > 1 else closes[-1]
        body = abs(c1 - opens[-i])
        # Bullish OB: bearish candle before impulsive bull move
        if c0 < o0 and c1 > c0 * 1.003 and c2 > c1:
            ob = {"top": float(o0), "bot": float(c0),
                  "mid": float((o0+c0)/2), "idx": n-i-1}
            # Check unmitigated (price hasn't returned to OB)
            future_lo = float(lows[-(i-1)]) if i > 1 else float(lows[-1])
            if future_lo > ob["bot"]:
                result["ob_bull"].append(ob)
        # Bearish OB: bullish candle before impulsive bear move
        if c0 > o0 and c1 < c0 * 0.997 and c2 < c1:
            ob = {"top": float(c0), "bot": float(o0),
                  "mid": float((c0+o0)/2), "idx": n-i-1}
            future_hi = float(highs[-(i-1)]) if i > 1 else float(highs[-1])
            if future_hi < ob["top"]:
                result["ob_bear"].append(ob)

    result["ob_bull"] = result["ob_bull"][:3]
    result["ob_bear"] = result["ob_bear"][:3]

    # ── Fair Value Gaps ───────────────────────────────────────────────────
    for i in range(2, min(n, 40)):
        h_prev = highs[-(i+1)]; l_cur = lows[-(i-1)]
        l_prev = lows[-(i+1)];  h_cur = highs[-(i-1)]
        # Bullish FVG: gap between candle[-3].high and candle[-1].low
        if l_cur > h_prev:
            result["fvg_bull"].append({
                "top": float(l_cur), "bot": float(h_prev),
                "mid": float((l_cur+h_prev)/2), "idx": n-i
            })
        # Bearish FVG: gap between candle[-3].low and candle[-1].high
        if h_cur < l_prev:
            result["fvg_bear"].append({
                "top": float(l_prev), "bot": float(h_cur),
                "mid": float((l_prev+h_cur)/2), "idx": n-i
            })

    result["fvg_bull"] = result["fvg_bull"][:3]
    result["fvg_bear"] = result["fvg_bear"][:3]

    # ── Liquidity Sweeps ──────────────────────────────────────────────────
    # Equal Highs sweep: recent high wick above previous equal highs then closes below
    if len(peaks) >= 3:
        prev_hi_vals = [p[1] for p in peaks[-3:]]
        eq_range     = max(prev_hi_vals) - min(prev_hi_vals)
        if eq_range / (max(prev_hi_vals) or 1) < 0.003:   # within 0.3% = equal highs
            cur_hi = float(highs[-1]); cur_cl = float(closes[-1])
            if cur_hi > max(prev_hi_vals) and cur_cl < max(prev_hi_vals):
                result["sweep_hi"] = True   # swept equal highs → expect reversal down

    if len(troughs) >= 3:
        prev_lo_vals = [p[1] for p in troughs[-3:]]
        eq_range     = max(prev_lo_vals) - min(prev_lo_vals)
        if eq_range / (max(prev_lo_vals) or 1) < 0.003:
            cur_lo = float(lows[-1]); cur_cl = float(closes[-1])
            if cur_lo < min(prev_lo_vals) and cur_cl > min(prev_lo_vals):
                result["sweep_lo"] = True   # swept equal lows → expect reversal up

    # ── SMC Bias Score ────────────────────────────────────────────────────
    bias = 0.0
    if result["bos"] == "BULL":                        bias += 0.6
    elif result["bos"] == "BEAR":                      bias -= 0.6
    if result["choch"] == "BULL_CHOCH":                bias += 0.4
    elif result["choch"] == "BEAR_CHOCH":              bias -= 0.4
    if result["sweep_lo"]:                             bias += 0.5
    if result["sweep_hi"]:                             bias -= 0.5
    if result["ob_bull"]:                              bias += 0.3
    if result["ob_bear"]:                              bias -= 0.3

    result["bias"] = round(max(-1.0, min(1.0, bias)), 3)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# SECTION C — Harmonic & Classic Pattern Detection
# ═══════════════════════════════════════════════════════════════════════════

def _harmonic_ratio(a: float, b: float, c: float, d: float,
                     x: float) -> Dict[str, bool]:
    """
    Check all harmonic pattern ratios given XABCD points.
    Returns dict of pattern → True/False with tolerance 0.03.
    """
    tol = 0.05   # 5% tolerance

    def r(p, q): return abs(p) / (abs(q) or 1e-10)
    def ok(v, target): return abs(v - target) <= tol

    XA = abs(a - x)
    AB = abs(b - a)
    BC = abs(c - b)
    CD = abs(d - c)
    XD = abs(d - x)

    # Ratios
    ab_xa = r(AB, XA)
    bc_ab = r(BC, AB)
    cd_bc = r(CD, BC)
    xd_xa = r(XD, XA)

    patterns: Dict[str, bool] = {}

    # Gartley
    patterns["GARTLEY_BULL"] = (
        ok(ab_xa, 0.618) and bc_ab >= 0.382 and bc_ab <= 0.886 and
        ok(cd_bc, 1.272) and ok(xd_xa, 0.786)
    )
    # Bat
    patterns["BAT_BULL"] = (
        ab_xa >= 0.382 and ab_xa <= 0.50 and
        bc_ab >= 0.382 and bc_ab <= 0.886 and
        ok(cd_bc, 2.0) and ok(xd_xa, 0.886)
    )
    # Butterfly
    patterns["BUTTERFLY_BULL"] = (
        ok(ab_xa, 0.786) and bc_ab >= 0.382 and bc_ab <= 0.886 and
        cd_bc >= 1.618 and cd_bc <= 2.618 and
        xd_xa >= 1.272 and xd_xa <= 1.618
    )
    # Crab
    patterns["CRAB_BULL"] = (
        ab_xa >= 0.382 and ab_xa <= 0.618 and
        bc_ab >= 0.382 and bc_ab <= 0.886 and
        cd_bc >= 2.618 and cd_bc <= 3.618 and
        ok(xd_xa, 1.618)
    )
    # Cypher
    patterns["CYPHER_BULL"] = (
        ab_xa >= 0.382 and ab_xa <= 0.618 and
        bc_ab >= 1.272 and bc_ab <= 1.414 and
        ok(xd_xa, 0.786)
    )
    # Shark
    patterns["SHARK_BULL"] = (
        bc_ab >= 1.13 and bc_ab <= 1.618 and
        xd_xa >= 0.886 and xd_xa <= 1.13
    )

    # Bear patterns (mirror logic)
    for k in list(patterns.keys()):
        patterns[k.replace("BULL","BEAR")] = patterns[k]

    return patterns


def detect_harmonics(peaks: List[Tuple], troughs: List[Tuple],
                      price: float) -> Dict:
    """
    Detect harmonic patterns from last 5 swing points.
    Returns best matching pattern and PRZ (Potential Reversal Zone).
    """
    result = {"pattern": None, "prz_top": None, "prz_bot": None,
              "bull": False, "bear": False, "strength": 0.0}

    # Need at least 4 pivots for XABCD
    all_pivots = sorted(peaks + troughs, key=lambda x: x[0])
    if len(all_pivots) < 5: return result

    # Last 5 pivots as XABCD candidates
    pts = [p[1] for p in all_pivots[-5:]]
    X, A, B, C, D = pts[0], pts[1], pts[2], pts[3], pts[4]

    matched = _harmonic_ratio(A, B, C, D, X)
    bull_patterns = [k for k, v in matched.items() if v and "BULL" in k]
    bear_patterns = [k for k, v in matched.items() if v and "BEAR" in k]

    if bull_patterns:
        result["pattern"] = bull_patterns[0]
        result["bull"]    = True
        result["prz_top"] = round(D * 1.005, 4)
        result["prz_bot"] = round(D * 0.995, 4)
        result["strength"]= 0.8
    elif bear_patterns:
        result["pattern"] = bear_patterns[0]
        result["bear"]    = True
        result["prz_top"] = round(D * 1.005, 4)
        result["prz_bot"] = round(D * 0.995, 4)
        result["strength"]= 0.8

    return result


def detect_classic_patterns(df: pd.DataFrame,
                              peaks: List[Tuple],
                              troughs: List[Tuple]) -> Dict:
    """
    Detect: H&S, Double Top/Bottom, Triangles, Flags/Pennants.
    Returns list of active patterns.
    """
    patterns = []
    if len(peaks) < 3 or len(troughs) < 2 or len(df) < 30:
        return {"patterns": patterns, "bull": False, "bear": False}

    # ── Double Top ─────────────────────────────────────────────────────────
    if len(peaks) >= 2:
        p1, p2 = peaks[-2][1], peaks[-1][1]
        if abs(p1 - p2) / (p1 or 1) < 0.015:   # within 1.5%
            patterns.append("DOUBLE_TOP")

    # ── Double Bottom ──────────────────────────────────────────────────────
    if len(troughs) >= 2:
        t1, t2 = troughs[-2][1], troughs[-1][1]
        if abs(t1 - t2) / (t1 or 1) < 0.015:
            patterns.append("DOUBLE_BOTTOM")

    # ── Head & Shoulders ──────────────────────────────────────────────────
    if len(peaks) >= 3:
        l, h, r = peaks[-3][1], peaks[-2][1], peaks[-1][1]
        if h > l * 1.01 and h > r * 1.01 and abs(l - r) / (l or 1) < 0.02:
            patterns.append("HEAD_SHOULDERS")

    # ── Ascending Triangle (series of higher lows, flat highs) ────────────
    if len(peaks) >= 2 and len(troughs) >= 2:
        hi_flat   = abs(peaks[-1][1]   - peaks[-2][1])   / (peaks[-1][1]   or 1) < 0.01
        lo_rising = troughs[-1][1] > troughs[-2][1] * 1.002
        if hi_flat and lo_rising:
            patterns.append("ASCENDING_TRIANGLE")

    # ── Descending Triangle ────────────────────────────────────────────────
    if len(peaks) >= 2 and len(troughs) >= 2:
        lo_flat    = abs(troughs[-1][1] - troughs[-2][1]) / (troughs[-1][1] or 1) < 0.01
        hi_falling = peaks[-1][1] < peaks[-2][1] * 0.998
        if lo_flat and hi_falling:
            patterns.append("DESCENDING_TRIANGLE")

    # ── Bull/Bear Flag ─────────────────────────────────────────────────────
    if len(df) >= 15:
        pole_move = (float(df.close.iloc[-10]) - float(df.close.iloc[-15])) / \
                     (float(df.close.iloc[-15]) or 1)
        cons_range = (float(df.high.iloc[-10:].max()) - float(df.low.iloc[-10:].min())) / \
                      (float(df.close.iloc[-10]) or 1)
        if abs(pole_move) > 0.02 and cons_range < abs(pole_move) * 0.5:
            patterns.append("BULL_FLAG" if pole_move > 0 else "BEAR_FLAG")

    bull_patterns = [p for p in patterns if p in
                     ("DOUBLE_BOTTOM","ASCENDING_TRIANGLE","BULL_FLAG")]
    bear_patterns = [p for p in patterns if p in
                     ("DOUBLE_TOP","HEAD_SHOULDERS","DESCENDING_TRIANGLE","BEAR_FLAG")]

    return {
        "patterns": patterns,
        "bull":     bool(bull_patterns),
        "bear":     bool(bear_patterns),
        "bull_patterns": bull_patterns,
        "bear_patterns": bear_patterns,
    }


# ═══════════════════════════════════════════════════════════════════════════
# SECTION D — Fibonacci Matrix
# ═══════════════════════════════════════════════════════════════════════════

def build_fibonacci_matrix(swing_hi: float, swing_lo: float,
                             price: float) -> Dict:
    """
    Build complete Fibonacci retracement + extension levels.
    Focus ratios: 0.309, 0.4045, 0.618, 0.75, 0.78, 0.809
    """
    rng    = swing_hi - swing_lo
    if rng <= 0:
        return {"levels": {}, "supports": [], "resistances": [],
                "nearest": price, "zone": None}

    levels: Dict[str, float] = {}

    # Retracement levels (from swing high down to swing low)
    for r in FIB_RATIOS:
        levels[f"ret_{r}"] = round(swing_hi - rng * r, 4)

    # Extension levels (beyond swing high/low)
    for r in FIB_EXT:
        levels[f"ext_up_{r}"]  = round(swing_lo + rng * r, 4)
        levels[f"ext_dn_{r}"]  = round(swing_hi - rng * r, 4)

    # Classify: supports below price, resistances above price
    supports     = sorted(
        [(k, v) for k, v in levels.items() if v < price * 0.9995],
        key=lambda x: x[1], reverse=True)[:4]
    resistances  = sorted(
        [(k, v) for k, v in levels.items() if v > price * 1.0005],
        key=lambda x: x[1])[:4]

    # Nearest level to current price
    all_vals = [(k, v) for k, v in levels.items()]
    nearest  = min(all_vals, key=lambda x: abs(x[1] - price))

    # Determine active zone (is price near a fib level?)
    proximity = abs(nearest[1] - price) / (price or 1)
    zone = nearest[0] if proximity < 0.004 else None

    return {
        "levels":      levels,
        "supports":    supports,
        "resistances": resistances,
        "nearest":     nearest[1],
        "nearest_key": nearest[0],
        "zone":        zone,
        "near_golden": abs(levels.get("ret_0.618", 0) - price) / (price or 1) < 0.005,
    }


# ═══════════════════════════════════════════════════════════════════════════
# SECTION E — Momentum Divergence & Volume Spread Analysis
# ═══════════════════════════════════════════════════════════════════════════

def _calc_rsi(closes: pd.Series, period: int = 14) -> pd.Series:
    if HAS_TA:
        try:
            v = ta.rsi(closes, length=period)
            if v is not None: return v.fillna(50)
        except Exception: pass
    d = closes.diff()
    g = d.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
    return (100 - 100/(1 + g/l.replace(0, 1e-10))).fillna(50)


def _calc_macd(closes: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
    if HAS_TA:
        try:
            v = ta.macd(closes, fast=12, slow=26, signal=9)
            if v is not None and not v.empty:
                return v.iloc[:,0], v.iloc[:,1], v.iloc[:,2]
        except Exception: pass
    e12 = closes.ewm(span=12, adjust=False).mean()
    e26 = closes.ewm(span=26, adjust=False).mean()
    ms  = e12 - e26
    sig = ms.ewm(span=9, adjust=False).mean()
    return ms, sig, ms - sig


def _calc_obv(df: pd.DataFrame) -> pd.Series:
    if HAS_TA:
        try:
            v = ta.obv(df.close, df.volume)
            if v is not None: return v
        except Exception: pass
    sign = df.close.diff().apply(lambda x: 1 if x>0 else -1 if x<0 else 0)
    return (sign * df.volume).cumsum()


def detect_divergence(df: pd.DataFrame,
                       peaks: List[Tuple],
                       troughs: List[Tuple]) -> Dict:
    """
    Detect all 4 divergence types:
      1. Regular Bullish:  Price LL, Indicator HL → BUY signal
      2. Regular Bearish:  Price HH, Indicator LH → SELL signal
      3. Hidden Bullish:   Price HL, Indicator LL → BUY (trend continuation)
      4. Hidden Bearish:   Price LH, Indicator HH → SELL (trend continuation)
    Confirmed by OBV direction + Volume explosion (vol > vol_ma20 * 1.5)
    """
    result = {
        "regular_bullish":  False,
        "regular_bearish":  False,
        "hidden_bullish":   False,
        "hidden_bearish":   False,
        "obv_bullish":      False,
        "volume_explosion": False,
        "confirmed_long":   False,
        "confirmed_short":  False,
        "score":            0.0,
        "signals":          [],
    }

    if df is None or len(df) < 20: return result

    rsi     = _calc_rsi(df.close, 14)
    macd_l, macd_s, macd_h = _calc_macd(df.close)
    obv     = _calc_obv(df)
    vol_ma  = df.volume.rolling(20).mean()

    # ── Volume Explosion ───────────────────────────────────────────────────
    cur_vol  = float(df.volume.iloc[-1])
    avg_vol  = float(vol_ma.iloc[-1]) if not pd.isna(vol_ma.iloc[-1]) else 1
    result["volume_explosion"] = cur_vol > avg_vol * 1.5

    # ── OBV Direction ──────────────────────────────────────────────────────
    if len(obv) >= 10:
        obv_recent = float(obv.iloc[-5:].mean())
        obv_prev   = float(obv.iloc[-10:-5].mean())
        result["obv_bullish"] = obv_recent > obv_prev

    # ── Get indicator values at swing points ───────────────────────────────
    def _get_indicator_at(idx: int, indicator: pd.Series) -> float:
        idx = max(0, min(idx, len(indicator)-1))
        return float(indicator.iloc[idx])

    # ── Regular Bullish (Price LL, RSI HL) ────────────────────────────────
    if len(troughs) >= 2:
        t1_idx, t1_price = troughs[-2]
        t2_idx, t2_price = troughs[-1]
        if t2_price < t1_price:   # Price: Lower Low
            rsi_t1 = _get_indicator_at(t1_idx, rsi)
            rsi_t2 = _get_indicator_at(t2_idx, rsi)
            if rsi_t2 > rsi_t1:   # RSI: Higher Low
                result["regular_bullish"] = True
                result["signals"].append("REG_BULL_DIV(RSI)")
            mac_t1 = _get_indicator_at(t1_idx, macd_h)
            mac_t2 = _get_indicator_at(t2_idx, macd_h)
            if mac_t2 > mac_t1:
                result["regular_bullish"] = True
                result["signals"].append("REG_BULL_DIV(MACD)")

    # ── Regular Bearish (Price HH, RSI LH) ────────────────────────────────
    if len(peaks) >= 2:
        p1_idx, p1_price = peaks[-2]
        p2_idx, p2_price = peaks[-1]
        if p2_price > p1_price:   # Price: Higher High
            rsi_p1 = _get_indicator_at(p1_idx, rsi)
            rsi_p2 = _get_indicator_at(p2_idx, rsi)
            if rsi_p2 < rsi_p1:   # RSI: Lower High
                result["regular_bearish"] = True
                result["signals"].append("REG_BEAR_DIV(RSI)")
            mac_p1 = _get_indicator_at(p1_idx, macd_h)
            mac_p2 = _get_indicator_at(p2_idx, macd_h)
            if mac_p2 < mac_p1:
                result["regular_bearish"] = True
                result["signals"].append("REG_BEAR_DIV(MACD)")

    # ── Hidden Bullish (Price HL, RSI LL) ─────────────────────────────────
    if len(troughs) >= 2:
        t1_idx, t1_price = troughs[-2]
        t2_idx, t2_price = troughs[-1]
        if t2_price > t1_price:   # Price: Higher Low
            rsi_t1 = _get_indicator_at(t1_idx, rsi)
            rsi_t2 = _get_indicator_at(t2_idx, rsi)
            if rsi_t2 < rsi_t1:   # RSI: Lower Low
                result["hidden_bullish"] = True
                result["signals"].append("HID_BULL_DIV(RSI)")

    # ── Hidden Bearish (Price LH, RSI HH) ─────────────────────────────────
    if len(peaks) >= 2:
        p1_idx, p1_price = peaks[-2]
        p2_idx, p2_price = peaks[-1]
        if p2_price < p1_price:   # Price: Lower High
            rsi_p1 = _get_indicator_at(p1_idx, rsi)
            rsi_p2 = _get_indicator_at(p2_idx, rsi)
            if rsi_p2 > rsi_p1:   # RSI: Higher High
                result["hidden_bearish"] = True
                result["signals"].append("HID_BEAR_DIV(RSI)")

    # ── Confirmation: Divergence + OBV + Volume ───────────────────────────
    bull_div = result["regular_bullish"] or result["hidden_bullish"]
    bear_div = result["regular_bearish"] or result["hidden_bearish"]

    result["confirmed_long"]  = (
        bull_div and
        result["obv_bullish"] and
        result["volume_explosion"]
    )
    result["confirmed_short"] = (
        bear_div and
        not result["obv_bullish"] and
        result["volume_explosion"]
    )

    # Score: -1 (strong bear) → +1 (strong bull)
    score = 0.0
    if result["regular_bullish"]:  score += 0.60
    if result["hidden_bullish"]:   score += 0.40
    if result["regular_bearish"]:  score -= 0.60
    if result["hidden_bearish"]:   score -= 0.40
    if result["confirmed_long"]:   score += 0.40
    if result["confirmed_short"]:  score -= 0.40

    result["score"] = round(max(-1.0, min(1.0, score)), 3)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# SECTION F — Auto-Reversal Circuit
# ═══════════════════════════════════════════════════════════════════════════

def _detect_reversal_candle(df: pd.DataFrame, direction: str) -> bool:
    """
    Detect candlestick reversal patterns.
    direction='SHORT' → look for bearish reversals (Shooting Star, Bearish Engulfing)
    direction='LONG'  → look for bullish reversals (Hammer, Bullish Engulfing)
    """
    if len(df) < 3: return False
    r0 = df.iloc[-1]; r1 = df.iloc[-2]
    o0,h0,l0,c0 = float(r0.open),float(r0.high),float(r0.low),float(r0.close)
    o1,h1,l1,c1 = float(r1.open),float(r1.high),float(r1.low),float(r1.close)
    b0 = abs(c0 - o0); rng0 = h0 - l0 or 1e-4
    b1 = abs(c1 - o1); rng1 = h1 - l1 or 1e-4

    if direction == "SHORT":
        # Shooting Star
        if c0 < o0 and (h0-max(o0,c0)) > b0*2 and (min(o0,c0)-l0) < b0*0.3:
            return True
        # Bearish Engulfing
        if c0 < o0 and c1 > o1 and o0 > c1 and c0 < o1:
            return True
        # Evening Star (3 candles)
        if len(df) >= 3:
            r2 = df.iloc[-3]
            if float(r2.close) > float(r2.open) and b1 < b0 * 0.4 and c0 < o0:
                return True

    if direction == "LONG":
        # Hammer
        if c0 > o0 and (min(o0,c0)-l0) > b0*2 and (h0-max(o0,c0)) < b0*0.3:
            return True
        # Bullish Engulfing
        if c0 > o0 and c1 < o1 and o0 < c1 and c0 > o1:
            return True
        # Morning Star
        if len(df) >= 3:
            r2 = df.iloc[-3]
            if float(r2.close) < float(r2.open) and b1 < b0 * 0.4 and c0 > o0:
                return True

    return False


def check_reversal_conditions(
    df: pd.DataFrame,
    smc: Dict,
    fib: Dict,
    divergence: Dict,
    harmonic: Dict,
    current_direction: Optional[str],
) -> Dict:
    """
    Auto-Reversal Circuit:
    SHORT: price at resistance + bear divergence + bear candle + volume
    LONG:  price at support   + bull divergence + bull candle + volume

    Returns: { should_reverse: bool, new_direction: str, reason: str, strength: float }
    """
    result = {"should_reverse": False, "new_direction": None,
              "reason": "", "strength": 0.0}

    price   = float(df.close.iloc[-1])
    vol_exp = divergence.get("volume_explosion", False)

    # ── SHORT Reversal Circuit ────────────────────────────────────────────
    bull_candle  = _detect_reversal_candle(df, "SHORT")
    at_resistance = (
        bool(smc.get("ob_bear")) or
        fib.get("near_golden") and price > fib.get("nearest", 0) or
        bool(harmonic.get("bear")) or
        smc.get("sweep_hi", False)
    )
    short_strength = 0.0
    if at_resistance:   short_strength += 0.4
    if divergence.get("confirmed_short"): short_strength += 0.4
    if bull_candle:     short_strength += 0.2   # bearish candle at resistance
    if vol_exp:         short_strength += 0.1

    # ── LONG Reversal Circuit ─────────────────────────────────────────────
    bear_candle  = _detect_reversal_candle(df, "LONG")
    at_support    = (
        bool(smc.get("ob_bull")) or
        fib.get("near_golden") and price < fib.get("nearest", 0) or
        bool(harmonic.get("bull")) or
        smc.get("sweep_lo", False)
    )
    long_strength = 0.0
    if at_support:      long_strength += 0.4
    if divergence.get("confirmed_long"): long_strength += 0.4
    if bear_candle:     long_strength += 0.2
    if vol_exp:         long_strength += 0.1

    MIN_STRENGTH = 0.6

    if short_strength >= MIN_STRENGTH and short_strength > long_strength:
        result["should_reverse"] = True
        result["new_direction"]  = "SHORT"
        result["strength"]       = round(short_strength, 3)
        result["reason"]         = (
            f"SHORT reversal: at_resistance={at_resistance} "
            f"conf_div={divergence.get('confirmed_short')} "
            f"bear_candle={bull_candle} vol={vol_exp}"
        )
    elif long_strength >= MIN_STRENGTH and long_strength > short_strength:
        result["should_reverse"] = True
        result["new_direction"]  = "LONG"
        result["strength"]       = round(long_strength, 3)
        result["reason"]         = (
            f"LONG reversal: at_support={at_support} "
            f"conf_div={divergence.get('confirmed_long')} "
            f"bull_candle={bear_candle} vol={vol_exp}"
        )

    return result


# ═══════════════════════════════════════════════════════════════════════════
# SECTION G — Signal Builder + Trailing Stop
# ═══════════════════════════════════════════════════════════════════════════

def _calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    if HAS_TA:
        try:
            v = ta.atr(df.high, df.low, df.close, length=period)
            if v is not None and not pd.isna(v.iloc[-1]):
                return float(v.iloc[-1])
        except Exception: pass
    hl  = df.high - df.low
    hpc = (df.high - df.close.shift()).abs()
    lpc = (df.low  - df.close.shift()).abs()
    tr  = pd.concat([hl, hpc, lpc], axis=1).max(axis=1)
    v   = tr.ewm(alpha=1/period, adjust=False).mean()
    return float(v.iloc[-1]) if not pd.isna(v.iloc[-1]) else df.close.iloc[-1]*0.01


def build_scalp_signal(
    symbol:    str,
    df:        pd.DataFrame,
    direction: str,
    smc:       Dict,
    fib:       Dict,
    divergence: Dict,
    harmonic:   Dict,
    classic:    Dict,
    macro_ctx:  Optional[Dict],
    run_id:     str = "L3",
) -> Optional[Dict]:
    """
    Build a complete scalp signal with:
    - ATR-based dynamic SL
    - Fibonacci TP levels (0.309, 0.618, 0.809)
    - Trailing stop logic (breakeven after TP1)
    - Macro context as confluence bonus (NOT a gate)
    - Min RR ≥ 1.5
    """
    if direction not in ("LONG", "SHORT"): return None

    price = float(df.close.iloc[-1])
    atr   = _calc_atr(df, 14)

    # SL: ATR × 1.2
    sl = round(price - atr * 1.2, 4) if direction == "LONG" \
         else round(price + atr * 1.2, 4)

    # TP from Fibonacci ratios projected from ATR range
    last_hi = fib.get("nearest", price + atr * 3)
    last_lo = fib.get("nearest", price - atr * 3)

    # Use FVG or OB for TP targets when available
    if direction == "LONG":
        fvg_targets = [f["top"] for f in smc.get("fvg_bull", [])
                       if f["top"] > price][:3]
        ob_targets  = [o["top"] for o in smc.get("ob_bear", [])
                       if o["top"] > price][:2]
        raw_tp      = sorted(set(fvg_targets + ob_targets))
        if len(raw_tp) >= 3:
            tp1, tp2, tp3 = raw_tp[0], raw_tp[1], raw_tp[2]
        else:
            rng = atr * 6
            tp1 = round(price + rng * 0.309, 4)
            tp2 = round(price + rng * 0.618, 4)
            tp3 = round(price + rng * 0.809, 4)
    else:
        fvg_targets = [f["bot"] for f in smc.get("fvg_bear", [])
                       if f["bot"] < price][:3]
        ob_targets  = [o["bot"] for o in smc.get("ob_bull", [])
                       if o["bot"] < price][:2]
        raw_tp      = sorted(set(fvg_targets + ob_targets), reverse=True)
        if len(raw_tp) >= 3:
            tp1, tp2, tp3 = raw_tp[0], raw_tp[1], raw_tp[2]
        else:
            rng = atr * 6
            tp1 = round(price - rng * 0.309, 4)
            tp2 = round(price - rng * 0.618, 4)
            tp3 = round(price - rng * 0.809, 4)

    # Validate RR ≥ 1.5
    rr = abs(tp1 - price) / max(abs(sl - price), 1e-10)
    if rr < 1.5:
        logger.debug(f"[L3] {symbol} {direction} RR={rr:.2f} < 1.5 — rejected")
        return None

    # Score calculation
    score = 0.0
    if direction == "LONG":
        score += divergence.get("score", 0) * 0.30
        score += smc.get("bias", 0) * 0.25
        if harmonic.get("bull"):   score += 0.20
        if classic.get("bull"):    score += 0.15
        if fib.get("near_golden"): score += 0.10
    else:
        score -= divergence.get("score", 0) * 0.30
        score -= smc.get("bias", 0) * 0.25
        if harmonic.get("bear"):   score += 0.20
        if classic.get("bear"):    score += 0.15
        if fib.get("near_golden"): score += 0.10

    # ── Macro Context as Confluence Bonus (NOT a gate) ─────────────────────
    macro_bonus = 0.0
    if macro_ctx:
        macro_bias = macro_ctx.get("macro_bias", "NEUTRAL")
        if direction == "LONG"  and macro_bias == "BULL": macro_bonus = 0.15
        if direction == "SHORT" and macro_bias == "BEAR": macro_bonus = 0.15
        if macro_bonus:
            score += macro_bonus
            logger.debug(f"[L3] Macro confluence bonus +{macro_bonus}")

    score = round(max(0.0, min(1.0, abs(score))), 4)

    # Build experts fired list
    experts_fired = []
    if smc.get("bos"):           experts_fired.append("SMC_BOS")
    if smc.get("ob_bull") or smc.get("ob_bear"): experts_fired.append("SMC_OB")
    if smc.get("sweep_lo") or smc.get("sweep_hi"): experts_fired.append("SMC_SWEEP")
    if divergence.get("regular_bullish") or divergence.get("regular_bearish"):
        experts_fired.append("DIV_REGULAR")
    if divergence.get("hidden_bullish") or divergence.get("hidden_bearish"):
        experts_fired.append("DIV_HIDDEN")
    if harmonic.get("pattern"):  experts_fired.append(f"HARM_{harmonic['pattern']}")
    if classic.get("patterns"):  experts_fired.extend(classic["patterns"][:2])
    if fib.get("zone"):          experts_fired.append(f"FIB_{fib['zone']}")

    return {
        "symbol":        symbol,
        "direction":     direction,
        "trade_type":    "Scalp",
        "entry":         price,
        "sl":            sl,
        "tp1":           tp1,
        "tp2":           tp2,
        "tp3":           tp3,
        "atr":           round(atr, 6),
        "rr":            round(rr, 2),
        "score":         score,
        "expert_votes":  len(experts_fired),
        "experts_fired": experts_fired,
        "regime":        "L3_SCALP",
        "timeframe":     "15m",
        "run_id":        run_id,
        "trailing_stop": {
            "enabled":           True,
            "breakeven_at_tp1":  True,   # move SL to entry when TP1 hit
            "trail_atr_mult":    1.0,     # trail by 1 × ATR beyond TP2
        },
        "macro_confluence": macro_bonus > 0,
    }


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENGINE — run_layer3()
# ═══════════════════════════════════════════════════════════════════════════

async def run_layer3(
    symbol:      str,
    df_1h:       pd.DataFrame,
    df_15m:      pd.DataFrame,
    df_5m:       pd.DataFrame,
    open_trades: List[Dict],
    run_id:      str = "L3",
    macro_ctx:   Optional[Dict] = None,
) -> List[Dict]:
    """
    Layer 3 Scalp Engine — FULLY INDEPENDENT.

    ⚡ Layer 3 does NOT require Layer 1/2 confirmation.
       macro_ctx used as confluence bonus only (not a gate).

    Returns list of signal dicts ready to send.
    """
    signals  = []

    # Use 15m as primary, 5m for entry timing
    df = df_15m if df_15m is not None and len(df_15m) >= 60 else df_1h
    if df is None or len(df) < 30:
        logger.warning(f"[L3] {symbol}: insufficient data"); return signals

    # ── A. Peak / Trough Detection (ZigZag) ───────────────────────────────
    pt_15m = detect_peaks_troughs(df,    deviation=0.004, depth=4)
    pt_5m  = detect_peaks_troughs(df_5m, deviation=0.002, depth=3) \
             if df_5m is not None and len(df_5m) >= 20 \
             else pt_15m

    swing_hi = pt_15m["last_high"]
    swing_lo = pt_15m["last_low"]
    trend    = pt_15m["trend"]

    # ── B. SMC Analysis ────────────────────────────────────────────────────
    smc = _smc_analysis(df, pt_15m)

    # ── C. Harmonic + Classic ─────────────────────────────────────────────
    harmonic = detect_harmonics(pt_15m["peaks"], pt_15m["troughs"],
                                  float(df.close.iloc[-1]))
    classic  = detect_classic_patterns(df, pt_15m["peaks"], pt_15m["troughs"])

    # ── D. Fibonacci Matrix ────────────────────────────────────────────────
    price = float(df.close.iloc[-1])
    fib   = build_fibonacci_matrix(swing_hi, swing_lo, price)

    # ── E. Divergence + Volume ─────────────────────────────────────────────
    divergence = detect_divergence(df, pt_15m["peaks"], pt_15m["troughs"])

    # ── Find current open trade for this symbol ────────────────────────────
    current_trade = next(
        (t for t in open_trades
         if t.get("symbol") == symbol and t.get("status") == "OPEN"),
        None
    )
    current_dir   = current_trade.get("direction") if current_trade else None

    # ── F. Auto-Reversal Circuit ───────────────────────────────────────────
    reversal = check_reversal_conditions(
        df, smc, fib, divergence, harmonic, current_dir
    )

    if reversal["should_reverse"]:
        new_dir = reversal["new_direction"]
        # Close existing trade signal (handled by monitor job)
        logger.info(
            f"[L3] {symbol} REVERSAL → {new_dir} | {reversal['reason']}"
        )
        sig = build_scalp_signal(
            symbol, df, new_dir, smc, fib, divergence,
            harmonic, classic, macro_ctx, run_id
        )
        if sig:
            sig["is_reversal"]   = True
            sig["close_existing"] = True
            sig["reversal_strength"] = reversal["strength"]
            signals.append(sig)
        return signals

    # ── G. Normal Entry Logic ──────────────────────────────────────────────
    # Determine direction from combined signals
    # Layer 3 uses LOCAL trend from 15m/5m — NOT dependent on Layer 1
    long_score  = 0.0
    short_score = 0.0

    # SMC
    if smc["bias"] > 0:  long_score  += smc["bias"]
    elif smc["bias"] < 0: short_score += abs(smc["bias"])

    # Divergence
    if divergence["confirmed_long"]:   long_score  += 0.60
    if divergence["confirmed_short"]:  short_score += 0.60
    if divergence.get("regular_bullish") or divergence.get("hidden_bullish"):
        long_score  += 0.30
    if divergence.get("regular_bearish") or divergence.get("hidden_bearish"):
        short_score += 0.30

    # Harmonic
    if harmonic.get("bull"): long_score  += 0.40
    if harmonic.get("bear"): short_score += 0.40

    # Classic
    if classic.get("bull"):  long_score  += 0.25
    if classic.get("bear"):  short_score += 0.25

    # Local trend bias
    if trend == "UP":    long_score  += 0.30
    elif trend == "DOWN":short_score += 0.30

    # Fibonacci support/resistance
    if fib.get("near_golden"):
        if price < swing_hi * 0.99: long_score  += 0.20
        else:                        short_score += 0.20

    MIN_ENTRY_SCORE = 0.55   # relaxed threshold for maximum trade frequency

    direction = None
    if long_score  >= MIN_ENTRY_SCORE and long_score  > short_score:
        direction = "LONG"
    elif short_score >= MIN_ENTRY_SCORE and short_score > long_score:
        direction = "SHORT"

    if direction:
        sig = build_scalp_signal(
            symbol, df, direction, smc, fib, divergence,
            harmonic, classic, macro_ctx, run_id
        )
        if sig:
            sig["long_score"]   = round(long_score, 3)
            sig["short_score"]  = round(short_score, 3)
            sig["local_trend"]  = trend
            signals.append(sig)
            logger.info(
                f"[L3] {symbol} {direction} | "
                f"score={sig['score']:.3f} rr={sig['rr']:.2f} "
                f"experts={sig['expert_votes']} "
                f"macro_confluence={sig['macro_confluence']}"
            )

    return signals

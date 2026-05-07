# strategies/expert_02_harmonic.py — E2: Harmonic Patterns
# Gartley, Bat, Alt Bat, Butterfly, Crab, Shark, Cypher, 5-0 + Fibonacci levels

from __future__ import annotations
from typing import Dict, List, Optional, Tuple, Union
import numpy as np
from loguru import logger

# ── Fibonacci tolerances ──────────────────────────────────────────────────────
def _in_range(val: float, target, tol: float = 0.03) -> bool:
    if isinstance(target, (list, tuple)) and len(target) == 2:
        return target[0] * (1 - tol) <= val <= target[1] * (1 + tol)
    return abs(val - target) / (target + 1e-12) <= tol

def _get_pivots(candles: List, order: int = 2) -> Dict:
    """Detect swing highs and lows."""
    highs, lows = [], []
    for i in range(order, len(candles) - order):
        h = float(candles[i][2])
        l = float(candles[i][3])
        if all(h >= float(candles[i - j][2]) for j in range(1, order + 1)) and \
           all(h >= float(candles[i + j][2]) for j in range(1, order + 1)):
            highs.append(h)
        if all(l <= float(candles[i - j][3]) for j in range(1, order + 1)) and \
           all(l <= float(candles[i + j][3]) for j in range(1, order + 1)):
            lows.append(l)
    return {"highs": highs, "lows": lows}

def _check_xabcd(X: float, A: float, B: float, C: float,
                 price: float, is_bull: bool) -> Optional[Dict]:
    """Core XABCD pattern check. Mirrors checkXABCD() in GAS."""
    try:
        xa = abs(A - X)
        if xa == 0: return None
        ab_xa = abs(B - A) / xa
        bc_ab = abs(C - B) / abs(A - B) if abs(A - B) > 0 else 0
        ad_xa = abs(price - A) / xa

        pattern, score = None, 0.0

        if _in_range(ab_xa, 0.618, 0.03) and _in_range(bc_ab, [0.382, 0.886]) and _in_range(ad_xa, 0.786, 0.03):
            pattern, score = "Gartley", 0.55
        elif _in_range(ab_xa, 0.382, 0.03) and _in_range(bc_ab, [0.382, 0.886]) and _in_range(ad_xa, 0.886, 0.03):
            pattern, score = "Bat", 0.55
        elif _in_range(ab_xa, 0.382, 0.03) and _in_range(bc_ab, [0.382, 0.886]) and _in_range(ad_xa, 1.13, 0.04):
            pattern, score = "Alt Bat", 0.50
        elif _in_range(ab_xa, 0.786, 0.035) and _in_range(bc_ab, [0.382, 0.886]) and _in_range(ad_xa, [1.272, 1.618]):
            pattern, score = "Butterfly", 0.48
        elif _in_range(ab_xa, [0.382, 0.618]) and _in_range(bc_ab, [0.382, 0.886]) and _in_range(ad_xa, 1.618, 0.045):
            pattern, score = "Crab", 0.48
        elif _in_range(ab_xa, 0.886, 0.035) and _in_range(ad_xa, 1.618, 0.045):
            pattern, score = "Deep Crab", 0.45
        elif _in_range(ab_xa, [0.382, 0.618]) and _in_range(bc_ab, [1.13, 1.618]) and _in_range(ad_xa, [0.886, 1.13]):
            pattern, score = "Shark", 0.45
        elif _in_range(ab_xa, [0.382, 0.618]) and _in_range(bc_ab, [1.272, 1.414]) and _in_range(ad_xa, 0.786, 0.035):
            pattern, score = "Cypher", 0.48
        elif _in_range(ab_xa, [1.13, 1.618]) and _in_range(bc_ab, [1.618, 2.24]) and _in_range(ad_xa, 0.50, 0.035):
            pattern, score = "5-0", 0.52

        if not pattern: return None

        prz_range = abs(price - A) * 0.1
        in_prz = is_bull and (price >= A - prz_range) or (not is_bull and price <= A + prz_range)
        cd = abs(price - C)
        tp1 = (price + cd * 0.382) if is_bull else (price - cd * 0.382)
        tp2 = (price + cd * 0.618) if is_bull else (price - cd * 0.618)
        sl_lv = X * 0.995 if is_bull else X * 1.005

        risk = abs(price - sl_lv)
        rr   = abs(tp2 - price) / risk if risk > 0 else 0

        return {
            "name": pattern, "score": score, "is_bull": is_bull,
            "in_prz": in_prz, "sl": round(sl_lv, 6),
            "tp1": round(tp1, 6), "tp2": round(tp2, 6), "rr": round(rr, 2),
        }
    except Exception:
        return None

def _check_fibs(candles: List, price: float, weight: float) -> Tuple[float, float, str]:
    """Fibonacci retracement level proximity check."""
    if not candles or len(candles) < 20:
        return 0.0, 0.0, ""
    hi = max(float(c[2]) for c in candles[1:40] if c)
    lo = min(float(c[3]) for c in candles[1:40] if c)
    rng = hi - lo
    if rng <= 0: return 0.0, 0.0, ""
    tol = rng * 0.005
    FIBS = [0.236, 0.382, 0.500, 0.618, 0.786, 0.886, 1.13, 1.272, 1.618, 2.618]
    for r in FIBS:
        lvl = hi - rng * r
        if abs(price - lvl) < tol:
            w = weight * (0.28 if r in (0.618, 0.786) else 0.15)
            label = f"Fib {r*100:.1f}%"
            if price < hi - rng * 0.3: return w, 0.0, label   # bullish
            else:                       return 0.0, w, label   # bearish
    return 0.0, 0.0, ""

def analyze(data: Dict) -> Optional[Dict]:
    try:
        c5m  = data.get("c5m",  [])
        c15m = data.get("c15m", [])
        c30m = data.get("c30m", [])
        c1h  = data.get("c1h",  [])
        c4h  = data.get("c4h",  [])
        if not c5m or len(c5m) < 25: return None

        price = float(c5m[0][4])
        long_s = short_s = 0.0
        why: Dict = {}

        def _run_on(candles, label, weight):
            nonlocal long_s, short_s
            if not candles or len(candles) < 25: return
            pv = _get_pivots(candles, 2)
            hs, ls = pv["highs"], pv["lows"]
            if len(hs) < 3 or len(ls) < 3: return

            # Bull pattern: X=low, A=high, B=low, C=high, D=low(PRZ)
            if len(ls) >= 2 and len(hs) >= 2:
                X, A, B, C = ls[-1], hs[-1], ls[-2] if len(ls) >= 2 else 0, hs[-2] if len(hs) >= 2 else 0
                if X > 0 and A > X and B > 0 and C > B:
                    res = _check_xabcd(X, A, B, C, price, True)
                    if res:
                        boost = 1.3 if res["in_prz"] else 1.0
                        rr_b  = 1.2 if res["rr"] >= 2.5 else 1.0
                        v = res["score"] * weight * boost * rr_b
                        if res["rr"] < 1.5: v *= 0.7
                        long_s += v
                        why["harmonic"] = f"{res['name']} BULL ({label}) RR={res['rr']:.1f}"

            # Bear pattern
            if len(hs) >= 2 and len(ls) >= 2:
                X2, A2, B2, C2 = hs[-1], ls[-1], hs[-2] if len(hs) >= 2 else 0, ls[-2] if len(ls) >= 2 else 0
                if X2 > 0 and A2 < X2 and B2 > A2 and C2 < B2:
                    res2 = _check_xabcd(X2, A2, B2, C2, price, False)
                    if res2:
                        boost = 1.3 if res2["in_prz"] else 1.0
                        rr_b  = 1.2 if res2["rr"] >= 2.5 else 1.0
                        v = res2["score"] * weight * boost * rr_b
                        if res2["rr"] < 1.5: v *= 0.7
                        short_s += v
                        why["harmonic_bear"] = f"{res2['name']} BEAR ({label})"

            # Fibonacci levels
            fl, fs, flabel = _check_fibs(candles, price, weight)
            long_s  += fl; short_s += fs
            if flabel: why["fib"] = flabel

        _run_on(c5m,  "5m",  1.0)
        _run_on(c15m, "15m", 0.8)
        _run_on(c30m, "30m", 0.9)
        _run_on(c1h,  "1H",  1.2)
        _run_on(c4h,  "4H",  1.5)

        return {
            "name": "Harmonic",
            "long": round(min(long_s, 1.0), 4),
            "short": round(min(short_s, 1.0), 4),
            "why": why,
        }
    except Exception as e:
        return {"name": "Harmonic", "long": 0, "short": 0, "why": {"err": str(e)}}

# utils/helpers.py — Shared utility functions
# Mirrors: safeParseJson, closesAsc, calcVolumeProfile, cooldown logic from GAS

from __future__ import annotations
import json, time, asyncio
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import numpy as np
from loguru import logger

# ── safe JSON parse ───────────────────────────────────────────────────────────
def safe_json(s: str) -> Optional[Any]:
    try: return json.loads(s) if s else None
    except: return None

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

def utc_hour() -> int:
    return datetime.now(timezone.utc).hour

def utc_weekday() -> int:          # 0=Mon … 6=Sun
    return datetime.now(timezone.utc).weekday()

# ── Candle helpers (OKX format: [ts, o, h, l, c, vol, ...]) ──────────────────
def closes_asc(candles: List) -> np.ndarray:
    """Convert OKX candles (newest-first) to ascending close array."""
    if not candles: return np.array([])
    arr = np.array([float(c[4]) for c in reversed(candles)])
    return arr

def highs(candles: List) -> np.ndarray:
    return np.array([float(c[2]) for c in candles])

def lows(candles: List) -> np.ndarray:
    return np.array([float(c[3]) for c in candles])

def volumes(candles: List) -> np.ndarray:
    return np.array([float(c[5] or 0) for c in candles])

def latest_close(candles: List) -> float:
    return float(candles[0][4]) if candles else 0.0

# ── Volume Profile (mirrors calcVolumeProfile in GAS) ────────────────────────
def calc_volume_profile(candles: List, bins: int = 10) -> Dict:
    try:
        if len(candles) < 10:
            return {"poc": 0, "vah": 0, "val": 0}
        hs = [float(c[2]) for c in candles]
        ls = [float(c[3]) for c in candles]
        vs = [float(c[5] or 0) for c in candles]
        r_hi, r_lo = max(hs), min(ls)
        if r_hi <= r_lo: return {"poc": r_lo, "vah": r_hi, "val": r_lo}
        step = (r_hi - r_lo) / bins
        profile = []
        for b in range(bins):
            lo2, hi2 = r_lo + b * step, r_lo + (b + 1) * step
            vol = sum(vs[i] for i, c in enumerate(candles)
                      if lo2 <= (float(c[2]) + float(c[3])) / 2 < hi2)
            profile.append({"lo": lo2, "hi": hi2, "mid": (lo2 + hi2) / 2, "vol": vol})
        profile.sort(key=lambda x: x["vol"], reverse=True)
        poc = profile[0]["mid"] if profile else r_lo
        tot_v = sum(p["vol"] for p in profile) * 0.70
        cum_v, vah, val = 0, r_hi, r_lo
        for p in profile:
            if cum_v < tot_v:
                cum_v += p["vol"]
                vah = max(vah, p["hi"]); val = min(val, p["lo"])
        return {"poc": poc, "vah": vah, "val": val}
    except Exception as e:
        logger.warning(f"[VP] {e}")
        return {"poc": 0, "vah": 0, "val": 0}

# ── In-memory cooldown store (replaces Script Properties) ────────────────────
_COOLDOWNS: Dict[str, float] = {}

def was_recently_sent(symbol: str, direction: str, is_scalp: bool,
                      scalp_cd_sec: int = 120, swing_cd_sec: int = 600) -> bool:
    key = f"{symbol}_{direction}_{'SC' if is_scalp else 'SW'}"
    t = _COOLDOWNS.get(key, 0)
    is_major = "BTC" in symbol or "ETH" in symbol
    cd = (scalp_cd_sec if is_scalp else swing_cd_sec) * (1 if is_major else 0.5)
    return (time.time() - t) < cd

def mark_sent(symbol: str, direction: str, is_scalp: bool) -> None:
    key = f"{symbol}_{direction}_{'SC' if is_scalp else 'SW'}"
    _COOLDOWNS[key] = time.time()

# ── SL Hit memory (mirrors checkReEntry / recordSLHit in GAS) ────────────────
_SL_HITS: Dict[str, List[float]] = {}
_SL_BLOCKS: Dict[str, float] = {}

def record_sl_hit(symbol: str, direction: str) -> None:
    key = f"{symbol}_{direction}"
    hits = _SL_HITS.setdefault(key, [])
    hits.insert(0, time.time())
    _SL_HITS[key] = hits[:2]

def check_re_entry(symbol: str, direction: str) -> Dict:
    key = f"{symbol}_{direction}"
    now = time.time()
    block_t = _SL_BLOCKS.get(symbol, 0)
    if block_t and now - block_t < 7200: return {"blocked": True, "reason": "SL Trap"}
    if block_t: _SL_BLOCKS.pop(symbol, None)
    hits = _SL_HITS.get(key, [])
    if not hits: return {"blocked": False, "re_entry": False}
    if len(hits) >= 2 and now - hits[1] < 3600:
        _SL_BLOCKS[symbol] = now
        return {"blocked": True, "reason": "Double SL"}
    if now - hits[0] < 1800:
        return {"blocked": False, "re_entry": True, "size_mult": 0.5}
    return {"blocked": False, "re_entry": False}

# ── API Circuit Breaker ───────────────────────────────────────────────────────
_api_fails = 0

def record_api_fail() -> None:
    global _api_fails; _api_fails += 1

def clear_api_fails() -> None:
    global _api_fails; _api_fails = 0

def is_circuit_open() -> bool:
    return _api_fails >= 8

# ── Run ID generator ──────────────────────────────────────────────────────────
def make_run_id(prefix: str = "RUN") -> str:
    return f"{prefix}_{int(time.time())}"

# ── Session detector (mirrors getSession() in GAS) ───────────────────────────
def get_session() -> Dict:
    h = utc_hour()
    if 13 <= h <= 17: return {"name": "NY+London",    "vol": "VERY_HIGH", "long_bias": 0.20, "short_bias": 0.18, "overlap": True}
    if 17 <= h < 22:  return {"name": "New York",     "vol": "HIGH",      "long_bias": 0.16, "short_bias": 0.14, "overlap": False}
    if 22 <= h < 24:  return {"name": "NY Close",     "vol": "MEDIUM",    "long_bias": 0.07, "short_bias": 0.09, "overlap": False}
    if 8  <= h < 13:  return {"name": "London",       "vol": "HIGH",      "long_bias": 0.12, "short_bias": 0.15, "overlap": False}
    if 6  <= h < 8:   return {"name": "Frankfurt",    "vol": "MEDIUM",    "long_bias": 0.08, "short_bias": 0.10, "overlap": False}
    if 0  <= h < 2:   return {"name": "Dead Zone",    "vol": "VERY_LOW",  "long_bias": 0.02, "short_bias": 0.02, "overlap": False}
    if 2  <= h < 6:   return {"name": "Asia Late",    "vol": "LOW",       "long_bias": 0.05, "short_bias": 0.05, "overlap": False}
    return                    {"name": "Tokyo/Asia",  "vol": "LOW",       "long_bias": 0.06, "short_bias": 0.06, "overlap": False}

def is_opportunity_window() -> bool:
    h = utc_hour()
    return (8 <= h <= 10) or (13 <= h <= 15) or (h == 0)

# ── Hurst exponent ────────────────────────────────────────────────────────────
def calc_hurst(prices: np.ndarray) -> float:
    if len(prices) < 20: return 0.5
    try:
        n = len(prices)
        mean = prices.mean()
        cum_dev = np.cumsum(prices - mean)
        R = cum_dev.max() - cum_dev.min()
        S = prices.std()
        return float(np.log(R / S) / np.log(n / 2)) if S > 0 else 0.5
    except: return 0.5

"""
engine/analysis_engine.py — Ramos 360 Ai 🎖️
E10: BTC + ETH deep analysis (1D/4H/1H) - real-time prices
E11: USDT.D analysis with crypto market signal
Libraries: pandas, pandas-ta, numpy, scipy, httpx
"""
from __future__ import annotations
import asyncio, math, time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import httpx
import numpy as np
import pandas as pd

try:
    import pandas_ta as ta
    HAS_TA = True
except Exception:
    HAS_TA = False

from loguru import logger

_OKX = "https://www.okx.com/api/v5"
_HDR = {"Accept": "application/json", "User-Agent": "Ramos360Ai/1.0"}
_TF  = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m",
         "1h":"1H","4h":"4H","1d":"1D","1w":"1W"}


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _inst(symbol: str) -> str:
    return symbol.split(":")[0].replace("/", "-") + "-SWAP"


async def _fetch_candles(symbol: str, tf: str, limit: int = 200) -> Optional[pd.DataFrame]:
    """Fetch OKX public candles → DataFrame (oldest first)."""
    try:
        async with httpx.AsyncClient(timeout=15, headers=_HDR) as cl:
            r = await cl.get(f"{_OKX}/market/candles", params={
                "instId": _inst(symbol),
                "bar":    _TF.get(tf, tf),
                "limit":  str(min(limit, 300)),
            })
            if r.status_code != 200:
                logger.warning(f"[Candles] {symbol} {tf}: HTTP {r.status_code}")
                return None
            j = r.json()
            if j.get("code") != "0" or not j.get("data"):
                logger.warning(f"[Candles] {symbol} {tf}: {j.get('msg','no data')}")
                return None
            rows = []
            for c in reversed(j["data"]):  # OKX newest first → reverse to oldest first
                try:
                    rows.append({
                        "ts":     int(c[0]),
                        "open":   float(c[1]),
                        "high":   float(c[2]),
                        "low":    float(c[3]),
                        "close":  float(c[4]),
                        "volume": float(c[5]),
                    })
                except Exception:
                    continue
            if len(rows) < 5:
                return None
            df = pd.DataFrame(rows)
            df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            return df.set_index("ts")
    except Exception as e:
        logger.warning(f"[Candles] {symbol} {tf}: {e}")
        return None


async def _fetch_price(symbol: str) -> float:
    """Real-time last price from OKX ticker."""
    try:
        async with httpx.AsyncClient(timeout=8, headers=_HDR) as cl:
            r = await cl.get(f"{_OKX}/market/ticker",
                             params={"instId": _inst(symbol)})
            if r.status_code == 200:
                j = r.json()
                if j.get("code") == "0" and j.get("data"):
                    return float(j["data"][0].get("last", 0))
    except Exception as e:
        logger.warning(f"[Price] {symbol}: {e}")
    return 0.0


async def _fetch_macro() -> Dict:
    """USDT.D + BTC.D from CoinGecko."""
    try:
        async with httpx.AsyncClient(timeout=12) as cl:
            r = await cl.get("https://api.coingecko.com/api/v3/global",
                             headers={"Accept": "application/json"})
            if r.status_code == 200:
                d = r.json().get("data", {})
                dom = d.get("market_cap_percentage", {})
                return {
                    "usdt_d":   round(dom.get("usdt", 7.0), 3),
                    "btc_d":    round(dom.get("btc",  50.0), 2),
                    "total_mc": d.get("total_market_cap", {}).get("usd", 0),
                    "ok": True,
                }
    except Exception as e:
        logger.warning(f"[Macro] {e}")
    return {"usdt_d": 7.0, "btc_d": 50.0, "total_mc": 0, "ok": False}


# ═══════════════════════════════════════════════════════════════════════
# INDICATORS (pandas-ta when available, manual fallback)
# ═══════════════════════════════════════════════════════════════════════

def _rsi(df: pd.DataFrame, period: int = 14) -> float:
    if HAS_TA:
        try:
            v = ta.rsi(df["close"], length=period)
            if v is not None and not v.empty:
                return round(float(v.iloc[-1]), 2)
        except Exception:
            pass
    # Manual RSI fallback
    delta = df["close"].diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, float("nan"))
    rsi   = 100 - 100 / (1 + rs)
    return round(float(rsi.iloc[-1]), 2) if not rsi.empty else 50.0


def _macd(df: pd.DataFrame) -> Dict:
    if HAS_TA:
        try:
            m = ta.macd(df["close"], fast=12, slow=26, signal=9)
            if m is not None and not m.empty:
                cols = list(m.columns)
                macd_v = float(m[cols[0]].iloc[-1])
                sig_v  = float(m[cols[1]].iloc[-1])
                hist_v = float(m[cols[2]].iloc[-1])
                return {"macd": macd_v, "signal": sig_v,
                        "hist": hist_v, "bull": hist_v > 0}
        except Exception:
            pass
    # Manual fallback
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    macd_s= ema12 - ema26
    sig   = macd_s.ewm(span=9, adjust=False).mean()
    hist  = macd_s - sig
    return {"macd":  round(float(macd_s.iloc[-1]), 6),
            "signal":round(float(sig.iloc[-1]),    6),
            "hist":  round(float(hist.iloc[-1]),   6),
            "bull":  float(hist.iloc[-1]) > 0}


def _ema(df: pd.DataFrame, period: int) -> float:
    if HAS_TA:
        try:
            v = ta.ema(df["close"], length=period)
            if v is not None and not v.empty:
                return round(float(v.iloc[-1]), 4)
        except Exception:
            pass
    return round(float(df["close"].ewm(span=period, adjust=False).mean().iloc[-1]), 4)


def _atr(df: pd.DataFrame, period: int = 14) -> float:
    if HAS_TA:
        try:
            v = ta.atr(df["high"], df["low"], df["close"], length=period)
            if v is not None and not v.empty:
                return round(float(v.iloc[-1]), 6)
        except Exception:
            pass
    hl  = df["high"] - df["low"]
    hpc = (df["high"] - df["close"].shift()).abs()
    lpc = (df["low"]  - df["close"].shift()).abs()
    tr  = pd.concat([hl, hpc, lpc], axis=1).max(axis=1)
    return round(float(tr.rolling(period).mean().iloc[-1]), 6)


def _bbands(df: pd.DataFrame, period: int = 20) -> Dict:
    if HAS_TA:
        try:
            bb = ta.bbands(df["close"], length=period, std=2)
            if bb is not None and not bb.empty:
                cols = list(bb.columns)
                return {"upper": float(bb[cols[2]].iloc[-1]),
                        "mid":   float(bb[cols[1]].iloc[-1]),
                        "lower": float(bb[cols[0]].iloc[-1])}
        except Exception:
            pass
    mid = df["close"].rolling(period).mean()
    std = df["close"].rolling(period).std()
    return {"upper": float((mid + 2*std).iloc[-1]),
            "mid":   float(mid.iloc[-1]),
            "lower": float((mid - 2*std).iloc[-1])}


def _obv(df: pd.DataFrame) -> Dict:
    if HAS_TA:
        try:
            v = ta.obv(df["close"], df["volume"])
            if v is not None and len(v) >= 10:
                recent = float(v.iloc[-5:].mean())
                old    = float(v.iloc[-10:-5].mean())
                p_now  = float(df["close"].iloc[-1])
                p_old  = float(df["close"].iloc[-6]) if len(df) > 5 else p_now
                return {"rising":   recent > old,
                        "falling":  recent < old,
                        "div_bull": p_now < p_old and recent > old,
                        "div_bear": p_now > p_old and recent < old}
        except Exception:
            pass
    direction = df["close"].diff().apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
    obv_s = (direction * df["volume"]).cumsum()
    recent = float(obv_s.iloc[-5:].mean()); old = float(obv_s.iloc[-10:-5].mean())
    p_now = float(df["close"].iloc[-1]); p_old = float(df["close"].iloc[-6]) if len(df)>5 else p_now
    return {"rising": recent>old, "falling": recent<old,
            "div_bull": p_now<p_old and recent>old, "div_bear": p_now>p_old and recent<old}


def _stoch(df: pd.DataFrame) -> Dict:
    if HAS_TA:
        try:
            s = ta.stoch(df["high"], df["low"], df["close"])
            if s is not None and not s.empty:
                k = float(s.iloc[-1, 0])
                return {"k": k, "oversold": k < 20, "overbought": k > 80}
        except Exception:
            pass
    return {"k": 50.0, "oversold": False, "overbought": False}


def _cmf(df: pd.DataFrame) -> float:
    if HAS_TA:
        try:
            v = ta.cmf(df["high"], df["low"], df["close"], df["volume"])
            if v is not None and not v.empty:
                return round(float(v.iloc[-1]), 4)
        except Exception:
            pass
    return 0.0


def _vwap(df: pd.DataFrame) -> float:
    try:
        typical = (df["high"] + df["low"] + df["close"]) / 3
        return round(float((typical * df["volume"]).sum() / df["volume"].sum()), 4)
    except Exception:
        return float(df["close"].iloc[-1])


def _supertrend(df: pd.DataFrame) -> Dict:
    if HAS_TA:
        try:
            st = ta.supertrend(df["high"], df["low"], df["close"], length=10, multiplier=3)
            if st is not None and not st.empty:
                dir_col = [c for c in st.columns if "SUPERTd" in c]
                if dir_col:
                    cur  = int(st[dir_col[0]].iloc[-1])
                    prev = int(st[dir_col[0]].iloc[-2]) if len(st) > 1 else cur
                    return {"bull": cur == 1, "bear": cur == -1,
                            "flip": cur != prev,
                            "bull_flip": cur == 1 and prev != 1,
                            "bear_flip": cur == -1 and prev == 1}
        except Exception:
            pass
    return {"bull": False, "bear": False, "flip": False,
            "bull_flip": False, "bear_flip": False}


def _ichimoku(df: pd.DataFrame) -> Dict:
    if len(df) < 52:
        return {"above": False, "below": False, "bull_cross": False,
                "in_cloud": True, "cloud_top": 0.0, "cloud_bot": 0.0}
    hi = df["high"]; lo = df["low"]; price = float(df["close"].iloc[-1])
    ten = (hi.iloc[-9:].max()  + lo.iloc[-9:].min())  / 2
    kij = (hi.iloc[-26:].max() + lo.iloc[-26:].min()) / 2
    sa  = (ten + kij) / 2
    sb  = (hi.iloc[-52:].max() + lo.iloc[-52:].min()) / 2
    ct  = max(float(sa), float(sb))
    cb  = min(float(sa), float(sb))
    return {"above": price > ct, "below": price < cb,
            "in_cloud": cb <= price <= ct,
            "bull_cross": float(ten) > float(kij),
            "bear_cross": float(ten) < float(kij),
            "cloud_top": round(ct, 4), "cloud_bot": round(cb, 4),
            "ten": round(float(ten), 4), "kij": round(float(kij), 4)}


def _heikin_ashi(df: pd.DataFrame) -> Dict:
    ha_close = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    ha_open  = ha_close.shift(2)
    if ha_open.isna().all():
        return {"bull": True, "bear": False, "strong_bull": False, "strong_bear": False}
    last_c = float(ha_close.iloc[-1]); last_o = float(ha_open.iloc[-1])
    last_h = float(df["high"].iloc[-1]); last_l = float(df["low"].iloc[-1])
    bull   = last_c > last_o
    strong_bull = bull and abs(last_l - min(last_o, last_c)) < (last_h - last_l) * 0.05
    strong_bear = not bull and abs(last_h - max(last_o, last_c)) < (last_h - last_l) * 0.05
    return {"bull": bull, "bear": not bull,
            "strong_bull": strong_bull, "strong_bear": strong_bear}


# ═══════════════════════════════════════════════════════════════════════
# FIBONACCI LEVELS
# ═══════════════════════════════════════════════════════════════════════

def _fibonacci(df4h: pd.DataFrame, df1d: Optional[pd.DataFrame], price: float) -> Dict:
    try:
        hi_4h = float(df4h["high"].iloc[-120:].max())  if len(df4h) >= 120 else float(df4h["high"].max())
        lo_4h = float(df4h["low"].iloc[-120:].min())   if len(df4h) >= 120 else float(df4h["low"].min())
        hi_1d = float(df1d["high"].max()) if df1d is not None and len(df1d) >= 5 else hi_4h
        lo_1d = float(df1d["low"].min())  if df1d is not None and len(df1d) >= 5 else lo_4h
        hi = max(hi_4h, hi_1d); lo = min(lo_4h, lo_1d)
        rng = hi - lo
        if rng <= 0: return {}
        lvls = {
            "23.6": round(hi - rng * 0.236, 4),
            "38.2": round(hi - rng * 0.382, 4),
            "50.0": round(hi - rng * 0.500, 4),
            "61.8": round(hi - rng * 0.618, 4),  # 🌟 Golden
            "78.6": round(hi - rng * 0.786, 4),
            "88.6": round(hi - rng * 0.886, 4),
            "ext_127": round(lo + rng * 1.272, 4),
            "ext_161": round(lo + rng * 1.618, 4),
            "ext_261": round(lo + rng * 2.618, 4),
        }
        nearest = min(lvls.items(), key=lambda x: abs(x[1] - price))
        res = sorted([(k,v) for k,v in lvls.items() if v > price * 1.001], key=lambda x: x[1])
        sup = sorted([(k,v) for k,v in lvls.items() if v < price * 0.999], key=lambda x: x[1], reverse=True)
        return {"levels": lvls, "nearest": nearest,
                "resistances": res[:4], "supports": sup[:4],
                "hi": round(hi,4), "lo": round(lo,4), "rng": round(rng,4)}
    except Exception as e:
        logger.debug(f"[Fib] {e}"); return {}


# ═══════════════════════════════════════════════════════════════════════
# GANN SQUARE OF 9
# ═══════════════════════════════════════════════════════════════════════

def _gann_sq9(price: float) -> Dict:
    if price <= 0: return {"levels": [], "nearest": price, "resistances": [], "supports": []}
    root = math.sqrt(price)
    lvls = []
    for step in [-2.0, -1.75, -1.5, -1.25, -1.0, -0.75, -0.5, -0.25,
                  0.0,  0.25,  0.5,  0.75,  1.0,  1.25,  1.5,  1.75,  2.0]:
        v = round((root + step) ** 2, 4)
        if v > 0: lvls.append(v)
    lvls = sorted(set(lvls))
    nearest = min(lvls, key=lambda x: abs(x - price)) if lvls else price
    res = sorted([v for v in lvls if v > price * 1.001])[:4]
    sup = sorted([v for v in lvls if v < price * 0.999], reverse=True)[:4]
    return {"levels": lvls, "nearest": round(nearest,4),
            "resistances": res, "supports": sup, "root": round(root, 6)}


def _gann_angles(df4h: pd.DataFrame, price: float) -> Dict:
    try:
        lo_idx  = df4h["low"].iloc[-60:].idxmin()
        lo_val  = float(df4h["low"].loc[lo_idx])
        bars    = len(df4h) - df4h.index.get_loc(lo_idx) - 1
        atr_v   = _atr(df4h, 14)
        if atr_v <= 0: raise ValueError("atr zero")
        a1x1 = lo_val + bars * atr_v
        a2x1 = lo_val + bars * atr_v * 2.0
        a1x2 = lo_val + bars * atr_v * 0.5
        return {"pivot_low": round(lo_val,4), "bars": bars,
                "angle_1x1": round(a1x1,4),
                "angle_2x1": round(a2x1,4),
                "angle_1x2": round(a1x2,4),
                "above_1x1": price > a1x1,
                "above_2x1": price > a2x1}
    except Exception as e:
        logger.debug(f"[Gann] {e}")
        return {"above_1x1": False, "above_2x1": False,
                "angle_1x1": 0, "angle_2x1": 0, "angle_1x2": 0}


# ═══════════════════════════════════════════════════════════════════════
# SMC — Smart Money Concepts
# ═══════════════════════════════════════════════════════════════════════

def _smc(df4h: pd.DataFrame, price: float) -> Dict:
    result = {"bias": "NEUTRAL", "bos_bull": False, "bos_bear": False,
              "ob_bull": None, "ob_bear": None,
              "liq_hi": None, "liq_lo": None,
              "sweep_bull": False, "sweep_bear": False}
    if len(df4h) < 12: return result
    try:
        r_hi = float(df4h["high"].iloc[-6:].max())
        r_lo = float(df4h["low"].iloc[-6:].min())
        p_hi = float(df4h["high"].iloc[-12:-6].max())
        p_lo = float(df4h["low"].iloc[-12:-6].min())
        bos_bull = r_hi > p_hi * 1.003 and price > p_hi
        bos_bear = r_lo < p_lo * 0.997 and price < p_lo
        result["bos_bull"] = bos_bull; result["bos_bear"] = bos_bear
        # Order Blocks
        for i in range(1, min(len(df4h)-1, 20)):
            row  = df4h.iloc[-(i+1)]; nxt = df4h.iloc[-i]
            o,h,l,c = float(row.open),float(row.high),float(row.low),float(row.close)
            nc = float(nxt.close); nh = float(nxt.high); nl = float(nxt.low)
            if c < o and nc > nh and result["ob_bull"] is None:
                result["ob_bull"] = {"top": round(o,4), "bot": round(l,4), "mid": round((o+l)/2,4)}
            if c > o and nc < nl and result["ob_bear"] is None:
                result["ob_bear"] = {"top": round(h,4), "bot": round(c,4), "mid": round((h+c)/2,4)}
            if result["ob_bull"] and result["ob_bear"]: break
        # Liquidity
        result["liq_hi"] = round(float(df4h["high"].iloc[-20:].max()), 4)
        result["liq_lo"] = round(float(df4h["low"].iloc[-20:].min()),  4)
        # Liquidity sweep
        prev_hi = float(df4h["high"].iloc[-8:-1].max())
        prev_lo = float(df4h["low"].iloc[-8:-1].min())
        last_hi = float(df4h["high"].iloc[-1])
        last_lo = float(df4h["low"].iloc[-1])
        result["sweep_bull"] = last_lo < prev_lo * 0.999 and price > prev_lo
        result["sweep_bear"] = last_hi > prev_hi * 1.001 and price < prev_hi
        # Bias
        if bos_bull:   result["bias"] = "BULL"
        elif bos_bear: result["bias"] = "BEAR"
    except Exception as e:
        logger.debug(f"[SMC] {e}")
    return result


# ═══════════════════════════════════════════════════════════════════════
# PIVOT POINTS
# ═══════════════════════════════════════════════════════════════════════

def _pivots(df1d: Optional[pd.DataFrame]) -> Dict:
    if df1d is None or len(df1d) < 2: return {}
    prev = df1d.iloc[-2]
    H = float(prev.high); L = float(prev.low); C = float(prev.close)
    PP = (H + L + C) / 3
    return {"PP": round(PP,4),
            "R1": round(2*PP-L,4),   "R2": round(PP+(H-L),4),  "R3": round(H+2*(PP-L),4),
            "S1": round(2*PP-H,4),   "S2": round(PP-(H-L),4),  "S3": round(L-2*(H-PP),4)}


# ═══════════════════════════════════════════════════════════════════════
# CANDLESTICK PATTERNS
# ═══════════════════════════════════════════════════════════════════════

def _candle_pattern(df: pd.DataFrame) -> Dict:
    if len(df) < 3: return {"name": "—", "bull": None}
    r0 = df.iloc[-1]; r1 = df.iloc[-2]; r2 = df.iloc[-3]
    o0,h0,l0,c0 = float(r0.open),float(r0.high),float(r0.low),float(r0.close)
    o1,h1,l1,c1 = float(r1.open),float(r1.high),float(r1.low),float(r1.close)
    o2,h2,l2,c2 = float(r2.open),float(r2.high),float(r2.low),float(r2.close)
    b1 = abs(c1-o1); r1v = h1-l1 or 0.0001
    b0 = abs(c0-o0); r0v = h0-l0 or 0.0001
    # Hammer
    if c1>o1 and (min(o1,c1)-l1) > b1*2 and (h1-max(o1,c1)) < b1*0.4:
        return {"name": "🔨 Hammer", "bull": True}
    # Shooting Star
    if c1<o1 and (h1-max(o1,c1)) > b1*2 and (min(o1,c1)-l1) < b1*0.4:
        return {"name": "⭐ Shooting Star", "bull": False}
    # Bullish Engulfing
    if c1>o1 and c0<o0 and o1<c0 and c1>o0:
        return {"name": "🟢 Bull Engulf", "bull": True}
    # Bearish Engulfing
    if c1<o1 and c0>o0 and o1>c0 and c1<o0:
        return {"name": "🔴 Bear Engulf", "bull": False}
    # Doji
    if b0 < r0v * 0.08:
        return {"name": "⚖️ Doji", "bull": None}
    # Morning Star
    if c2<o2 and b1<b2*0.4 and c0>o0 and c0>(o2+c2)/2 if (b2:=abs(c2-o2)) else False:
        return {"name": "🌅 Morning Star", "bull": True}
    return {"name": f"{'🟢 Bull' if c0>o0 else '🔴 Bear'} Candle", "bull": c0>o0}


# ═══════════════════════════════════════════════════════════════════════
# SMART TARGET BUILDER
# ═══════════════════════════════════════════════════════════════════════

def _build_targets(price: float, fib: Dict, gann: Dict,
                   pvt: Dict, smc: Dict, ema50: float, ema200: float) -> Dict:
    res: List[Tuple[float,str]] = []
    sup: List[Tuple[float,str]] = []
    def ar(v, s):
        if v and v > price * 1.003: res.append((round(float(v),4), s))
    def as_(v, s):
        if v and v < price * 0.997: sup.append((round(float(v),4), s))
    # Fibonacci
    for k,v in (fib.get("levels") or {}).items():
        lbl = f"Fib {k}%{'🌟' if k=='61.8' else ''}"
        ar(v, lbl); as_(v, lbl)
    # Gann
    for g in gann.get("resistances", [])[:3]: ar(g, "Gann Sq9 ✨")
    for g in gann.get("supports",    [])[:3]: as_(g, "Gann Sq9 ✨")
    # Pivot
    for k in ["R1","R2","R3"]: ar(pvt.get(k), f"Pivot {k}")
    for k in ["S1","S2","S3"]: as_(pvt.get(k), f"Pivot {k}")
    # SMC
    if smc.get("ob_bear"): ar( smc["ob_bear"]["mid"], "SMC Bear OB 🏛️")
    if smc.get("ob_bull"): as_(smc["ob_bull"]["mid"], "SMC Bull OB 🏛️")
    if smc.get("liq_hi"):  ar( smc["liq_hi"],         "SMC Liquidity 💧")
    if smc.get("liq_lo"):  as_(smc["liq_lo"],          "SMC Liquidity 💧")
    # EMAs
    ar(ema50, "EMA50"); as_(ema50, "EMA50")
    ar(ema200,"EMA200"); as_(ema200,"EMA200")
    # Dedup
    def dedup(lst):
        out = []
        for lvl,src in sorted(lst, key=lambda x: x[0]):
            if not out or abs(lvl - out[-1][0]) / (out[-1][0] or 1) > 0.003:
                out.append((lvl, src))
        return out
    r3 = dedup(res)[:3]; s3 = dedup(sup)[:3]
    while len(r3) < 3: r3.append((round((r3[-1][0] if r3 else price)*1.025, 4), "ATR Ext"))
    while len(s3) < 3: s3.append((round((s3[-1][0] if s3 else price)*0.975, 4), "ATR Ext"))
    return {"resistances": r3[:3], "supports": s3[:3]}


# ═══════════════════════════════════════════════════════════════════════
# FULL ASSET ANALYSIS
# ═══════════════════════════════════════════════════════════════════════

def _analyze_asset(symbol: str, price: float,
                   df1d: Optional[pd.DataFrame],
                   df4h: pd.DataFrame,
                   df1h: Optional[pd.DataFrame]) -> Optional[Dict]:
    try:
        if df4h is None or len(df4h) < 20: return None
        # Indicators
        rsi_4h  = _rsi(df4h, 14)
        rsi_1h  = _rsi(df1h, 14) if df1h is not None and len(df1h) >= 15 else rsi_4h
        rsi_1d  = _rsi(df1d, 14) if df1d is not None and len(df1d) >= 15 else rsi_4h
        macd_   = _macd(df4h)
        ema20   = _ema(df4h, 20)
        ema50   = _ema(df4h, 50)
        ema200  = _ema(df4h, 200) if len(df4h) >= 200 else _ema(df4h, len(df4h)//2)
        ema20_1h= _ema(df1h, 20)  if df1h is not None and len(df1h) >= 20 else ema20
        bb_     = _bbands(df4h, 20)
        obv_    = _obv(df4h)
        stoch_  = _stoch(df1h if df1h is not None and len(df1h)>=20 else df4h)
        cmf_    = _cmf(df4h)
        vwap_   = _vwap(df4h.iloc[-48:] if len(df4h)>=48 else df4h)
        st_     = _supertrend(df4h)
        ichi_   = _ichimoku(df4h)
        ha_     = _heikin_ashi(df4h)
        candle_ = _candle_pattern(df4h)
        atr_    = _atr(df4h, 14)
        fib_    = _fibonacci(df4h, df1d, price)
        gann_   = _gann_sq9(price)
        gann_a_ = _gann_angles(df4h, price)
        smc_    = _smc(df4h, price)
        pvt_    = _pivots(df1d)
        tgts_   = _build_targets(price, fib_, gann_, pvt_, smc_, ema50, ema200)

        # Score (normalized -1 to +1 per school)
        scores: List[float] = []
        # 1. EMA Alignment
        if price > ema20 > ema50 > ema200:     scores.append(1.0)
        elif price > ema20 > ema50:            scores.append(0.6)
        elif price < ema20 < ema50 < ema200:   scores.append(-1.0)
        elif price < ema20 < ema50:            scores.append(-0.6)
        else:                                   scores.append(0.0)
        # 2. RSI
        if rsi_4h < 30:    scores.append(0.9)
        elif rsi_4h < 40:  scores.append(0.5)
        elif rsi_4h < 50:  scores.append(0.2)
        elif rsi_4h > 70:  scores.append(-0.9)
        elif rsi_4h > 60:  scores.append(-0.5)
        elif rsi_4h > 50:  scores.append(-0.2)
        else:              scores.append(0.0)
        # 3. MACD
        scores.append(0.7 if macd_["bull"] else -0.7)
        # 4. Ichimoku
        if ichi_["above"] and ichi_["bull_cross"]:    scores.append(1.0)
        elif ichi_["above"]:                           scores.append(0.5)
        elif ichi_["below"] and ichi_["bear_cross"]:   scores.append(-1.0)
        elif ichi_["below"]:                           scores.append(-0.5)
        else:                                          scores.append(0.0)
        # 5. SuperTrend
        if st_["bull_flip"]:    scores.append(1.0)
        elif st_["bull"]:       scores.append(0.6)
        elif st_["bear_flip"]:  scores.append(-1.0)
        elif st_["bear"]:       scores.append(-0.6)
        else:                   scores.append(0.0)
        # 6. Heikin Ashi
        if ha_["strong_bull"]: scores.append(0.8)
        elif ha_["bull"]:      scores.append(0.4)
        elif ha_["strong_bear"]:scores.append(-0.8)
        else:                  scores.append(-0.4)
        # 7. Bollinger Bands
        if price <= bb_["lower"]: scores.append(0.7)
        elif price >= bb_["upper"]:scores.append(-0.7)
        elif price > bb_["mid"]:  scores.append(0.2)
        else:                     scores.append(-0.2)
        # 8. OBV
        if obv_["div_bull"]:   scores.append(0.9)
        elif obv_["rising"]:   scores.append(0.4)
        elif obv_["div_bear"]: scores.append(-0.9)
        else:                  scores.append(-0.4)
        # 9. CMF
        if cmf_ > 0.15:   scores.append(0.6)
        elif cmf_ > 0:    scores.append(0.2)
        elif cmf_ < -0.15:scores.append(-0.6)
        elif cmf_ < 0:    scores.append(-0.2)
        else:             scores.append(0.0)
        # 10. VWAP
        scores.append(0.4 if price > vwap_ else -0.4)
        # 11. Stochastic
        if stoch_["oversold"]:   scores.append(0.6)
        elif stoch_["overbought"]:scores.append(-0.6)
        else:                    scores.append(0.0)
        # 12. Fibonacci position
        fib_618 = (fib_.get("levels") or {}).get("61.8", price)
        fib_382 = (fib_.get("levels") or {}).get("38.2", price)
        if price > fib_382:    scores.append(0.5)
        elif price < fib_618:  scores.append(-0.5)
        else:                  scores.append(0.0)
        # 13. Gann Angles
        if gann_a_.get("above_2x1"):   scores.append(0.9)
        elif gann_a_.get("above_1x1"): scores.append(0.5)
        else:                          scores.append(-0.3)
        # 14. SMC
        if smc_["bias"] == "BULL":     scores.append(0.8)
        elif smc_["bias"] == "BEAR":   scores.append(-0.8)
        else:                          scores.append(0.0)
        if smc_["sweep_bull"]:         scores.append(0.5)
        if smc_["sweep_bear"]:         scores.append(-0.5)
        # 15. Candlestick
        if candle_["bull"] is True:    scores.append(0.4)
        elif candle_["bull"] is False: scores.append(-0.4)
        # 16. Pivot position
        if pvt_.get("PP"):
            scores.append(0.3 if price > pvt_["PP"] else -0.3)

        avg = sum(scores) / len(scores)
        conf = int(min(abs(avg), 1.0) * 100)
        direction = "BULL" if avg > 0.15 else "BEAR" if avg < -0.15 else "NEUTRAL"

        return {
            "symbol": symbol, "price": price, "direction": direction,
            "confidence": conf, "avg_score": round(avg, 3),
            "rsi_4h": rsi_4h, "rsi_1h": rsi_1h, "rsi_1d": rsi_1d,
            "macd": macd_, "ema20": ema20, "ema50": ema50, "ema200": ema200,
            "ema20_1h": ema20_1h, "bb": bb_, "obv": obv_, "stoch": stoch_,
            "cmf": cmf_, "vwap": vwap_, "supertrend": st_, "ichimoku": ichi_,
            "heikin_ashi": ha_, "candle": candle_, "atr": atr_,
            "fib": fib_, "gann": gann_, "gann_angles": gann_a_,
            "smc": smc_, "pivots": pvt_, "targets": tgts_,
        }
    except Exception as e:
        logger.error(f"[Analyze] {symbol}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════
# TELEGRAM MESSAGE BUILDERS
# ═══════════════════════════════════════════════════════════════════════

def _f(v: float) -> str:
    if v >= 10000: return f"${v:,.0f}"
    if v >= 100:   return f"${v:,.2f}"
    if v >= 1:     return f"${v:,.4f}"
    return f"${v:.6f}"

def _de(d: str) -> str:
    return "🟢" if d == "BULL" else "🔴" if d == "BEAR" else "🟡"


def _asset_msg(a: Dict) -> str:
    sym  = a["symbol"].replace("/USDT:USDT","")
    icon = "₿" if "BTC" in sym else "Ξ"
    p    = a["price"]; d = a["direction"]; c = a["confidence"]
    r4   = a["rsi_4h"]; r1h = a["rsi_1h"]; r1d = a["rsi_1d"]
    mac  = a["macd"]; bb = a["bb"]; obv = a["obv"]
    st   = a["supertrend"]; ichi = a["ichimoku"]; ha = a["heikin_ashi"]
    cnd  = a["candle"]; fib = a["fib"]; gann = a["gann"]
    ga   = a["gann_angles"]; smc = a["smc"]; pvt = a["pivots"]
    tgt  = a["targets"]
    e20  = a["ema20"]; e50 = a["ema50"]; e200 = a["ema200"]
    vwap = a["vwap"]; cmf  = a["cmf"]; stoch = a["stoch"]

    # EMA
    if   p > e20 > e50 > e200: ema_l = "فوق 20/50/200 ✅"
    elif p > e20 > e50:         ema_l = "فوق EMA20/50 🔼"
    elif p < e20 < e50 < e200: ema_l = "تحت 20/50/200 ❌"
    elif p < e20 < e50:         ema_l = "تحت EMA20/50 🔽"
    else:                       ema_l = "متذبذب ↔️"

    # Ichimoku
    if   ichi["above"] and ichi["bull_cross"]:  ich_l = "فوق السحابة — صاعد قوي ✅"
    elif ichi["above"]:                          ich_l = "فوق السحابة 🔼"
    elif ichi["below"] and ichi["bear_cross"]:   ich_l = "تحت السحابة — هابط قوي ❌"
    elif ichi["below"]:                          ich_l = "تحت السحابة 🔽"
    else:                                        ich_l = "داخل السحابة ↔️"

    # SuperTrend
    if   st["bull_flip"]: st_l = "انعكاس للصعود 🚀"
    elif st["bear_flip"]: st_l = "انعكاس للهبوط 💥"
    elif st["bull"]:      st_l = "صاعد ✅"
    else:                 st_l = "هابط ❌"

    # Heikin Ashi
    if   ha["strong_bull"]: ha_l = "صعود قوي (لا ظل سفلي) 🟢🔥"
    elif ha["bull"]:         ha_l = "صعود 🟢"
    elif ha["strong_bear"]:  ha_l = "هبوط قوي (لا ظل علوي) 🔴🔥"
    else:                    ha_l = "هبوط 🔴"

    # Fibonacci nearest
    fn = fib.get("nearest", ("?",0))
    fib_l = f"الأقرب: Fib {fn[0]}% @ {_f(fn[1])}" if isinstance(fn,tuple) else "—"

    # Gann
    g_near = _f(gann.get("nearest", p))
    g_ang  = f"فوق 1×1 ✅ ({_f(ga.get('angle_1x1',0))})" if ga.get("above_1x1") \
             else f"تحت 1×1 ⚠️ ({_f(ga.get('angle_1x1',0))})"

    # SMC
    smc_b  = {"BULL":"صاعد 🏛️","BEAR":"هابط 🏛️","NEUTRAL":"محايد ↔️"}.get(smc["bias"],"—")
    smc_ex = ""
    if smc["sweep_bull"]: smc_ex = " | Sweep القاع ✅"
    if smc["sweep_bear"]: smc_ex = " | Sweep القمة ⚠️"
    if smc.get("ob_bull"): smc_ex += f" | OB دعم {_f(smc['ob_bull']['mid'])}"
    if smc.get("ob_bear"): smc_ex += f" | OB مقاومة {_f(smc['ob_bear']['mid'])}"

    # OBV
    obv_l = ("تباعد إيجابي 🟢🔥" if obv["div_bull"] else
             "حجم صاعد 📈"        if obv["rising"] else
             "تباعد سلبي 🔴⚠️"   if obv["div_bear"] else "حجم هابط 📉")

    # CMF
    cmf_l = f"{cmf:+.3f} ({'تدفق شراء ✅' if cmf>0.1 else 'تدفق بيع ❌' if cmf<-0.1 else 'محايد'})"

    # Stochastic
    sto_l = f"{stoch['k']:.0f} — {'تشبع بيع 🔵' if stoch['oversold'] else 'تشبع شراء 🔴' if stoch['overbought'] else 'طبيعي'}"

    # Pivot position
    pvt_l = f"فوق PP ({_f(pvt.get('PP',0))})" if pvt.get("PP") and p > pvt["PP"] else \
            f"تحت PP ({_f(pvt.get('PP',0))})" if pvt.get("PP") else "—"

    # VWAP
    vwap_l = f"{'فوق ✅' if p > vwap else 'تحت ⚠️'} VWAP {_f(vwap)}"

    # BB
    if   p >= bb["upper"]: bb_l = f"عند الحد العلوي 🔴 {_f(bb['upper'])}"
    elif p <= bb["lower"]: bb_l = f"عند الحد السفلي 🟢 {_f(bb['lower'])}"
    else:                  bb_l = f"داخل النطاق | Mid: {_f(bb['mid'])}"

    # Targets
    r_lines = [f"  🎯 R{i+1}: {_f(l)} ← {s}"
               for i,(l,s) in enumerate(tgt.get("resistances",[])[:3])]
    s_lines = [f"  🛡️ S{i+1}: {_f(l)} ← {s}"
               for i,(l,s) in enumerate(tgt.get("supports",[])[:3])]

    # Verdict
    if   d == "BULL": verd = f"🟢 <b>ترند صاعد — ثقة {c}%</b>\n  ✅ ابحث عن صفقات LONG"
    elif d == "BEAR": verd = f"🔴 <b>ترند هابط — ثقة {c}%</b>\n  ✅ ابحث عن صفقات SHORT"
    else:             verd = f"🟡 <b>محايد — انتظر تأكيداً</b>"

    return (
        f"{'━'*32}\n"
        f"{icon} <b>{sym}/USDT</b>  ·  {_f(p)}\n"
        f"{_de(d)} الاتجاه: <b>{d}</b>  |  ثقة: <b>{c}%</b>\n\n"
        f"<b>📋 التحليل الفني (1D / 4H / 1H):</b>\n"
        f"  📊 RSI:        4H={r4:.0f}  1H={r1h:.0f}  1D={r1d:.0f}\n"
        f"  📉 MACD:       {'صاعد ✅' if mac['bull'] else 'هابط ❌'} (hist={mac['hist']:+.5f})\n"
        f"  📊 EMA:        {ema_l}\n"
        f"  ☁️ Ichimoku:   {ich_l}\n"
        f"  🔄 SuperTrend: {st_l}\n"
        f"  🕯️ Heikin Ashi:{ha_l}\n"
        f"  🎯 BB:         {bb_l}\n"
        f"  📈 OBV:        {obv_l}\n"
        f"  💧 CMF:        {cmf_l}\n"
        f"  📊 Stoch:      {sto_l}\n"
        f"  💹 VWAP:       {vwap_l}\n"
        f"  📌 Pivot:      {pvt_l}\n"
        f"  🎯 Fibonacci:  {fib_l}\n"
        f"  ✨ Gann Sq9:   أقرب مستوى = {g_near}\n"
        f"  📐 Gann زوايا: {g_ang}\n"
        f"  🏛️ SMC:        {smc_b}{smc_ex}\n"
        f"  🕯️ الشمعة:    {cnd['name']}\n\n"
        f"<b>🎯 أهداف الصعود (مقاومات):</b>\n" + "\n".join(r_lines) + "\n\n"
        f"<b>🛡️ أهداف الهبوط (دعوم):</b>\n"   + "\n".join(s_lines) + "\n\n"
        f"<b>🏁 الخلاصة:</b> {verd}"
    )


def _e11_msg(e11: Dict, usdt_d: float, btc_d: float, now_str: str) -> str:
    d    = e11.get("direction","NEUTRAL")
    rsi  = e11.get("rsi", 50.0)
    e20  = e11.get("ema20", usdt_d); e50 = e11.get("ema50", usdt_d)
    mac_b= e11.get("macd_bull", False)
    t_up = e11.get("targets_up", []); t_dn = e11.get("targets_down", [])

    if d == "BULL_USDT":
        verdict = "🔴 USDT.D صاعد = ضغط بيع على الكريبتو"
        action  = "⚠️  توقع ضغط بيع على الكريبتو\n  ✅ ابحث عن صفقات SHORT"
        tgt_lbl = "📈 أهداف صعود USDT.D (هبوط للكريبتو):"; tgts = t_up
    elif d == "BEAR_USDT":
        verdict = "🟢 USDT.D هابط = إشارة صعود قوية للكريبتو"
        action  = "✅ سوق الكريبتو في وضع شراء\n  🚀 ابحث عن LONG (Scalp + Swing)"
        tgt_lbl = "📉 أهداف هبوط USDT.D (صعود للكريبتو):"; tgts = t_dn
    else:
        verdict = "🟡 USDT.D محايد — لا اتجاه واضح"
        action  = "⚖️ انتظر تأكيداً من حركة السعر"
        tgt_lbl = "📊 نطاق USDT.D المتوقع:"; tgts = t_dn[:2] + t_up[:1]

    ema_l = ("فوق EMA20/50 🔼" if usdt_d > e20 > e50 else
             "تحت EMA20/50 🔽" if usdt_d < e20 < e50 else "متذبذب ↔️")
    tgt_lines = [f"  🎯 T{i+1}: {t:.3f}%" for i,t in enumerate((tgts or [])[:3])]

    return (
        f"{'━'*32}\n"
        f"📡 <b>E11 — USDT Dominance</b>\n"
        f"  السعر الحالي: <b>{usdt_d:.3f}%</b>  |  BTC.D: <b>{btc_d:.1f}%</b>\n\n"
        f"<b>📋 التحليل (تقريبي 1D):</b>\n"
        f"  📈 RSI:  {rsi:.0f}{'  🔴تشبع شراء' if rsi>70 else '  🔵تشبع بيع' if rsi<30 else ''}\n"
        f"  📊 EMA:  {ema_l}\n"
        f"  📉 MACD: {'صاعد ✅' if mac_b else 'هابط ❌'}\n\n"
        f"<b>{tgt_lbl}</b>\n" + "\n".join(tgt_lines) + "\n\n"
        f"<b>💡 التفسير:</b> {verdict}\n"
        f"<b>📣 التوصية:</b>\n  {action}"
    )


# ═══════════════════════════════════════════════════════════════════════
# E11 USDT.D ANALYSIS
# ═══════════════════════════════════════════════════════════════════════

async def _analyze_usdt_d(usdt_d_now: float) -> Dict:
    """Analyze USDT.D trend using simple slope from CoinGecko history."""
    t_up   = [round(usdt_d_now*1.04,3), round(usdt_d_now*1.08,3), round(usdt_d_now*1.13,3)]
    t_down = [round(usdt_d_now*0.96,3), round(usdt_d_now*0.92,3), round(usdt_d_now*0.87,3)]
    try:
        async with httpx.AsyncClient(timeout=15) as cl:
            r = await cl.get(
                "https://api.coingecko.com/api/v3/coins/tether/market_chart",
                params={"vs_currency":"usd","days":"30","interval":"daily"}
            )
            r2 = await cl.get("https://api.coingecko.com/api/v3/global",
                              headers={"Accept":"application/json"})
            if r.status_code != 200 or r2.status_code != 200:
                raise ValueError("CoinGecko failed")
            usdt_mc = r.json().get("market_caps", [])
            total_now = r2.json().get("data",{}).get("total_market_cap",{}).get("usd", 1)
            if len(usdt_mc) < 10 or total_now <= 0:
                raise ValueError("insufficient data")
            # Build dominance series
            dom_vals = [usdt_d_now]
            scale = usdt_mc[-1][1] / total_now if usdt_mc[-1][1] else 1
            for mc in usdt_mc[-15:-1]:
                approx_total = mc[1] / scale if scale else total_now
                dom_vals.append((mc[1] / approx_total) * 100 if approx_total else usdt_d_now)
            dom_vals = list(reversed(dom_vals))  # oldest first
            dom_s = pd.Series(dom_vals)
            # Indicators
            ema20_d = float(dom_s.ewm(span=min(10, len(dom_s)), adjust=False).mean().iloc[-1])
            ema50_d = float(dom_s.ewm(span=min(15, len(dom_s)), adjust=False).mean().iloc[-1])
            delta   = dom_s.diff()
            gain    = delta.clip(lower=0).rolling(min(10,len(dom_s)-1)).mean()
            loss    = (-delta.clip(upper=0)).rolling(min(10,len(dom_s)-1)).mean()
            rsi_d   = float(100 - 100/(1 + gain.iloc[-1]/(loss.iloc[-1] or 1e-9)))
            ema12d  = dom_s.ewm(span=6,  adjust=False).mean()
            ema26d  = dom_s.ewm(span=12, adjust=False).mean()
            macd_b  = float((ema12d - ema26d).iloc[-1]) > 0
            # Score
            score = 0.0
            if usdt_d_now > ema20_d > ema50_d: score += 0.8
            elif usdt_d_now < ema20_d < ema50_d: score -= 0.8
            if rsi_d > 60: score += 0.4
            elif rsi_d < 40: score -= 0.4
            score += 0.3 if macd_b else -0.3
            direction = ("BULL_USDT" if score > 0.25 else
                         "BEAR_USDT" if score < -0.25 else "NEUTRAL")
            # Fibonacci targets on USDT.D
            recent_hi = float(dom_s.max()); recent_lo = float(dom_s.min())
            rng = recent_hi - recent_lo
            if rng > 0:
                t_up   = sorted([round(usdt_d_now + rng*r, 3) for r in [0.236,0.382,0.618]])[:3]
                t_down = sorted([round(usdt_d_now - rng*r, 3) for r in [0.236,0.382,0.618]],
                                 reverse=True)[:3]
            return {"direction": direction, "score": round(score,3),
                    "rsi": round(rsi_d,2), "ema20": round(ema20_d,3),
                    "ema50": round(ema50_d,3), "macd_bull": macd_b,
                    "targets_up": t_up, "targets_down": t_down}
    except Exception as e:
        logger.warning(f"[E11] {e}")
        direction = ("BULL_USDT" if usdt_d_now > 7.5 else
                     "BEAR_USDT" if usdt_d_now < 6.0 else "NEUTRAL")
        return {"direction": direction, "score": 0.0, "rsi": 50.0,
                "ema20": usdt_d_now, "ema50": usdt_d_now, "macd_bull": False,
                "targets_up": t_up, "targets_down": t_down}


# ═══════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

async def run_full_analysis(db: Any, notifier: Any, fetcher: Any) -> Dict:
    """E10+E11 full analysis with real-time prices. Stores in Supabase."""
    now     = datetime.now(timezone.utc)
    now_str = now.strftime("%d/%m/%Y %H:%M UTC")
    results: Dict[str, Any] = {}

    logger.info(f"[Analysis] 🔍 Starting E10+E11 — {now_str}")

    # ── Macro (USDT.D) ────────────────────────────────────────────────
    macro      = await _fetch_macro()
    usdt_d_now = macro.get("usdt_d", 7.0)
    btc_d      = macro.get("btc_d",  50.0)
    e11        = await _analyze_usdt_d(usdt_d_now)
    results["USDT_D"] = e11

    # Store E11
    try:
        crypto_bias = ("BULL" if e11["direction"] == "BEAR_USDT" else
                       "BEAR" if e11["direction"] == "BULL_USDT" else "NEUTRAL")
        await db.log_regime("USDT.D", crypto_bias, abs(e11["score"]))
    except Exception as ex:
        logger.warning(f"[DB] E11: {ex}")

    # ── E10: BTC + ETH ────────────────────────────────────────────────
    for symbol in ["BTC/USDT:USDT", "ETH/USDT:USDT"]:
        sym_c = symbol.replace("/USDT:USDT", "")
        try:
            logger.info(f"[E10] {sym_c}: fetching real-time price + candles …")
            price, df1d, df4h, df1h = await asyncio.gather(
                _fetch_price(symbol),
                _fetch_candles(symbol, "1d", 90),
                _fetch_candles(symbol, "4h", 200),
                _fetch_candles(symbol, "1h", 100),
            )
            if not price or price <= 0:
                logger.warning(f"[E10] {sym_c}: no price"); continue
            if df4h is None or len(df4h) < 20:
                logger.warning(f"[E10] {sym_c}: no 4H data"); continue

            logger.info(f"[E10] {sym_c}: price={price:,.2f} — running analysis …")
            a = _analyze_asset(symbol, price, df1d, df4h, df1h)
            if not a:
                logger.warning(f"[E10] {sym_c}: analysis returned None"); continue

            results[sym_c] = a
            try:
                await db.log_regime(symbol, a["direction"], a["confidence"]/100)
                logger.info(f"[E10] {sym_c}: {a['direction']} {a['confidence']}% ✅ saved")
            except Exception as ex:
                logger.warning(f"[DB] {sym_c}: {ex}")

        except Exception as e:
            logger.error(f"[E10] {sym_c}: {e}")

    # ── Send Telegram ─────────────────────────────────────────────────
    header = f"📊 <b>Ramos 360 Ai 🎖️ — التحليل الفني</b>\n🕐 {now_str}\n"
    parts  = [header]

    if results.get("BTC"): parts.append(_asset_msg(results["BTC"]))
    if results.get("ETH"): parts.append(_asset_msg(results["ETH"]))
    parts.append(_e11_msg(e11, usdt_d_now, btc_d, now_str))
    parts.append(
        f"{'━'*32}\n"
        f"💾 النتائج محفوظة — الخبراء الآخرون سيقرؤها\n"
        f"<i>🎖️ Ramos 360 Ai — E10+E11</i>"
    )

    # Send each section separately (avoid 4096 limit)
    for part in parts:
        if part.strip():
            await notifier.send(part)
            await asyncio.sleep(0.8)

    logger.success(f"[Analysis] ✅ Done — {len([k for k in results if k!='USDT_D'])} assets")
    return results

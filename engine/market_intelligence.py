
"""
engine/market_intelligence.py — Ramos 360 Ai 🎖️
LAYER 1 — Market Intelligence Engine
Runs every 4 hours. Stores context for Layer 2 + Layer 3.

Experts: Wyckoff · Gann Sq9 · William %R · E10 Bias
Output stored in Supabase → read by all other layers.
"""
from __future__ import annotations
import asyncio, math, time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import httpx
from loguru import logger

_OKX = "https://www.okx.com/api/v5"
_HDR = {"Accept": "application/json"}
_L1_CACHE: Dict = {}


def _inst(symbol: str) -> str:
    return symbol.split(":")[0].replace("/", "-") + "-SWAP"


async def _candles(symbol: str, tf: str, limit: int = 100) -> List:
    try:
        TF = {"1h":"1H","4h":"4H","1d":"1D","1w":"1W","15m":"15m"}
        async with httpx.AsyncClient(timeout=12, headers=_HDR) as cl:
            r = await cl.get(f"{_OKX}/market/candles", params={
                "instId": _inst(symbol),
                "bar": TF.get(tf, tf), "limit": str(limit),
            })
            if r.status_code == 200:
                j = r.json()
                if j.get("code") == "0" and j.get("data"):
                    return list(reversed([
                        [int(c[0]), float(c[1]), float(c[2]),
                         float(c[3]), float(c[4]), float(c[5])]
                        for c in j["data"]
                    ]))
    except Exception as e:
        logger.warning(f"[L1] candles {symbol} {tf}: {e}")
    return []


# ══════════════════════════════════════════════════════════════════
# WYCKOFF PHASE DETECTOR
# ══════════════════════════════════════════════════════════════════

def _wyckoff_phase(candles: List) -> Dict:
    """
    Detect Wyckoff phase from 4H candles.
    Phases: ACCUMULATION | MARKUP | DISTRIBUTION | MARKDOWN | UNKNOWN
    """
    if len(candles) < 40:
        return {"phase": "UNKNOWN", "bias": 0, "score": 0.5, "events": []}

    closes = [float(c[4]) for c in candles]
    vols   = [float(c[5]) for c in candles]
    highs  = [float(c[2]) for c in candles]
    lows   = [float(c[3]) for c in candles]

    # Price trend (last 20 vs prev 20)
    recent_close = sum(closes[:20]) / 20
    old_close    = sum(closes[20:40]) / 20
    price_trend  = (recent_close - old_close) / (old_close or 1)

    # Volume trend
    recent_vol = sum(vols[:10]) / 10
    old_vol    = sum(vols[10:20]) / 10
    vol_expanding = recent_vol > old_vol * 1.15

    # Range check (tight range = accumulation/distribution)
    recent_range = max(highs[:20]) - min(lows[:20])
    prev_range   = max(highs[20:40]) - min(lows[20:40])
    tight_range  = recent_range < prev_range * 0.7

    # Spring detection (price dipped below range then recovered)
    range_lo     = min(lows[5:20])
    range_hi     = max(highs[5:20])
    latest_lo    = min(lows[:3])
    latest_close = closes[0]
    spring       = latest_lo < range_lo * 0.998 and latest_close > range_lo

    # Upthrust detection
    latest_hi    = max(highs[:3])
    upthrust     = latest_hi > range_hi * 1.002 and latest_close < range_hi

    events = []
    if spring:  events.append("Spring")
    if upthrust:events.append("Upthrust")

    # Phase classification
    if price_trend > 0.03 and vol_expanding:
        phase = "MARKUP";       bias = 1
    elif price_trend < -0.03 and vol_expanding:
        phase = "MARKDOWN";     bias = -1
    elif tight_range and spring:
        phase = "ACCUMULATION"; bias = 1
    elif tight_range and upthrust:
        phase = "DISTRIBUTION"; bias = -1
    elif tight_range:
        phase = "ACCUMULATION" if price_trend >= 0 else "DISTRIBUTION"
        bias  = 1 if price_trend >= 0 else -1
    else:
        phase = "UNKNOWN"; bias = 0

    score = min(abs(price_trend) * 10, 1.0)
    return {"phase": phase, "bias": bias, "score": round(score, 3), "events": events}


# ══════════════════════════════════════════════════════════════════
# GANN SQUARE OF 9
# ══════════════════════════════════════════════════════════════════

def _gann_sq9(price: float) -> Dict:
    """Gann Square of 9 key levels."""
    if price <= 0:
        return {"nearest": price, "resistances": [], "supports": [], "bias": 0}
    root = math.sqrt(price)
    lvls = sorted(set(
        round((root + s) ** 2, 2)
        for s in [-2, -1.5, -1, -0.75, -0.5, -0.25,
                   0,  0.25, 0.5, 0.75, 1, 1.5, 2]
        if round((root + s) ** 2, 2) > 0
    ))
    nearest   = min(lvls, key=lambda x: abs(x - price)) if lvls else price
    dist      = (nearest - price) / price if price else 0
    res_lvls  = [v for v in lvls if v > price * 1.002][:3]
    sup_lvls  = sorted([v for v in lvls if v < price * 0.998], reverse=True)[:3]
    bias      = 1 if dist > 0 else -1 if dist < 0 else 0

    return {
        "nearest":     round(nearest, 4),
        "distance_pct":round(dist * 100, 3),
        "resistances": res_lvls,
        "supports":    sup_lvls,
        "bias":        bias,
    }


# ══════════════════════════════════════════════════════════════════
# WILLIAM %R
# ══════════════════════════════════════════════════════════════════

def _william_r(candles: List, period: int = 14) -> Dict:
    """Williams %R — key reversal indicator."""
    if len(candles) < period:
        return {"value": -50, "oversold": False, "overbought": False}
    highs  = [float(c[2]) for c in candles[:period]]
    lows   = [float(c[3]) for c in candles[:period]]
    close  = float(candles[0][4])
    hi_max = max(highs); lo_min = min(lows)
    rng    = hi_max - lo_min
    if rng <= 0:
        return {"value": -50, "oversold": False, "overbought": False}
    wr = -100 * (hi_max - close) / rng
    return {
        "value":      round(wr, 2),
        "oversold":   wr < -80,    # < -80 = potential LONG reversal
        "overbought": wr > -20,    # > -20 = potential SHORT reversal
        "bias":       1 if wr < -80 else -1 if wr > -20 else 0,
    }


# ══════════════════════════════════════════════════════════════════
# EMA TREND
# ══════════════════════════════════════════════════════════════════

def _ema_trend(candles: List) -> Dict:
    """EMA 20/50/200 alignment."""
    if len(candles) < 50:
        return {"bull": False, "bear": False, "score": 0}
    closes = [float(c[4]) for c in candles]

    def ema(p):
        k = 2 / (p + 1); e = closes[-1]
        for c in reversed(closes[:-1]): e = c * k + e * (1 - k)
        return e

    price = closes[0]
    e20   = ema(20); e50 = ema(50)
    e200  = ema(200) if len(closes) >= 200 else ema(len(closes) // 2)

    if price > e20 > e50 > e200:
        return {"bull": True, "bear": False, "score": 1.0,
                "e20": e20, "e50": e50, "e200": e200}
    if price < e20 < e50 < e200:
        return {"bull": False, "bear": True, "score": -1.0,
                "e20": e20, "e50": e50, "e200": e200}
    if price > e20 > e50:
        return {"bull": True, "bear": False, "score": 0.6,
                "e20": e20, "e50": e50, "e200": e200}
    if price < e20 < e50:
        return {"bull": False, "bear": True, "score": -0.6,
                "e20": e20, "e50": e50, "e200": e200}
    return {"bull": False, "bear": False, "score": 0.0,
            "e20": e20, "e50": e50, "e200": e200}


# ══════════════════════════════════════════════════════════════════
# MACRO: USDT.D
# ══════════════════════════════════════════════════════════════════

async def _fetch_usdt_d() -> Dict:
    try:
        async with httpx.AsyncClient(timeout=12) as cl:
            r = await cl.get("https://api.coingecko.com/api/v3/global",
                             headers={"Accept": "application/json"})
            if r.status_code == 200:
                d   = r.json().get("data", {})
                dom = d.get("market_cap_percentage", {})
                ud  = dom.get("usdt", 7.0)
                bd  = dom.get("btc",  50.0)
                crypto_bias = ("BEAR" if ud > 7.5 else
                               "BULL" if ud < 6.0 else "NEUTRAL")
                return {"usdt_d": round(ud, 3), "btc_d": round(bd, 2),
                        "crypto_bias": crypto_bias, "ok": True}
    except Exception as e:
        logger.warning(f"[L1] USDT.D: {e}")
    return {"usdt_d": 7.0, "btc_d": 50.0, "crypto_bias": "NEUTRAL", "ok": False}


# ══════════════════════════════════════════════════════════════════
# LAYER 1 — MAIN ANALYSIS
# ══════════════════════════════════════════════════════════════════

async def run_layer1(db: Any = None) -> Dict:
    """
    Run Layer 1 Market Intelligence.
    Analyzes BTC + ETH on 4H + 1D.
    Stores result in Supabase for Layer 2 + Layer 3 to read.
    Returns market_context dict.
    """
    now = datetime.now(timezone.utc)
    logger.info(f"[L1] 🧠 Running Market Intelligence — {now.strftime('%H:%M UTC')}")

    # Fetch candles
    btc_4h, btc_1d, eth_4h, eth_1d = await asyncio.gather(
        _candles("BTC/USDT:USDT", "4h", 200),
        _candles("BTC/USDT:USDT", "1d",  60),
        _candles("ETH/USDT:USDT", "4h", 200),
        _candles("ETH/USDT:USDT", "1d",  60),
    )
    macro = await _fetch_usdt_d()

    context: Dict = {"ts": now.isoformat(), "ok": True}

    for sym, c4h, c1d in [("BTC", btc_4h, btc_1d), ("ETH", eth_4h, eth_1d)]:
        if not c4h or len(c4h) < 40:
            context[sym] = {"bias": "NEUTRAL", "phase": "UNKNOWN"}
            continue

        price   = float(c4h[-1][4]) if c4h else 0
        wyk     = _wyckoff_phase(list(reversed(c4h)))  # newest first
        gann    = _gann_sq9(price)
        wlr     = _william_r(list(reversed(c4h)), 14)
        ema     = _ema_trend(list(reversed(c4h)))

        # Aggregate score
        scores = [wyk["bias"], gann["bias"], wlr["bias"]]
        if ema["bull"]:  scores.append(1)
        elif ema["bear"]:scores.append(-1)
        avg = sum(scores) / len(scores) if scores else 0

        bias = "BULL" if avg > 0.25 else "BEAR" if avg < -0.25 else "NEUTRAL"

        context[sym] = {
            "price":    price,
            "bias":     bias,
            "score":    round(avg, 3),
            "wyckoff":  wyk,
            "gann":     gann,
            "william_r":wlr,
            "ema":      ema,
        }
        logger.info(f"[L1] {sym}: bias={bias} score={avg:.3f} "
                    f"wyckoff={wyk['phase']} wr={wlr['value']:.0f}")

        # Store in Supabase
        if db:
            try:
                await db.log_regime(
                    f"{sym}/USDT:USDT",
                    bias,
                    abs(round(avg, 3)),
                )
            except Exception as e:
                logger.warning(f"[L1] DB {sym}: {e}")

    context["macro"] = macro
    context["usdt_d_bias"] = macro.get("crypto_bias", "NEUTRAL")

    # Overall market verdict
    btc_b = context.get("BTC", {}).get("bias", "NEUTRAL")
    eth_b = context.get("ETH", {}).get("bias", "NEUTRAL")
    ud_b  = macro.get("crypto_bias", "NEUTRAL")

    bull_count = sum(1 for b in [btc_b, eth_b, ud_b] if b == "BULL")
    bear_count = sum(1 for b in [btc_b, eth_b, ud_b] if b == "BEAR")

    if bull_count >= 2:
        context["overall_bias"] = "BULL"
    elif bear_count >= 2:
        context["overall_bias"] = "BEAR"
    else:
        context["overall_bias"] = "NEUTRAL"

    context["scalp_allowed"] = context["overall_bias"] != "NEUTRAL"
    context["swing_allowed"] = True  # Swing always allowed

    # Cache globally
    _L1_CACHE["context"] = context
    _L1_CACHE["ts"]       = time.time()

    logger.info(
        f"[L1] ✅ Overall={context['overall_bias']} "
        f"BTC={btc_b} ETH={eth_b} USDT.D={ud_b} "
        f"ScalpOK={context['scalp_allowed']}"
    )
    return context


def get_cached_context() -> Optional[Dict]:
    """Get Layer 1 context (valid for 4 hours)."""
    ctx = _L1_CACHE.get("context")
    ts  = _L1_CACHE.get("ts", 0)
    if ctx and (time.time() - ts) < 14400:  # 4 hours
        return ctx
    return None

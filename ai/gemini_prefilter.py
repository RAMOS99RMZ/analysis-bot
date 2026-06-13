
"""
ai/gemini_prefilter.py — Ramos 360 Ai 🎖️
Gemini AI Pre-Filter: analyzes BTC + ETH 1D chart
before allowing ANY scalp signals.

Free tier: 15 req/min, ~1500/day
We cap at 10/hour to stay safely within limits.
"""
from __future__ import annotations
import time
from typing import Dict, Optional
import httpx
from loguru import logger
from config import Secrets

# ── Rate limiting ──────────────────────────────────────────────────────────
_CALL_LOG: list = []          # timestamps of calls
_MAX_PER_HOUR = 10            # safe limit for free tier
_CACHE: Dict   = {}           # cache result for 30 minutes
_CACHE_TTL     = 1800         # 30 minutes in seconds


def _can_call() -> bool:
    now = time.time()
    _CALL_LOG[:] = [t for t in _CALL_LOG if now - t < 3600]
    return len(_CALL_LOG) < _MAX_PER_HOUR


def _mark_call() -> None:
    _CALL_LOG.append(time.time())


def _get_cache() -> Optional[Dict]:
    if "result" in _CACHE and (time.time() - _CACHE.get("ts", 0)) < _CACHE_TTL:
        return _CACHE["result"]
    return None


def _set_cache(result: Dict) -> None:
    _CACHE["result"] = result
    _CACHE["ts"]     = time.time()


# ── Prompt builder ──────────────────────────────────────────────────────────

def _build_prompt(btc_price: float, eth_price: float,
                   btc_candles: list, eth_candles: list) -> str:
    """Build a concise prompt for Gemini."""

    def _summarize(candles: list, n: int = 7) -> str:
        if not candles or len(candles) < 2:
            return "No data"
        recent = candles[:n]
        closes = [float(c[4]) for c in recent]
        highs  = [float(c[2]) for c in recent]
        lows   = [float(c[3]) for c in recent]
        pct    = (closes[0] - closes[-1]) / closes[-1] * 100 if closes[-1] else 0
        return (f"Last {n} candles: "
                f"High={max(highs):,.0f} Low={min(lows):,.0f} "
                f"Close={closes[0]:,.0f} Change={pct:+.2f}%")

    btc_summary = _summarize(btc_candles)
    eth_summary = _summarize(eth_candles)

    return (
        "You are a professional crypto quant analyst. "
        "Analyze the following 1D chart data and answer with ONE word: YES or NO.\n\n"
        f"BTC/USDT current price: ${btc_price:,.0f}\n"
        f"BTC 1D summary: {btc_summary}\n\n"
        f"ETH/USDT current price: ${eth_price:,.0f}\n"
        f"ETH 1D summary: {eth_summary}\n\n"
        "Question: Is the current market condition suitable for short-term scalp "
        "trading (15min–2hr trades)? Consider overall trend, volatility, and "
        "whether the market is in a clear directional move (not choppy).\n\n"
        "Answer with ONE word only: YES or NO"
    )


# ── Main function ───────────────────────────────────────────────────────────

async def gemini_market_check(
    btc_price: float,
    eth_price: float,
    btc_candles: list,
    eth_candles: list,
) -> Dict:
    """
    Ask Gemini if the market is suitable for scalp trading.

    Returns:
        {
            "ok":      bool,   # True = proceed with scalp
            "answer":  str,    # "YES" | "NO" | "SKIP" | "CACHED"
            "reason":  str,
            "cached":  bool,
        }
    """
    # 1. Return cached result if fresh
    cached = _get_cache()
    if cached:
        logger.info(f"[GeminiPF] Using cached result: {cached['answer']} "
                    f"(expires in {int((_CACHE.get('ts',0)+_CACHE_TTL-time.time())/60)} min)")
        return {**cached, "cached": True}

    # 2. Check if Gemini key is available
    if not Secrets.has_gemini():
        logger.debug("[GeminiPF] No Gemini key — skipping pre-filter")
        return {"ok": True, "answer": "SKIP", "reason": "No API key", "cached": False}

    # 3. Rate limit check
    if not _can_call():
        logger.warning(f"[GeminiPF] Hourly quota reached ({_MAX_PER_HOUR}/hr) — allowing scalp")
        return {"ok": True, "answer": "SKIP", "reason": "Rate limit reached", "cached": False}

    # 4. Call Gemini
    try:
        prompt = _build_prompt(btc_price, eth_price, btc_candles, eth_candles)
        url    = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-1.5-flash:generateContent?key={Secrets.GEMINI_KEY}"
        )

        async with httpx.AsyncClient(timeout=15) as cl:
            r = await cl.post(url, json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 5, "temperature": 0},
            })

        _mark_call()

        if r.status_code == 200:
            txt = (r.json()
                     .get("candidates", [{}])[0]
                     .get("content", {})
                     .get("parts", [{}])[0]
                     .get("text", "SKIP")
                     .strip()
                     .upper())
            answer = "YES" if "YES" in txt else "NO" if "NO" in txt else "SKIP"
            ok     = answer != "NO"

            result = {
                "ok":     ok,
                "answer": answer,
                "reason": f"Gemini: {answer} (used {len(_CALL_LOG)}/{_MAX_PER_HOUR}/hr)",
                "cached": False,
            }
            _set_cache(result)
            logger.info(f"[GeminiPF] Answer={answer} BTC=${btc_price:,.0f}")
            return result

        if r.status_code == 429:
            logger.warning("[GeminiPF] 429 rate limited — allowing scalp")
            return {"ok": True, "answer": "SKIP", "reason": "429 from Gemini", "cached": False}

        logger.warning(f"[GeminiPF] HTTP {r.status_code}")

    except Exception as e:
        logger.warning(f"[GeminiPF] Error: {e}")

    # Fallback: allow if error
    return {"ok": True, "answer": "SKIP", "reason": "API error — allowing", "cached": False}

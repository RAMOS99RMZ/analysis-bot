# strategies/expert_01_classic_ta.py — E1: ClassicTA
# RSI, MACD, Bollinger Bands, ATR, SuperTrend, Heikin Ashi

from __future__ import annotations
from typing import Dict, Optional
from engine.indicator_engine import IndicatorEngine

IE = IndicatorEngine()

def analyze(data: Dict) -> Optional[Dict]:
    """
    E1 — Classic Technical Analysis.
    Returns {long, short, why, name}
    """
    try:
        c5m = data.get("c5m", [])
        c1h = data.get("c1h", [])
        c4h = data.get("c4h", [])
        if not c5m or len(c5m) < 20:
            return None

        price = float(c5m[0][4])

        # ── 5M indicators ────────────────────────────────────────────────────
        rsi5   = IE.rsi(c5m, 14)
        macd5  = IE.macd(c5m)
        bb5    = IE.bbands(c5m, 20)
        psar5  = IE.psar(c5m)
        wr5    = IE.williams_r(c5m, 14)
        sr5    = IE.stoch_rsi(c5m)
        atr5   = IE.atr(c5m, 14)

        # ── 1H indicators ────────────────────────────────────────────────────
        st1h   = IE.supertrend(c1h, 3) if c1h and len(c1h) >= 14 else None
        ha1h   = IE.heikin_ashi(c1h)   if c1h and len(c1h) >= 4  else None

        # ── 4H indicators ────────────────────────────────────────────────────
        ichi4h = IE.ichimoku(c4h)      if c4h and len(c4h) >= 55 else None
        adx4h  = IE.adx(c4h, 14)      if c4h and len(c4h) >= 28 else None

        long_s  = 0.0
        short_s = 0.0
        why: Dict = {}

        # ── RSI scoring ──────────────────────────────────────────────────────
        if rsi5 < 30:
            long_s += 0.30; why["rsi"] = f"RSI oversold {rsi5:.1f}"
        elif rsi5 < 42:
            long_s += 0.15; why["rsi"] = f"RSI weak {rsi5:.1f}"
        elif rsi5 > 70:
            short_s += 0.30; why["rsi"] = f"RSI overbought {rsi5:.1f}"
        elif rsi5 > 58:
            short_s += 0.15

        # ── MACD scoring ─────────────────────────────────────────────────────
        if macd5["hist"] > 0 and macd5["macd"] > 0:
            long_s += 0.25; why["macd"] = "MACD bullish"
        elif macd5["hist"] < 0 and macd5["macd"] < 0:
            short_s += 0.25; why["macd"] = "MACD bearish"
        elif macd5["hist"] > 0:
            long_s += 0.12
        elif macd5["hist"] < 0:
            short_s += 0.12

        # ── Bollinger Bands ──────────────────────────────────────────────────
        if bb5["upper"] > 0:
            if price <= bb5["lower"] * 1.002:
                long_s += 0.20; why["bb"] = "Price at lower BB"
            elif price >= bb5["upper"] * 0.998:
                short_s += 0.20; why["bb"] = "Price at upper BB"

        # ── PSAR ─────────────────────────────────────────────────────────────
        if psar5:
            if psar5["bull"]:   long_s  += 0.12 + (0.08 if psar5["flip"] else 0)
            else:               short_s += 0.12 + (0.08 if psar5["flip"] else 0)

        # ── Williams %R ──────────────────────────────────────────────────────
        if wr5 <= -80:   long_s  += 0.10; why["wr"] = f"W%R oversold {wr5:.0f}"
        elif wr5 >= -20: short_s += 0.10; why["wr"] = f"W%R overbought {wr5:.0f}"

        # ── StochRSI ─────────────────────────────────────────────────────────
        if sr5:
            if sr5["bull_cross"]: long_s  += 0.15; why["sr"] = "StochRSI bull cross"
            elif sr5["oversold"]: long_s  += 0.08
            if sr5["bear_cross"]: short_s += 0.15; why["sr"] = "StochRSI bear cross"
            elif sr5["overbought"]: short_s += 0.08

        # ── 1H SuperTrend ────────────────────────────────────────────────────
        if st1h:
            if st1h["bull_flip"]: long_s  += 0.20; why["st1h"] = "ST1H bull flip"
            elif st1h["bull"]:    long_s  += 0.12
            if st1h["bear_flip"]: short_s += 0.20; why["st1h"] = "ST1H bear flip"
            elif st1h["bear"]:    short_s += 0.12

        # ── 1H Heikin Ashi ───────────────────────────────────────────────────
        if ha1h:
            if ha1h["bull_strong"]: long_s  += 0.15
            elif ha1h["bull"]:      long_s  += 0.08
            if ha1h["bear_strong"]: short_s += 0.15
            elif ha1h["bear"]:      short_s += 0.08

        # ── 4H Ichimoku ──────────────────────────────────────────────────────
        if ichi4h:
            if ichi4h["above_cloud"] and ichi4h["bull_cross"]:
                long_s += 0.22; why["ichi"] = "Ichimoku bull"
            elif ichi4h["above_cloud"]:
                long_s += 0.10
            if ichi4h["below_cloud"] and not ichi4h["bull_cross"]:
                short_s += 0.22; why["ichi"] = "Ichimoku bear"
            elif ichi4h["below_cloud"]:
                short_s += 0.10

        # ── 4H ADX ───────────────────────────────────────────────────────────
        if adx4h:
            if adx4h["bullish"]:  long_s  += 0.12
            elif adx4h["ranging"]: long_s *= 0.8; short_s *= 0.8

        long_s  = round(min(long_s,  1.0), 4)
        short_s = round(min(short_s, 1.0), 4)

        return {"name": "ClassicTA", "long": long_s, "short": short_s, "why": why}
    except Exception as e:
        return {"name": "ClassicTA", "long": 0, "short": 0, "why": {"err": str(e)}}

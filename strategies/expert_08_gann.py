# strategies/expert_08_gann.py — E8: Gann Angles + Lunar Cycle
from __future__ import annotations
import math, time
from typing import Dict, Optional
from datetime import datetime, timezone

# Known New Moon reference: Jan 29 2025 12:36 UTC (Unix ms)
_NEW_MOON_REF_MS = 1738152960000
_LUNAR_CYCLE     = 29.53058867 * 86400 * 1000  # ms

def _calc_lunar_phase() -> Dict:
    now_ms = int(time.time() * 1000)
    elapsed = (now_ms - _NEW_MOON_REF_MS) % _LUNAR_CYCLE
    days_into = elapsed / 86400000
    angle = (elapsed / _LUNAR_CYCLE) * 360
    if days_into < 1.5:   phase, bias, strength, emoji = "NEW_MOON",    "BULL", 0.85, "🌑"
    elif days_into < 7.0: phase, bias, strength, emoji = "WAXING_C",    "BULL", 0.60, "🌒"
    elif days_into < 8.5: phase, bias, strength, emoji = "FIRST_Q",     "BULL", 0.70, "🌓"
    elif days_into < 14.0:phase, bias, strength, emoji = "WAXING_G",    "BULL", 0.55, "🌔"
    elif days_into < 16.0:phase, bias, strength, emoji = "FULL_MOON",   "BEAR", 0.85, "🌕"
    elif days_into < 21.5:phase, bias, strength, emoji = "WANING_G",    "BEAR", 0.60, "🌖"
    elif days_into < 23.0:phase, bias, strength, emoji = "LAST_Q",      "BEAR", 0.70, "🌗"
    else:                 phase, bias, strength, emoji = "WANING_C",    "BEAR", 0.45, "🌘"
    return {"phase": phase, "bias": bias, "strength": strength,
            "days_into": round(days_into, 2), "angle": round(angle, 1), "emoji": emoji}

def _gann_sq9(price: float) -> Dict:
    """Square of 9 key levels around current price."""
    try:
        root = math.sqrt(price)
        levels = []
        for step in [-0.5, -0.25, 0, 0.25, 0.5, 1.0]:
            lvl = (root + step) ** 2
            levels.append(round(lvl, 4))
        return {"levels": levels, "root": round(root, 4)}
    except:
        return {"levels": [], "root": 0}

def analyze(data: Dict) -> Optional[Dict]:
    try:
        c4h = data.get("c4h", []); c1h = data.get("c1h", [])
        if not c4h: return None
        price  = float(c4h[0][4])
        lunar  = _calc_lunar_phase()
        sq9    = _gann_sq9(price)
        long_s = short_s = 0.0; why = {}

        why["lunar"] = f"{lunar['emoji']} {lunar['phase']} day={lunar['days_into']}"
        days = lunar["days_into"]

        # Lunar bias scoring
        if lunar["phase"] == "NEW_MOON":
            long_s += 0.55; why["gann"] = "Gann: New Moon — start of bull cycle"
        elif lunar["phase"] == "FIRST_Q":
            long_s += 0.35; why["gann"] = "First Quarter — confirm trend"
        elif lunar["phase"] == "WAXING_G":
            long_s += 0.25
        elif lunar["phase"] == "FULL_MOON":
            short_s += 0.55; why["gann"] = "Gann: Full Moon — reversal zone 🔻"
        elif lunar["phase"] == "LAST_Q":
            short_s += 0.35; why["gann"] = "Last Quarter — secondary short"
        elif lunar["phase"] == "WANING_G":
            short_s += 0.22

        # Critical window ±1.75 days → boost
        crit_days = [1.0, 7.5, 14.77, 22.0]
        is_crit   = any(abs(days - cd) <= 1.75 for cd in crit_days)
        if is_crit:
            long_s  *= 1.30; short_s *= 1.30
            why["crit"] = "⭐ Critical Gann window"

        # Square of 9 proximity
        for lvl in sq9["levels"]:
            if lvl > 0 and abs(price - lvl) / price < 0.003:
                if price < lvl: long_s  += 0.20; why["sq9"] = f"Sq9 support {lvl}"
                else:           short_s += 0.15; why["sq9"] = f"Sq9 resistance {lvl}"
                break

        return {"name":"Gann","long":round(min(long_s,1.0),4),"short":round(min(short_s,1.0),4),"why":why}
    except Exception as e:
        return {"name":"Gann","long":0,"short":0,"why":{"err":str(e)}}

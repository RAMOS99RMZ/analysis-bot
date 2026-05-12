"""
strategies/expert_14_ls_ratio.py — E14: Long/Short Ratio Contrarian
Ported from GAS expert14_LSContrarian (v100).
L/S > 0.78 → longs crowded → SHORT contrarian.
L/S < 0.25 → shorts crowded → LONG contrarian.
"""
from __future__ import annotations
from typing import Dict, Optional


def analyze(data: Dict) -> Optional[Dict]:
    """E14 — Long/Short Ratio Contrarian."""
    try:
        ls      = data.get("ls_ratio")
        funding = data.get("funding", {})
        fr      = funding.get("rate", 0.0)

        if ls is None:
            return {"name": "LSRatio", "long": 0.0, "short": 0.0,
                    "why": {"na": "no L/S data"}}

        long_s = short_s = 0.0
        why: Dict = {"ls_pct": round(ls * 100, 1), "fr_bps": round(fr * 10000, 2)}

        if ls > 0.78:
            short_s += 0.42
            why["signal"] = f"L/S {ls*100:.0f}% — longs crowded → Short Contrarian"
            if fr > 0.0006:
                short_s += 0.18
                why["synergy"] = "Funding+ confirms long crowding"
        elif ls > 0.70:
            short_s += 0.22

        if ls < 0.25:
            long_s += 0.42
            why["signal"] = f"L/S {ls*100:.0f}% — shorts crowded → Long Contrarian"
            if fr < -0.0004:
                long_s += 0.18
                why["synergy"] = "Funding- confirms short crowding"
        elif ls < 0.32:
            long_s += 0.22

        return {
            "name":  "LSRatio",
            "long":  round(min(long_s,  1.0), 4),
            "short": round(min(short_s, 1.0), 4),
            "why":   why,
        }
    except Exception as e:
        return {"name": "LSRatio", "long": 0.0, "short": 0.0,
                "why": {"error": str(e)}}

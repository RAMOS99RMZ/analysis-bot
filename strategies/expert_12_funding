"""
strategies/expert_12_funding.py — E12: Funding Rate Extreme
Ported from GAS expert12_FundingExtreme (v100).
When funding is extremely positive → longs crowded → SHORT contrarian.
When funding is extremely negative → shorts crowded → LONG contrarian.
"""
from __future__ import annotations
from typing import Dict, Optional


def analyze(data: Dict) -> Optional[Dict]:
    """
    E12 — Funding Rate Extreme.
    Returns: {long, short, why}  scores in [0, 1]
    """
    try:
        funding = data.get("funding", {})
        fr      = funding.get("rate", 0.0)

        long_s = short_s = 0.0
        why: Dict = {"fr_bps": round(fr * 10000, 2)}

        # Extreme thresholds (mirror of GAS)
        if fr > 0.0010:
            short_s += 0.40
            why["signal"] = f"Funding EXTREME+ {fr*10000:.1f}bps — longs crowded → SHORT"
        elif fr > 0.0006:
            short_s += 0.22
            why["signal"] = f"Funding HIGH+ {fr*10000:.1f}bps"
        elif fr < -0.0006:
            long_s += 0.40
            why["signal"] = f"Funding EXTREME- {fr*10000:.1f}bps — shorts crowded → LONG"
        elif fr < -0.0003:
            long_s += 0.22
            why["signal"] = f"Funding LOW- {fr*10000:.1f}bps"

        # Sustained extreme check (if available)
        fh = data.get("funding_history", {})
        if fh:
            if fh.get("all_positive") and fh.get("avg", 0) > 0.0006:
                short_s += 0.18
                why["sustained"] = "Funding+ sustained — squeeze incoming"
            if fh.get("all_negative") and fh.get("avg", 0) < -0.0004:
                long_s += 0.18
                why["sustained"] = "Funding- sustained — squeeze incoming"

        return {
            "name":  "FundingExtreme",
            "long":  round(min(long_s,  1.0), 4),
            "short": round(min(short_s, 1.0), 4),
            "why":   why,
        }
    except Exception as e:
        return {"name": "FundingExtreme", "long": 0.0, "short": 0.0,
                "why": {"error": str(e)}}

"""
strategies/expert_13_oi_delta.py — E13: Open Interest Delta
Ported from GAS expert13_OIDelta (v100).
OI rising + price rising = real trend.
OI rising + price falling = shorts entering.
"""
from __future__ import annotations
from typing import Dict, List, Optional


def analyze(data: Dict) -> Optional[Dict]:
    """E13 — Open Interest Delta."""
    try:
        oi_data = data.get("oi_data")
        c1h     = data.get("c1h", [])

        if not oi_data:
            return {"name": "OIDelta", "long": 0.0, "short": 0.0,
                    "why": {"na": "no OI data"}}

        slope = oi_data.get("slope", 0.0)
        long_s = short_s = 0.0
        why: Dict = {"oi_slope_pct": round(slope * 100, 2)}

        if c1h and len(c1h) >= 4:
            p_now = float(c1h[0][4])
            p_old = float(c1h[3][4])
            p_slope = (p_now - p_old) / (p_old or 1)

            oi_up = slope > 0.04
            oi_dn = slope < -0.04
            p_up  = p_slope > 0.005
            p_dn  = p_slope < -0.005

            if oi_up and p_up:
                long_s += 0.32
                why["signal"] = "OI↑ + Price↑ = real bullish trend"
            elif oi_up and p_dn:
                short_s += 0.32
                why["signal"] = "OI↑ + Price↓ = new shorts entering"
            elif oi_dn and p_up:
                long_s += 0.18
                why["signal"] = "OI↓ + Price↑ = short squeeze (tactical)"
            elif oi_dn and p_dn:
                long_s += 0.22
                why["signal"] = "OI↓ + Price↓ = long liquidation → possible bottom"

        return {
            "name":  "OIDelta",
            "long":  round(min(long_s,  1.0), 4),
            "short": round(min(short_s, 1.0), 4),
            "why":   why,
        }
    except Exception as e:
        return {"name": "OIDelta", "long": 0.0, "short": 0.0,
                "why": {"error": str(e)}}

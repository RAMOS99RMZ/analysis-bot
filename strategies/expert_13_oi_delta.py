
"""
strategies/expert_13_oi_delta.py — E13: Open Interest Delta (FIXED)
✅ إصلاح: يعمل بدون oi_data أيضاً (fallback لـ c4h)
OI rising + price rising  = real trend (confirm)
OI rising + price falling = shorts entering (caution)
"""
from __future__ import annotations
from typing import Dict, List, Optional


def analyze(data: Dict) -> Optional[Dict]:
    try:
        oi = data.get("oi_data", {})
        c4h = data.get("c4h", [])

        long_s = short_s = 0.0
        why: Dict = {}

        # ── بيانات OI حقيقية من OKX ──────────────────────────────────
        if oi and oi.get("oi_now") and oi.get("oi_prev"):
            oi_now  = float(oi["oi_now"])
            oi_prev = float(oi["oi_prev"])
            oi_delta_pct = (oi_now - oi_prev) / max(oi_prev, 1) * 100

            why["oi_delta_pct"] = round(oi_delta_pct, 2)

            if c4h and len(c4h) >= 2:
                price_now  = float(c4h[0][4])
                price_prev = float(c4h[1][4])
                price_up   = price_now > price_prev

                if oi_delta_pct > 2.0 and price_up:
                    long_s += 0.40
                    why["signal"] = "OI↑ + Price↑ = Real Trend LONG"
                elif oi_delta_pct > 2.0 and not price_up:
                    short_s += 0.40
                    why["signal"] = "OI↑ + Price↓ = Shorts entering"
                elif oi_delta_pct < -2.0 and price_up:
                    long_s += 0.20
                    why["signal"] = "OI↓ + Price↑ = Shorts exiting (squeeze)"
                elif oi_delta_pct < -2.0 and not price_up:
                    short_s += 0.20
                    why["signal"] = "OI↓ + Price↓ = Longs exiting"
                else:
                    why["signal"] = f"OI delta {oi_delta_pct:.1f}% — neutral"
            else:
                if oi_delta_pct > 3.0:
                    long_s += 0.20
                    why["signal"] = f"OI rising {oi_delta_pct:.1f}%"
                elif oi_delta_pct < -3.0:
                    short_s += 0.20
                    why["signal"] = f"OI falling {oi_delta_pct:.1f}%"

        # ── Fallback: volume proxy إذا لا يوجد OI ────────────────────
        elif c4h and len(c4h) >= 6:
            vols   = [float(c[5]) for c in c4h[:6]]
            closes = [float(c[4]) for c in c4h[:6]]
            avg_vol   = sum(vols[1:]) / max(len(vols[1:]), 1)
            curr_vol  = vols[0]
            price_up  = closes[0] > closes[1]
            vol_surge = curr_vol > avg_vol * 1.5

            if vol_surge and price_up:
                long_s  += 0.20
                why["signal"] = "Volume surge + Price up (OI proxy)"
            elif vol_surge and not price_up:
                short_s += 0.20
                why["signal"] = "Volume surge + Price down (OI proxy)"
            else:
                why["signal"] = "No OI data — volume neutral"
        else:
            why["signal"] = "No OI or candle data"

        return {
            "name":  "OIDelta",
            "long":  round(min(long_s,  1.0), 4),
            "short": round(min(short_s, 1.0), 4),
            "why":   why,
        }
    except Exception as e:
        return {"name": "OIDelta", "long": 0.0, "short": 0.0,
                "why": {"error": str(e)}}

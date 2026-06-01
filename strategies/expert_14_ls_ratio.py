
"""
strategies/expert_14_ls_ratio.py — E14: Long/Short Ratio Contrarian (FIXED)
✅ إصلاح: "rate" → "funding_rate" + fallback إذا لا يوجد ls_ratio
L/S > 0.78 → longs crowded → SHORT contrarian
L/S < 0.25 → shorts crowded → LONG contrarian
"""
from __future__ import annotations
from typing import Dict, Optional


def analyze(data: Dict) -> Optional[Dict]:
    try:
        ls  = data.get("ls_ratio", {})
        funding = data.get("funding", {})

        long_s = short_s = 0.0
        why: Dict = {}

        # ── L/S Ratio من OKX ─────────────────────────────────────────
        if ls and ls.get("long_ratio") is not None:
            lr = float(ls["long_ratio"])
            sr = float(ls.get("short_ratio", 1.0 - lr))
            why["long_ratio"]  = round(lr, 4)
            why["short_ratio"] = round(sr, 4)

            if lr > 0.78:
                short_s += 0.45
                why["signal"] = f"L/S {lr:.2f} — Longs CROWDED → SHORT contrarian"
            elif lr > 0.68:
                short_s += 0.25
                why["signal"] = f"L/S {lr:.2f} — Longs elevated"
            elif lr < 0.25:
                long_s += 0.45
                why["signal"] = f"L/S {lr:.2f} — Shorts CROWDED → LONG contrarian"
            elif lr < 0.38:
                long_s += 0.25
                why["signal"] = f"L/S {lr:.2f} — Shorts elevated"
            else:
                why["signal"] = f"L/S {lr:.2f} — Balanced"

        # ── Fallback: Funding Rate كـ proxy ──────────────────────────
        else:
            # ✅ الإصلاح: "funding_rate" وليس "rate"
            fr = float(funding.get("funding_rate",
                       funding.get("rate", 0.0)) or 0.0)
            why["funding_proxy"] = round(fr * 10000, 2)

            if fr > 0.0008:
                short_s += 0.20
                why["signal"] = f"No L/S — Funding proxy HIGH: {fr*10000:.1f}bps → SHORT"
            elif fr < -0.0005:
                long_s += 0.20
                why["signal"] = f"No L/S — Funding proxy LOW: {fr*10000:.1f}bps → LONG"
            else:
                why["signal"] = "No L/S data — neutral"

        return {
            "name":  "LSContrarian",
            "long":  round(min(long_s,  1.0), 4),
            "short": round(min(short_s, 1.0), 4),
            "why":   why,
        }
    except Exception as e:
        return {"name": "LSContrarian", "long": 0.0, "short": 0.0,
                "why": {"error": str(e)}}

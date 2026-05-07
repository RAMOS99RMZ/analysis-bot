# strategies/expert_05_onchain.py — E5: On-Chain (Funding Rate + OI proxy)
from __future__ import annotations
from typing import Dict, Optional

def analyze(data: Dict) -> Optional[Dict]:
    try:
        funding = data.get("funding", {})
        book    = data.get("book_imbalance", {})
        long_s = short_s = 0.0; why = {}
        fr = funding.get("funding_rate", 0)
        # Funding Rate contrarian signal (negative = bullish, positive = bearish)
        if fr < -0.0003: long_s  += 0.45; why["fr"] = f"Extreme negative funding {fr:.4f} 🟢"
        elif fr < -0.0001: long_s += 0.25; why["fr"] = f"Negative funding {fr:.4f}"
        elif fr > 0.0010: short_s += 0.45; why["fr"] = f"Extreme positive funding {fr:.4f} 🔴"
        elif fr > 0.0005: short_s += 0.25; why["fr"] = f"High funding {fr:.4f}"
        # Order book imbalance
        if book:
            ratio = book.get("ratio", 0.5)
            if ratio > 0.68:   long_s  += 0.30; why["obi"] = f"OBI bull {ratio:.2f}"
            elif ratio > 0.60: long_s  += 0.15
            elif ratio < 0.32: short_s += 0.30; why["obi"] = f"OBI bear {ratio:.2f}"
            elif ratio < 0.40: short_s += 0.15
        return {"name":"OnChain","long":round(min(long_s,1.0),4),"short":round(min(short_s,1.0),4),"why":why}
    except Exception as e:
        return {"name":"OnChain","long":0,"short":0,"why":{"err":str(e)}}

# strategies/expert_11_usdt.py — E11: USDT Dominance
# USDT.D rising = risk-off (short crypto), falling = risk-on (long crypto)
from __future__ import annotations
from typing import Dict, Optional
from engine.indicator_engine import IndicatorEngine
IE = IndicatorEngine()

def analyze(data: Dict) -> Optional[Dict]:
    try:
        usdt_d = data.get("usdt_dominance", {})
        c4h    = data.get("c4h", [])
        long_s = short_s = 0.0; why = {}

        # USDT.D data (fetched externally and passed in)
        trend  = usdt_d.get("trend", "UNKNOWN")
        value  = usdt_d.get("value", 5.0)
        rising = usdt_d.get("rising", False)
        falling= usdt_d.get("falling", False)

        if falling:
            long_s  += 0.45; why["usdt_d"] = f"USDT.D falling ↓ {value:.2f}% — risk ON 🟢"
        elif trend == "BEAR":
            long_s  += 0.25; why["usdt_d"] = f"USDT.D bearish trend"
        if rising:
            short_s += 0.45; why["usdt_d"] = f"USDT.D rising ↑ {value:.2f}% — risk OFF 🔴"
        elif trend == "BULL":
            short_s += 0.25

        # Extreme levels
        if value < 4.0:   long_s  += 0.20; why["usdt_ext"] = "USDT.D extreme low — max risk on"
        elif value > 8.0: short_s += 0.20; why["usdt_ext"] = "USDT.D extreme high — max risk off"

        # If no USDT.D data, use BTC 4H dominance proxy (BTC vs alts)
        if not usdt_d and c4h and len(c4h) >= 20:
            trend4 = IE.get_trend(c4h)
            if trend4 == "BULL":   long_s  += 0.20; why["proxy"] = "BTC 4H proxy bull"
            elif trend4 == "BEAR": short_s += 0.20; why["proxy"] = "BTC 4H proxy bear"

        return {"name":"USDT","long":round(min(long_s,1.0),4),"short":round(min(short_s,1.0),4),"why":why}
    except Exception as e:
        return {"name":"USDT","long":0,"short":0,"why":{"err":str(e)}}

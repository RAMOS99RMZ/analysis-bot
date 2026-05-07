# strategies/expert_07_fear_greed.py — E7: Fear & Greed Index
from __future__ import annotations
from typing import Dict, Optional

def analyze(data: Dict) -> Optional[Dict]:
    try:
        fg = data.get("fear_greed", {"value": 50})
        val = fg.get("value", 50)
        long_s = short_s = 0.0; why = {"fg_value": val, "fg_label": fg.get("label","?")}

        # Contrarian: extreme fear = buy, extreme greed = sell
        if val < 15:   long_s = 0.80; why["signal"] = "🩸 Extreme Fear — contrarian LONG"
        elif val < 25: long_s = 0.55; why["signal"] = "😨 Fear — LONG bias"
        elif val < 40: long_s = 0.30
        elif val < 60: long_s = 0.10; short_s = 0.10  # neutral
        elif val < 75: short_s = 0.30; why["signal"] = "😏 Greed — SHORT bias"
        elif val < 85: short_s = 0.50; why["signal"] = "🤑 High Greed — SHORT"
        else:          short_s = 0.75; why["signal"] = "🔥 Extreme Greed — strong SHORT"

        return {"name":"FearGreed","long":round(min(long_s,1.0),4),"short":round(min(short_s,1.0),4),"why":why}
    except Exception as e:
        return {"name":"FearGreed","long":0,"short":0,"why":{"err":str(e)}}

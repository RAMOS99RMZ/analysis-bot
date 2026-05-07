# strategies/expert_03_wyckoff.py — E3: Wyckoff

from __future__ import annotations
from typing import Dict, Optional, List

def detect_wyckoff_phase(candles: List) -> Dict:
    try:
        if not candles or len(candles) < 25:
            return {"phase": "UNKNOWN", "signal": 0, "events": [], "bias": 0}
        n = min(40, len(candles) - 1)
        closes = [float(candles[i][4]) for i in range(1, n+1)]
        vols   = [float(candles[i][5] or 0) for i in range(1, n+1)]
        highs  = [float(candles[i][2]) for i in range(1, n+1)]
        lows   = [float(candles[i][3]) for i in range(1, n+1)]
        if len(closes) < 15:
            return {"phase": "UNKNOWN", "signal": 0, "events": [], "bias": 0}
        avg_v  = sum(vols) / len(vols)
        rv5    = sum(vols[:5]) / 5 if len(vols) >= 5 else avg_v
        vol_exp  = rv5 > avg_v * 1.3
        vol_con  = rv5 < avg_v * 0.7
        vol_3x   = vols[0] > avg_v * 3
        vol_2x   = vols[0] > avg_v * 2
        rH = max(highs[:10]); rL = min(lows[:10])
        pH = max(highs[10:25]) if len(highs) > 10 else rH
        pL = min(lows[10:25])  if len(lows)  > 10 else rL
        rising = closes[0] > closes[-1]
        falling= closes[0] < closes[-1]
        side   = abs(closes[0] - closes[-1]) / (closes[-1] or 1) < 0.025
        range_w= (max(highs) - min(lows)) / (lows[0] or 1)
        phase = "UNKNOWN"; score = 0.0; events = []; bias = 0
        if not rising and side and range_w < 0.12:
            phase = "ACCUMULATION"; score = 0.55; bias = 1
            if vol_3x and falling and closes[0] > lows[0]: events.append("SC"); score += 0.20
            if abs(rL - pL) / (pL or 1) < 0.02 and vol_con: events.append("ST"); score += 0.15
            if len(candles) >= 5:
                sL = float(candles[1][3]); sC = float(candles[1][4])
                supp = min(lows[2:12]) if len(lows) >= 12 else rL
                if sL < supp * 0.999 and sC > supp and float(candles[0][4]) > sC:
                    events.append("SPRING"); score += 0.35
            if not vol_exp and rL > pL * 1.001: events.append("LPS"); score += 0.12
            if rH > pH * 1.01 and vol_exp: events.append("SOS"); score += 0.30
        elif rising and vol_exp and rH > pH * 1.005:
            phase = "MARKUP"; score = 0.50; bias = 1; events.append("MARKUP")
            if vol_2x: score += 0.15
        elif rising and side and rH <= pH * 1.02:
            phase = "DISTRIBUTION"; score = 0.55; bias = -1
            if vol_3x and rising and closes[0] < highs[0]: events.append("BC"); score += 0.20
            if vol_2x and rising: events.append("PSY"); score += 0.10
            if len(candles) >= 5:
                utH = float(candles[1][2]); utC = float(candles[1][4])
                res  = max(highs[2:12]) if len(highs) >= 12 else rH
                if utH > res * 1.001 and utC < res and float(candles[0][4]) < utC:
                    events.append("UPTHRUST"); score += 0.35
            if not vol_exp and rH < pH * 0.999: events.append("LPSY"); score += 0.12
            if rL < pL * 0.99 and vol_exp: events.append("SOW"); score += 0.30
        elif falling and vol_exp and rL < pL * 0.995:
            phase = "MARKDOWN"; score = 0.50; bias = -1; events.append("MARKDOWN")
        signal = round(score * bias, 3)
        return {"phase": phase, "signal": signal, "score": score, "events": events, "bias": bias}
    except Exception as e:
        return {"phase": "UNKNOWN", "signal": 0, "score": 0, "events": [], "bias": 0}

def analyze(data: Dict) -> Optional[Dict]:
    try:
        c4h = data.get("c4h", []); c1h = data.get("c1h", [])
        if not c4h or len(c4h) < 30: return None
        w4 = detect_wyckoff_phase(c4h)
        w1 = detect_wyckoff_phase(c1h) if c1h and len(c1h) >= 30 else None
        long_s = short_s = 0.0; why = {}
        if w4["bias"] == 1:
            long_s += w4["score"] * 0.70
            why["wyc4h"] = f"4H:{w4['phase']} {w4['events']}"
            if "SPRING" in w4["events"]: long_s += 0.20; why["spring"] = "🌱 Spring!"
        elif w4["bias"] == -1:
            short_s += w4["score"] * 0.70
            why["wyc4h"] = f"4H:{w4['phase']}"
            if "UPTHRUST" in w4["events"]: short_s += 0.20
        if w1 and w1["phase"] == w4["phase"] and w4["bias"] != 0:
            if w4["bias"] == 1: long_s += 0.15
            else: short_s += 0.15
            why["sync"] = "Wyckoff 1H+4H sync ⭐"
        return {"name":"Wyckoff","long":round(min(long_s,1.0),4),"short":round(min(short_s,1.0),4),"why":why}
    except Exception as e:
        return {"name":"Wyckoff","long":0,"short":0,"why":{"err":str(e)}}

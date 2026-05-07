# strategies/expert_04_smc.py — E4: Smart Money Concepts
# Order Blocks, FVG, BOS/CHOCH, Liquidity Sweep

from __future__ import annotations
from typing import Dict, List, Optional

def _detect_order_blocks(candles: List, is_bull: bool) -> List[Dict]:
    blocks = []
    for i in range(2, min(len(candles)-1, 30)):
        c   = candles[i]
        nxt = candles[i-1]
        o,h,l,cl = float(c[1]),float(c[2]),float(c[3]),float(c[4])
        no,nh,nl,nc = float(nxt[1]),float(nxt[2]),float(nxt[3]),float(nxt[4])
        body = abs(cl - o)
        if body < (h - l) * 0.3: continue
        if is_bull and cl < o and nc > nh:          # bear candle → strong bull reaction
            blocks.append({"top": h, "bot": l, "mid": (h+l)/2, "type": "bullOB"})
        elif not is_bull and cl > o and nc < nl:    # bull candle → strong bear reaction
            blocks.append({"top": h, "bot": l, "mid": (h+l)/2, "type": "bearOB"})
    return blocks

def _detect_fvg(candles: List) -> List[Dict]:
    gaps = []
    for i in range(2, min(len(candles), 50)):
        c0 = candles[i-2]; c1 = candles[i-1]; c2 = candles[i]
        h0,l2 = float(c0[2]), float(c2[3])
        l0,h2 = float(c0[3]), float(c2[2])
        if l2 > float(c0[2]):   # bullish FVG
            gaps.append({"top": l2, "bot": float(c0[2]), "type": "bullFVG"})
        elif h2 < float(c0[3]): # bearish FVG
            gaps.append({"top": float(c0[3]), "bot": h2, "type": "bearFVG"})
    return gaps

def _detect_bos_choch(candles: List) -> Dict:
    if len(candles) < 10: return {"bos_bull": False, "choch_bull": False, "bos_bear": False}
    recent_hi = max(float(c[2]) for c in candles[1:6])
    recent_lo = min(float(c[3]) for c in candles[1:6])
    prev_hi   = max(float(c[2]) for c in candles[6:12]) if len(candles) >= 12 else recent_hi
    prev_lo   = min(float(c[3]) for c in candles[6:12]) if len(candles) >= 12 else recent_lo
    price = float(candles[0][4])
    bos_bull   = recent_hi > prev_hi * 1.002
    choch_bull = recent_lo > prev_lo * 1.002 and recent_hi <= prev_hi
    bos_bear   = recent_lo < prev_lo * 0.998
    choch_bear = recent_hi < prev_hi * 0.998 and recent_lo >= prev_lo
    return {"bos_bull": bos_bull, "choch_bull": choch_bull,
            "bos_bear": bos_bear, "choch_bear": choch_bear, "price": price}

def _liquidity_sweep(candles: List) -> Dict:
    if len(candles) < 15: return {"swept_high": False, "swept_low": False}
    price = float(candles[0][4])
    prev_hi = max(float(c[2]) for c in candles[2:10])
    prev_lo = min(float(c[3]) for c in candles[2:10])
    c_hi = float(candles[0][2]); c_lo = float(candles[0][3])
    swept_high = c_hi > prev_hi * 1.001 and price < prev_hi * 0.999
    swept_low  = c_lo < prev_lo * 0.999 and price > prev_lo * 1.001
    return {"swept_high": swept_high, "swept_low": swept_low}

def analyze(data: Dict) -> Optional[Dict]:
    try:
        c5m = data.get("c5m",[]); c1h = data.get("c1h",[]); c4h = data.get("c4h",[])
        if not c5m or len(c5m) < 15: return None
        price = float(c5m[0][4])
        long_s = short_s = 0.0; why = {}

        for candles, lbl, w in [(c4h,"4H",1.5),(c1h,"1H",1.2),(c5m,"5m",1.0)]:
            if not candles or len(candles) < 10: continue
            bos = _detect_bos_choch(candles)
            if bos["bos_bull"]:   long_s  += 0.25*w/1.5; why["bos"] = f"BOS Bull {lbl}"
            if bos["choch_bull"]: long_s  += 0.20*w/1.5; why["choch"] = f"CHOCH Bull {lbl}"
            if bos["bos_bear"]:   short_s += 0.25*w/1.5
            if bos.get("choch_bear"): short_s += 0.20*w/1.5

            # Order Blocks
            bull_obs = _detect_order_blocks(candles, True)
            bear_obs = _detect_order_blocks(candles, False)
            for ob in bull_obs[:2]:
                if ob["bot"] <= price <= ob["top"] * 1.003:
                    long_s += 0.22*w/1.5; why["ob"] = f"Bull OB {lbl} @ {ob['mid']:.4f}"
            for ob in bear_obs[:2]:
                if ob["bot"] * 0.997 <= price <= ob["top"]:
                    short_s += 0.22*w/1.5; why["ob_b"] = f"Bear OB {lbl}"

            # FVG
            fvgs = _detect_fvg(candles)
            for g in fvgs[:3]:
                if g["type"] == "bullFVG" and g["bot"] <= price <= g["top"]:
                    long_s += 0.15*w/1.5; why["fvg"] = f"Bull FVG {lbl}"
                elif g["type"] == "bearFVG" and g["bot"] <= price <= g["top"]:
                    short_s += 0.15*w/1.5

        # Liquidity Sweep (5m)
        sweep = _liquidity_sweep(c5m)
        if sweep["swept_low"]:  long_s  += 0.30; why["sweep"] = "Liquidity sweep Low 🎯"
        if sweep["swept_high"]: short_s += 0.30; why["sweep"] = "Liquidity sweep High 🎯"

        return {"name":"SMC","long":round(min(long_s,1.0),4),"short":round(min(short_s,1.0),4),"why":why}
    except Exception as e:
        return {"name":"SMC","long":0,"short":0,"why":{"err":str(e)}}

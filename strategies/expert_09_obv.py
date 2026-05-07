# strategies/expert_09_obv.py — E9: OBV + Volume Profile
from __future__ import annotations
from typing import Dict, Optional
from engine.indicator_engine import IndicatorEngine
from utils.helpers import calc_volume_profile
IE = IndicatorEngine()

def analyze(data: Dict) -> Optional[Dict]:
    try:
        c5m = data.get("c5m",[]); c1h = data.get("c1h",[]); c4h = data.get("c4h",[])
        if not c5m or len(c5m) < 12: return None
        price = float(c5m[0][4])
        long_s = short_s = 0.0; why = {}

        # OBV multi-timeframe
        for candles, lbl, w in [(c4h,"4H",0.40),(c1h,"1H",0.35),(c5m,"5m",0.25)]:
            if not candles or len(candles) < 12: continue
            obv_r = IE.obv(candles)
            cmf_r = IE.cmf(candles)
            if obv_r:
                if obv_r["div_bull"]: long_s  += 0.35*w; why[f"obv_div_{lbl}"] = f"OBV Bull Div {lbl}"
                elif obv_r["rising"]: long_s  += 0.20*w
                if obv_r["div_bear"]: short_s += 0.35*w; why[f"obv_div_{lbl}"] = f"OBV Bear Div {lbl}"
                elif obv_r["falling"]:short_s += 0.20*w
            if cmf_r:
                if cmf_r["strong"] and cmf_r["bullish"]: long_s  += 0.20*w
                elif cmf_r["strong"] and cmf_r["bearish"]:short_s += 0.20*w

        # Volume Profile: POC / VAH / VAL
        vp = calc_volume_profile(c5m, 10)
        poc = vp.get("poc",0); vah = vp.get("vah",0); val = vp.get("val",0)
        if poc > 0:
            if price > poc * 1.002: long_s  += 0.15; why["vpc"] = f"Above POC {poc:.4f}"
            elif price < poc * 0.998: short_s += 0.15; why["vpc"] = f"Below POC"
        if val > 0 and price <= val * 1.003: long_s  += 0.20; why["val"] = "At VAL support"
        if vah > 0 and price >= vah * 0.997: short_s += 0.20; why["vah"] = "At VAH resistance"

        # VWAP
        vwap = IE.vwap(c5m)
        if vwap > 0:
            if price > vwap * 1.001: long_s  += 0.12; why["vwap"] = f"Above VWAP {vwap:.4f}"
            elif price < vwap * 0.999: short_s += 0.12

        return {"name":"OBV","long":round(min(long_s,1.0),4),"short":round(min(short_s,1.0),4),"why":why}
    except Exception as e:
        return {"name":"OBV","long":0,"short":0,"why":{"err":str(e)}}

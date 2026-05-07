# strategies/expert_10_daily.py — E10: Daily Director
# Weekly/Daily trend analysis, CME gap detection, daily POC
from __future__ import annotations
from typing import Dict, Optional
from engine.indicator_engine import IndicatorEngine
from utils.helpers import calc_volume_profile, utc_weekday, utc_hour
IE = IndicatorEngine()

def _detect_cme_gap(c1d) -> Dict:
    """Detect CME gap between Friday close and Sunday open (crypto trades 24/7)."""
    try:
        if not c1d or len(c1d) < 3: return {"has_gap": False}
        fri_close = float(c1d[2][4])
        mon_open  = float(c1d[0][1])
        gap_pct   = (mon_open - fri_close) / fri_close
        if abs(gap_pct) > 0.005:
            return {"has_gap": True, "gap_pct": round(gap_pct, 4),
                    "gap_fill": fri_close, "bullish": gap_pct < 0}
        return {"has_gap": False}
    except:
        return {"has_gap": False}

def analyze(data: Dict) -> Optional[Dict]:
    try:
        c1d = data.get("c1d",[]); c1w = data.get("c1w",[])
        c4h = data.get("c4h",[]); c1h = data.get("c1h",[]); c5m = data.get("c5m",[])
        if not c4h or len(c4h) < 20: return None
        price = float(c4h[0][4])
        long_s = short_s = 0.0; why = {}

        # Weekly trend
        if c1w and len(c1w) >= 4:
            wk_trend = IE.get_trend(c1w)
            if wk_trend == "BULL":   long_s  += 0.30; why["weekly"] = "Weekly BULL"
            elif wk_trend == "BEAR": short_s += 0.30; why["weekly"] = "Weekly BEAR"

        # Daily trend
        if c1d and len(c1d) >= 20:
            d_trend  = IE.get_trend(c1d)
            if d_trend == "BULL":   long_s  += 0.25
            elif d_trend == "BEAR": short_s += 0.25

        # Daily POC
        if c1d and len(c1d) >= 10:
            vp = calc_volume_profile(c1d, 30)
            poc = vp.get("poc", 0)
            if poc > 0:
                if price > poc * 1.002:   long_s  += 0.20; why["poc"] = f"Above daily POC {poc:.2f}"
                elif price < poc * 0.998: short_s += 0.20; why["poc"] = f"Below daily POC"

        # CME Gap magnet
        gap = _detect_cme_gap(c1d)
        if gap["has_gap"]:
            if gap["bullish"]: long_s  += 0.25; why["cme"] = f"CME gap fill bull {gap['gap_pct']:.2%}"
            else:              short_s += 0.25; why["cme"] = f"CME gap fill bear"

        # 4H regime
        regime = IE.get_market_regime(c4h, c1h)
        if regime["bull_align"]:   long_s  += 0.20; why["regime"] = "4H EMA aligned bull"
        elif regime["bear_align"]: short_s += 0.20; why["regime"] = "4H EMA aligned bear"

        # Weekday bonus
        wd = utc_weekday()
        if wd in (1,2,3): long_s += 0.05  # Tue-Thu statistically strongest

        return {"name":"Daily","long":round(min(long_s,1.0),4),"short":round(min(short_s,1.0),4),"why":why}
    except Exception as e:
        return {"name":"Daily","long":0,"short":0,"why":{"err":str(e)}}

# strategies/expert_06_sessions.py — E6: Sessions + Silver Bullet
from __future__ import annotations
from typing import Dict, Optional
from utils.helpers import get_session, is_opportunity_window, utc_hour, utc_weekday

def analyze(data: Dict) -> Optional[Dict]:
    try:
        session = get_session()
        h = utc_hour(); wd = utc_weekday()
        long_s = short_s = 0.0; why = {}

        # Session biases
        lb = session.get("long_bias", 0.05)
        sb = session.get("short_bias", 0.05)
        long_s  += lb; short_s += sb
        why["session"] = session["name"]

        # High-volume overlap bonus
        if session.get("overlap"): long_s += 0.10; short_s += 0.05; why["overlap"] = "NY+London ⚡"

        # Dead zone penalty
        if session["vol"] == "VERY_LOW": long_s *= 0.30; short_s *= 0.30

        # Silver Bullet windows (ICT): 03:00-04:00, 10:00-11:00, 14:00-15:00 UTC
        silver_bullet = h in (3, 10, 14)
        if silver_bullet:
            long_s += 0.20; short_s += 0.15; why["silver"] = f"🥈 Silver Bullet {h}:00 UTC"

        # NY Open opportunity window
        if is_opportunity_window():
            long_s += 0.10; short_s += 0.08; why["opp"] = "Opportunity window"

        # Weekly bias: avoid Mon/Fri extremes
        if wd == 0:   long_s *= 0.85  # Monday — cautious open
        elif wd == 4: short_s += 0.05 # Friday — slight short bias EOW

        return {"name":"Sessions","long":round(min(long_s,1.0),4),"short":round(min(short_s,1.0),4),"why":why}
    except Exception as e:
        return {"name":"Sessions","long":0,"short":0,"why":{"err":str(e)}}

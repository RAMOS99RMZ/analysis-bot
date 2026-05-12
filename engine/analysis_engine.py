"""
engine/analysis_engine.py  —  E10 + E11 Daily Analysis Engine
الإصلاحات: CME Gap datetime · أوزان مُعيَّرة · فلتر تقارب المستويات
"""
from __future__ import annotations
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import httpx
from loguru import logger
from engine.indicator_engine import IndicatorEngine
from utils.helpers import calc_volume_profile

IE = IndicatorEngine()
_PROXIMITY_THRESHOLD = 0.008
_CONFIDENCE_BULL     = 0.30
_CONFIDENCE_BEAR     = -0.30


# ── CME Gap (إصلاح: datetime.weekday فعلي) ───────────────────────────

def _detect_cme_gap(c1d: List) -> Dict:
    if not c1d or len(c1d) < 10:
        return {"has_gap": False}
    try:
        friday_close: Optional[float] = None
        monday_open:  Optional[float] = None
        for candle in c1d[:14]:
            ts = int(candle[0]) / 1000
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            wd = dt.weekday()
            if wd == 4 and friday_close is None:
                friday_close = float(candle[4])
            if wd == 0 and monday_open is None:
                monday_open = float(candle[1])
            if friday_close and monday_open:
                break
        if not friday_close or not monday_open:
            return {"has_gap": False}
        gap_pct = (monday_open - friday_close) / friday_close
        if abs(gap_pct) < 0.003:
            return {"has_gap": False}
        direction = "UP" if gap_pct > 0 else "DOWN"
        return {
            "has_gap":    True,
            "gap_pct":    round(gap_pct * 100, 3),
            "direction":  direction,
            "fill_level": friday_close,
            "bullish":    gap_pct < 0,
            "label":      f"CME Gap {direction} {abs(gap_pct*100):.2f}%",
        }
    except Exception as e:
        logger.warning(f"[CME Gap] {e}")
        return {"has_gap": False}


# ── Proximity Filter ──────────────────────────────────────────────────

def _calc_proximity_score(price: float, levels: Dict) -> Dict:
    if not price:
        return {"score": 0.5, "nearest": "—", "distance": 1.0,
                "near_key_level": False}
    key_levels: Dict[str, float] = {}
    fibs = levels.get("fib", {})
    for n, v in fibs.items():
        if v and v > 0:
            key_levels[f"Fib {n}%"] = v
    for n, k in [("PP","pp"),("R1","tp1"),("S1","sup1"),("R2","tp2"),
                 ("S2","sup2"),("POC","poc"),("EMA20","ema20"),
                 ("EMA50","ema50"),("EMA200","ema200")]:
        v = levels.get(k, 0)
        if v and v > 0:
            key_levels[n] = v
    if not key_levels:
        return {"score": 0.5, "nearest": "—", "distance": 1.0,
                "near_key_level": False}
    best_name = "—"; best_dist = float("inf")
    for name, lvl in key_levels.items():
        dist = abs(price - lvl) / price
        if dist < best_dist:
            best_dist = dist; best_name = name
    score = (1.0 if best_dist <= _PROXIMITY_THRESHOLD * 0.5 else
             0.75 if best_dist <= _PROXIMITY_THRESHOLD else
             0.50 if best_dist <= _PROXIMITY_THRESHOLD * 2 else
             0.25 if best_dist <= _PROXIMITY_THRESHOLD * 4 else 0.10)
    return {
        "score":          round(score, 3),
        "nearest":        best_name,
        "distance":       round(best_dist * 100, 3),
        "near_key_level": best_dist <= _PROXIMITY_THRESHOLD,
    }


# ── Expert Analysis (أوزان مُعيَّرة -1 إلى +1) ───────────────────────

def _run_experts(c1d, c4h, c1h, c5m, price: float) -> Dict:
    results: Dict[str, Dict] = {}

    def _res(name, score, detail=""):
        s = max(-1.0, min(1.0, float(score)))
        e = "🟢" if s > 0.2 else "🔴" if s < -0.2 else "🟡"
        sig = ("صاعد 🟢" if s > 0.2 else "هابط 🔴" if s < -0.2 else "محايد 🟡")
        return {"score": round(s, 3), "signal": sig, "emoji": e, "detail": detail}

    # 1. ClassicTA
    try:
        rsi  = IE.rsi(c4h, 14)        if c4h else 50.0
        macd = IE.macd(c4h)            if c4h else {"hist": 0, "macd": 0}
        st   = IE.supertrend(c4h, 3)  if c4h and len(c4h) >= 14 else None
        ha   = IE.heikin_ashi(c4h)    if c4h else None
        adx  = IE.adx(c4h, 14)       if c4h and len(c4h) >= 28 else None

        raw = 0.0
        # RSI weight=0.25
        if rsi < 35:   raw += 0.25
        elif rsi < 45: raw += 0.12
        elif rsi > 65: raw -= 0.25
        elif rsi > 55: raw -= 0.12
        # MACD weight=0.25
        h = macd.get("hist", 0); m = macd.get("macd", 0)
        raw += (0.25 if h > 0 and m > 0 else 0.12 if h > 0 else
               -0.25 if h < 0 and m < 0 else -0.12 if h < 0 else 0)
        # SuperTrend weight=0.30
        if st:
            if st.get("bull_flip"):   raw += 0.30
            elif st.get("bull"):      raw += 0.18
            elif st.get("bear_flip"): raw -= 0.30
            elif st.get("bear"):      raw -= 0.18
        # Heikin Ashi weight=0.15
        if ha:
            if ha.get("bull_strong"): raw += 0.15
            elif ha.get("bull"):      raw += 0.08
            elif ha.get("bear_strong"):raw -= 0.15
            elif ha.get("bear"):      raw -= 0.08
        # ADX — مُضاعِف
        if adx:
            raw *= (1.25 if adx["adx"] > 30 else 0.75 if adx["adx"] < 15 else 1.0)

        results["ClassicTA"] = _res("ClassicTA", raw,
            f"RSI={rsi:.0f} | MACD={'↑' if h>0 else '↓'} | "
            f"ST={'Bull' if st and st.get('bull') else 'Bear'}")
    except Exception as e:
        logger.debug(f"[ClassicTA] {e}")
        results["ClassicTA"] = _res("ClassicTA", 0.0, "خطأ")

    # 2. SMC
    try:
        raw = 0.0
        if c4h and len(c4h) >= 12:
            rH = max(float(c[2]) for c in c4h[:6])
            rL = min(float(c[3]) for c in c4h[:6])
            pH = max(float(c[2]) for c in c4h[6:12])
            pL = min(float(c[3]) for c in c4h[6:12])
            if rH > pH * 1.002 and price > pH:   raw += 1.0
            elif rL > pL * 1.002 and rH <= pH:   raw += 0.5
            if rL < pL * 0.998 and price < pL:   raw -= 1.0
            elif rH < pH * 0.998 and rL >= pL:   raw -= 0.5
        if c5m and len(c5m) >= 15:
            hi5 = max(float(c[2]) for c in c5m[1:8])
            lo5 = min(float(c[3]) for c in c5m[1:8])
            if float(c5m[0][3]) < lo5 * 0.999 and float(c5m[0][4]) > lo5: raw += 0.4
            elif float(c5m[0][2]) > hi5 * 1.001 and float(c5m[0][4]) < hi5: raw -= 0.4
        results["SMC"] = _res("SMC", raw, "BOS / Liquidity Sweep")
    except Exception as e:
        logger.debug(f"[SMC] {e}")
        results["SMC"] = _res("SMC", 0.0, "خطأ")

    # 3. Wyckoff
    try:
        from strategies.expert_03_wyckoff import detect_wyckoff_phase
        w    = detect_wyckoff_phase(c4h) if c4h and len(c4h) >= 30 else {"bias": 0, "score": 0.5, "phase": "UNKNOWN", "events": []}
        raw  = float(w.get("bias", 0)) * float(w.get("score", 0.5))
        ev   = " + ".join(w.get("events", [])[:2]) or "—"
        ph   = {"ACCUMULATION":"تراكم","DISTRIBUTION":"توزيع",
                "MARKUP":"صعود","MARKDOWN":"هبوط","UNKNOWN":"غير محدد"}.get(w.get("phase","UNKNOWN"),"—")
        results["Wyckoff"] = _res("Wyckoff", raw, f"{ph} | {ev}")
    except Exception as e:
        logger.debug(f"[Wyckoff] {e}")
        results["Wyckoff"] = _res("Wyckoff", 0.0, "—")

    # 4. Harmonic
    try:
        from strategies.expert_02_harmonic import analyze as h_anal
        hr   = h_anal({"c5m": c5m or [], "c15m": [], "c30m": [], "c1h": c1h or [], "c4h": c4h or []}) or {}
        raw  = hr.get("long", 0.0) - hr.get("short", 0.0)
        hw   = hr.get("why", {})
        det  = hw.get("harmonic", hw.get("harmonic_bear", hw.get("fib", "—")))
        results["Harmonic"] = _res("Harmonic", raw, str(det)[:30])
    except Exception as e:
        logger.debug(f"[Harmonic] {e}")
        results["Harmonic"] = _res("Harmonic", 0.0, "لا نموذج")

    # 5. Gann + Lunar
    try:
        from strategies.expert_08_gann import _calc_lunar_phase, _gann_sq9
        lunar = _calc_lunar_phase()
        sq9   = _gann_sq9(price)
        raw   = lunar["strength"] if lunar["bias"] == "BULL" else -lunar["strength"]
        days  = lunar["days_into"]
        if any(abs(days - cd) <= 1.75 for cd in [1.0, 7.5, 14.77, 22.0]):
            raw = max(-1.0, min(1.0, raw * 1.3))
        lvls  = sq9.get("levels", [])
        near  = f"${min(lvls, key=lambda x: abs(x-price)):,.0f}" if lvls else "—"
        results["Gann"] = _res("Gann", raw, f"{lunar['emoji']} يوم {days:.0f} | Sq9: {near}")
    except Exception as e:
        logger.debug(f"[Gann] {e}")
        results["Gann"] = _res("Gann", 0.0, "—")

    # 6. OBV + Volume
    try:
        obv  = IE.obv(c4h)  if c4h and len(c4h) >= 12 else None
        cmf  = IE.cmf(c4h)  if c4h and len(c4h) >= 14 else None
        vwap = IE.vwap(c4h) if c4h else 0.0
        vp   = calc_volume_profile((c4h or [])[:40], 10)
        poc  = vp.get("poc", price)
        raw  = 0.0
        if obv:
            if obv.get("div_bull"):   raw += 0.6
            elif obv.get("rising"):   raw += 0.3
            if obv.get("div_bear"):   raw -= 0.6
            elif obv.get("falling"):  raw -= 0.3
        if cmf:
            raw += cmf.get("value", 0) * 0.4
        if vwap and price > 0:
            raw += 0.2 if price > vwap else -0.2
        abv = "فوق VWAP ✅" if vwap and price > vwap else "تحت VWAP ⚠️"
        results["OBV"] = _res("OBV", raw, f"POC=${poc:,.0f} | {abv}")
    except Exception as e:
        logger.debug(f"[OBV] {e}")
        results["OBV"] = _res("OBV", 0.0, "—")

    # 7. Ichimoku + EMA
    try:
        ichi  = IE.ichimoku(c4h) if c4h and len(c4h) >= 55 else None
        e20   = IE.ema(c4h, 20)  if c4h else price
        e50   = IE.ema(c4h, 50)  if c4h else price
        e200  = IE.ema(c4h, 200) if c4h and len(c4h) >= 200 else price
        raw   = 0.0
        # EMA alignment
        if price > e20 > e50 > e200:   raw += 0.5
        elif price > e20 > e50:        raw += 0.3
        elif price < e20 < e50 < e200: raw -= 0.5
        elif price < e20 < e50:        raw -= 0.3
        # Ichimoku
        if ichi:
            if ichi.get("above_cloud") and ichi.get("bull_cross"):  raw += 0.5
            elif ichi.get("above_cloud"):                           raw += 0.25
            elif ichi.get("below_cloud") and not ichi.get("bull_cross"): raw -= 0.5
            elif ichi.get("below_cloud"):                               raw -= 0.25
        det = f"EMA20=${e20:,.0f} | EMA50=${e50:,.0f} | EMA200=${e200:,.0f}"
        results["Ichimoku"] = _res("Ichimoku", raw, det)
    except Exception as e:
        logger.debug(f"[Ichimoku] {e}")
        results["Ichimoku"] = _res("Ichimoku", 0.0, "—")

    return results


# ── S/R Levels ────────────────────────────────────────────────────────

def _calc_levels(c1d, c4h, c1h, price: float, direction: str) -> Dict:
    try:
        PP=R1=R2=R3=S1=S2=S3 = price
        if c1d and len(c1d) >= 2:
            H,L,C = float(c1d[1][2]), float(c1d[1][3]), float(c1d[1][4])
            PP = (H+L+C)/3
            R1=2*PP-L; R2=PP+(H-L); R3=H+2*(PP-L)
            S1=2*PP-H; S2=PP-(H-L); S3=L-2*(H-PP)
        lookback = (c4h or [])[:80]
        hi=lo=price; f382=f500=f618=f786=ext1272=ext1618=price
        if len(lookback) >= 20:
            hi  = max(float(c[2]) for c in lookback)
            lo  = min(float(c[3]) for c in lookback)
            rng = hi - lo
            if rng > 0:
                f382=hi-rng*0.382; f500=hi-rng*0.500
                f618=hi-rng*0.618; f786=hi-rng*0.786
                ext1272=lo-rng*0.272; ext1618=lo-rng*0.618
        e20  = IE.ema(c4h, 20)  if c4h else price
        e50  = IE.ema(c4h, 50)  if c4h else price
        e200 = IE.ema(c4h, 200) if c4h and len(c4h)>=200 else price
        vp   = calc_volume_profile((c4h or [])[:40], 15)
        poc  = vp.get("poc", price); vah=vp.get("vah",R1); val=vp.get("val",S1)
        if direction == "BULL":
            tp1,tp2,tp3 = R1,R2,R3
            sp1,sp2,sp3 = S1,(f618 if f618<price else S2),S2
        elif direction == "BEAR":
            tp1,tp2,tp3 = S1,S2,S3
            sp1,sp2,sp3 = R1,(f382 if f382>price else R2),R2
        else:
            tp1,tp2,tp3 = R1,f382,R2
            sp1,sp2,sp3 = S1,f618,S2
        return {
            "pp":pp,"poc":poc,"vah":vah,"val":val,
            "tp1":tp1,"tp2":tp2,"tp3":tp3,
            "sup1":sp1,"sup2":sp2,"sup3":sp3,
            "ema20":e20,"ema50":e50,"ema200":e200,
            "fib":{"38.2":f382,"50.0":f500,"61.8":f618,"78.6":f786},
            "ext":{"127.2":ext1272,"161.8":ext1618},
            "hi":hi,"lo":lo,
        }
    except Exception as e:
        logger.warning(f"[Levels] {e}")
        return {k:price for k in ["pp","poc","vah","val","tp1","tp2","tp3",
                                   "sup1","sup2","sup3","ema20","ema50","ema200","hi","lo"]}

# needed for levels function
pp = 0  # dummy, will be overwritten


# ── USDT Dominance (E11) ──────────────────────────────────────────────

async def _fetch_usdt_dominance() -> Dict:
    try:
        async with httpx.AsyncClient(timeout=12) as cl:
            r = await cl.get("https://api.coingecko.com/api/v3/global",
                             headers={"Accept":"application/json"})
            if r.status_code != 200:
                raise ValueError(f"HTTP {r.status_code}")
            data   = r.json().get("data", {})
            dom    = data.get("market_cap_percentage", {})
            usdt_d = dom.get("usdt", 7.0)
            usdc_d = dom.get("usdc", 1.5)
            btc_d  = dom.get("btc",  50.0)
            tmc    = data.get("total_market_cap",{}).get("usd",0)
            tvol   = data.get("total_volume",{}).get("usd",0)
            stab   = usdt_d + usdc_d
            if usdt_d > 7.5:
                trend="BEAR"; sig="SHORT"; sc=-0.8; tr_ar="ضغط بيع على الكريبتو 🔴"
            elif usdt_d > 6.8:
                trend="NEUTRAL"; sig="NEUTRAL"; sc=-0.2; tr_ar="ضغط خفيف 🟡"
            elif usdt_d < 5.5:
                trend="BULL"; sig="LONG"; sc=0.8; tr_ar="تدفق شراء قوي 🟢"
            else:
                trend="BULL"; sig="LONG"; sc=0.3; tr_ar="جاذبية للكريبتو 🟢"
            return {"usdt_d":round(usdt_d,3),"usdc_d":round(usdc_d,3),
                    "btc_d":round(btc_d,1),"stables":round(stab,3),
                    "total_mc":tmc,"total_vol":tvol,
                    "trend":trend,"trend_ar":tr_ar,"signal":sig,"score":sc}
    except Exception as e:
        logger.warning(f"[USDT.D] {e}")
        return {"usdt_d":7.0,"usdc_d":1.5,"btc_d":50.0,"stables":8.5,
                "total_mc":0,"total_vol":0,"trend":"NEUTRAL",
                "trend_ar":"محايد 🟡","signal":"NEUTRAL","score":0.0}


# ── Telegram Message ──────────────────────────────────────────────────

def _f(p: float) -> str:
    return f"${p:,.0f}" if p >= 1000 else f"${p:,.2f}" if p >= 1 else f"${p:,.6f}"


def _asset_block(sym: str, d: Dict) -> str:
    ic   = "₿" if "BTC" in sym else "Ξ"
    sym_c= sym.replace("/USDT:USDT","")
    dr   = d["direction"]
    de   = "🟢 صاعد" if dr=="BULL" else "🔴 هابط" if dr=="BEAR" else "🟡 محايد"
    cf   = int(d["confidence"]*100)
    pr   = d["proximity"]
    lv   = d["levels"]
    ex   = d["experts"]
    cm   = d["cme"]
    price= d["price"]

    prx_txt = ("✅ قريب من " + pr["nearest"] if pr["near_key_level"]
               else f"⚠️ بعيد ({pr['distance']:.1f}% من {pr['nearest']})")

    EICO = {"ClassicTA":"📊","SMC":"🏛️","Wyckoff":"⚖️",
            "Harmonic":"🎯","Gann":"✨","OBV":"📈","Ichimoku":"☁️"}
    exp_lines = "\n".join(
        f"  {EICO.get(n,'•')} <b>{n}:</b> {v['emoji']} {v['signal'][:25]}"
        for n,v in ex.items()
    )

    if dr == "BULL":
        tl="📈 أهداف الصعود:"; sl="🛡️ الدعم:"
        tg=f"  🎯 {_f(lv['tp1'])}\n  🎯 {_f(lv['tp2'])}\n  🏆 {_f(lv['tp3'])}"
        sg=f"  S1: {_f(lv['sup1'])}  S2: {_f(lv['sup2'])}"
    elif dr == "BEAR":
        tl="📉 أهداف الهبوط:"; sl="🚧 المقاومة:"
        tg=f"  🎯 {_f(lv['tp1'])}\n  🎯 {_f(lv['tp2'])}\n  🏆 {_f(lv['tp3'])}"
        sg=f"  R1: {_f(lv['sup1'])}  R2: {_f(lv['sup2'])}"
    else:
        tl="↔️ نطاق التداول:"; sl=""
        tg=(f"  📈 مقاومة: {_f(lv['tp1'])} / {_f(lv['tp2'])}\n"
            f"  📉 دعم:    {_f(lv['sup1'])} / {_f(lv['sup2'])}")
        sg=""

    fibs = lv.get("fib",{})
    fb = f"  38.2%: {_f(fibs.get('38.2',0))}  61.8%: {_f(fibs.get('61.8',0))} 🌟" if fibs else ""
    cme_l = f"\n🔷 <b>CME Gap:</b> {cm['label']} → ملء عند {_f(cm['fill_level'])}" if cm.get("has_gap") else ""

    lines = [
        f"{'━'*32}",
        f"{ic} <b>{sym_c}/USDT</b>  ·  {_f(price)}",
        f"📌 <b>{de}</b>  ·  ثقة: <b>{cf}%</b>  ·  {prx_txt}",
        "",
        f"📋 <b>الخبراء (1D/4H/1H):</b>",
        exp_lines,
        "",
        f"📐 <b>Fibonacci</b>  {_f(lv['lo'])} ↔ {_f(lv['hi'])}",
        fb,
        "",
        f"<b>{tl}</b>",
        tg,
    ]
    if sl: lines += [f"<b>{sl}</b>", sg]
    lines += [
        "",
        f"  💹 POC: {_f(lv['poc'])}  EMA50: {_f(lv['ema50'])}",
        cme_l,
    ]
    return "\n".join(str(l) for l in lines if l is not None)


def _build_message(btc: Dict, eth: Dict, ud: Dict, now_str: str) -> str:
    btc_b = _asset_block("BTC/USDT:USDT", btc)
    eth_b = _asset_block("ETH/USDT:USDT", eth)

    sig11 = ud.get("signal","NEUTRAL")
    sig_e = ("🟢 ابحث عن LONG"  if sig11=="LONG"  else
             "🔴 ابحث عن SHORT" if sig11=="SHORT" else "🟡 انتظر")
    tmc   = ud.get("total_mc",0) / 1e9

    e11 = (
        f"{'━'*32}\n"
        f"📡 <b>E11 — USDT Dominance</b>\n"
        f"  USDT.D: <b>{ud['usdt_d']:.3f}%</b>  BTC.D: <b>{ud['btc_d']:.1f}%</b>\n"
        f"  حجم السوق: <b>${tmc:,.0f}B</b>\n\n"
        f"  الحالة: {ud['trend_ar']}\n"
        f"  📣 <b>{sig_e}</b>"
    )

    bs = sum(e["score"] for e in btc["experts"].values())
    es = sum(e["score"] for e in eth["experts"].values())
    us = ud.get("score", 0.0)
    total = bs*0.40 + es*0.30 + us*0.30

    verdict = ("🟢 السوق إيجابي — ابحث عن LONG"   if total >= 0.25 else
               "🔴 السوق سلبي — ابحث عن SHORT"    if total <= -0.25 else
               "🟡 السوق محايد — انتظر تأكيداً")

    return (
        f"📊 <b>التحليل اليومي — Ramos Ai 360 🎖️</b>\n"
        f"🕐 {now_str}\n\n"
        f"{btc_b}\n\n{eth_b}\n\n{e11}\n\n"
        f"{'━'*32}\n"
        f"🏁 <b>الخلاصة:</b> {verdict}\n"
        f"{'━'*32}\n"
        f"<i>🎖️ Ramos Ai 360 ♾️ — E10+E11 Daily Analysis</i>"
    )


# ── Main Entry Point ──────────────────────────────────────────────────

async def run_full_analysis(db: Any, notifier: Any, fetcher: Any) -> Dict:
    """
    يُستدعى من main.py عند mode=analysis.
    - كل 4 ساعات: تحديث خفيف (Bias فقط، بدون رسالة مفصّلة)
    - 12:00 AM UTC: تحليل يومي كامل + رسالة Telegram مفصّلة
    - يخزن النتيجة في Supabase لتقرأها Scalp/Swing/SuperSwing
    """
    now        = datetime.now(timezone.utc)
    now_str    = now.strftime("%d/%m/%Y %H:%M UTC")
    is_midnight= (now.hour == 0)

    logger.info(f"[Analysis] {'تحليل يومي كامل ⭐' if is_midnight else 'تحديث دوري'} — {now_str}")

    usdt_d  = await _fetch_usdt_dominance()
    results: Dict[str, Any] = {}

    for symbol in ["BTC/USDT:USDT", "ETH/USDT:USDT"]:
        sym_c = symbol.replace("/USDT:USDT","")
        try:
            c1d, c4h, c1h, c5m = await asyncio.gather(
                fetcher.get_candles(symbol, "1d", 90),
                fetcher.get_candles(symbol, "4h", 200),
                fetcher.get_candles(symbol, "1h", 100),
                fetcher.get_candles(symbol, "5m",  50),
            )
            if not c4h or len(c4h) < 20:
                logger.warning(f"[Analysis] {sym_c}: بيانات غير كافية")
                continue

            price   = float(c4h[0][4])
            experts = _run_experts(c1d or [], c4h, c1h or [], c5m or [], price)
            avg_sc  = sum(e["score"] for e in experts.values()) / (len(experts) or 1)

            direction  = ("BULL"    if avg_sc >= _CONFIDENCE_BULL  else
                          "BEAR"    if avg_sc <= _CONFIDENCE_BEAR  else "NEUTRAL")
            confidence = min(1.0, abs(avg_sc))

            levels    = _calc_levels(c1d or [], c4h, c1h or [], price, direction)
            proximity = _calc_proximity_score(price, levels)
            cme       = _detect_cme_gap(c1d or [])

            await db.log_regime(symbol, direction, round(confidence, 3))

            results[sym_c] = {
                "price":      price,
                "direction":  direction,
                "confidence": confidence,
                "experts":    experts,
                "levels":     levels,
                "proximity":  proximity,
                "cme":        cme,
                "avg_score":  round(avg_sc, 3),
            }

            logger.info(f"[Analysis] {sym_c}: {direction} | "
                        f"ثقة={int(confidence*100)}% | "
                        f"أقرب={proximity['nearest']}")

        except Exception as e:
            logger.error(f"[Analysis] {sym_c} خطأ: {e}")

    # ── إرسال ─────────────────────────────────────────────────────
    if "BTC" in results and "ETH" in results:
        if is_midnight:
            msg = _build_message(results["BTC"], results["ETH"], usdt_d, now_str)
            await notifier.send(msg)
            logger.success("[Analysis] ✅ التقرير اليومي الكامل أُرسل.")
        else:
            btc_e = "🟢" if results["BTC"]["direction"]=="BULL" else "🔴" if results["BTC"]["direction"]=="BEAR" else "🟡"
            eth_e = "🟢" if results["ETH"]["direction"]=="BULL" else "🔴" if results["ETH"]["direction"]=="BEAR" else "🟡"
            sig_e = "🟢" if usdt_d["signal"]=="LONG" else "🔴" if usdt_d["signal"]=="SHORT" else "🟡"
            brief = (
                f"📊 <b>Market Bias</b> — {now_str}\n"
                f"{'━'*26}\n"
                f"  {btc_e} BTC: <b>{results['BTC']['direction']}</b>"
                f" {int(results['BTC']['confidence']*100)}%"
                f" · {results['BTC']['proximity']['nearest']}\n"
                f"  {eth_e} ETH: <b>{results['ETH']['direction']}</b>"
                f" {int(results['ETH']['confidence']*100)}%"
                f" · {results['ETH']['proximity']['nearest']}\n"
                f"  {sig_e} USDT.D: <b>{usdt_d['usdt_d']:.3f}%</b>"
                f" · {usdt_d['trend_ar']}\n"
                f"{'━'*26}\n"
                f"<i>🎖️ Ramos Ai 360 — E10+E11</i>"
            )
            await notifier.send(brief)
            logger.success("[Analysis] ✅ التحديث المختصر أُرسل.")
    else:
        await notifier.send(f"⚠️ بيانات غير كاملة — {now_str}")

    results["USDT_D"] = usdt_d
    return results

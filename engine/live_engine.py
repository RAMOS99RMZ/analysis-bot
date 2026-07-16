
"""
engine/live_engine.py — Ramos 360 Ai 🎖️
════════════════════════════════════════════
يحوّل منطق الـ backtest إلى قرارات تداول حية.
نفس الخوارزميات بالضبط — فقط يقرأ بيانات حية بدلاً من تاريخية.
"""
from __future__ import annotations
import asyncio, os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import httpx, pandas as pd
from loguru import logger

_OKX = "https://www.okx.com/api/v5"
_HDR = {"Accept":"application/json","User-Agent":"Ramos360Live/1.0"}
_TFM = {"1h":"1H","4h":"4H","1d":"1D","15m":"15m","5m":"5m"}

# ── Symbols routing ───────────────────────────────────────────────────────────
ALT_SYMBOLS   = {"SOL","LINK","DOGE","AVAX","ADA","BNB","XRP"}
MACRO_SYMBOLS = {"XAUUSD","XAGUSD","SPX","NDX"}

def _is_alt(sym_c: str)   -> bool: return sym_c.upper() in ALT_SYMBOLS
def _is_macro(sym_c: str) -> bool: return sym_c.upper() in MACRO_SYMBOLS

def _inst(sym: str) -> str:
    return sym.split(":")[0].replace("/","-")+"-SWAP"

# ── Live price ────────────────────────────────────────────────────────────────
async def get_live_price(symbol: str) -> float:
    """Real-time last price from OKX."""
    try:
        async with httpx.AsyncClient(timeout=8, headers=_HDR) as cl:
            r = await cl.get(f"{_OKX}/market/ticker",
                             params={"instId": _inst(symbol)})
            if r.status_code == 200:
                j = r.json()
                if j.get("code")=="0" and j.get("data"):
                    return float(j["data"][0].get("last",0))
    except Exception as e:
        logger.warning(f"[Price] {symbol}: {e}")
    return 0.0

# ── Live candles ──────────────────────────────────────────────────────────────
async def get_candles(symbol: str, tf: str = "1h",
                       limit: int = 200) -> Optional[pd.DataFrame]:
    """OKX candles → DataFrame (oldest first)."""
    try:
        bar = _TFM.get(tf.lower(), tf)
        async with httpx.AsyncClient(timeout=12, headers=_HDR) as cl:
            r = await cl.get(f"{_OKX}/market/candles", params={
                "instId": _inst(symbol), "bar": bar, "limit": str(min(limit,300))
            })
            if r.status_code != 200: return None
            j = r.json()
            if j.get("code")!="0" or not j.get("data"): return None
            rows = []
            for c in reversed(j["data"]):
                try:
                    rows.append({"ts":int(c[0]),"open":float(c[1]),"high":float(c[2]),
                                  "low":float(c[3]),"close":float(c[4]),"volume":float(c[5])})
                except: continue
            if len(rows)<10: return None
            df = pd.DataFrame(rows)
            df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            return df.set_index("ts")
    except Exception as e:
        logger.warning(f"[Candles] {symbol} {tf}: {e}")
    return None

# ── USDT Dominance ────────────────────────────────────────────────────────────
async def get_macro_context() -> Dict:
    """Fetch real-time USDT.D and BTC.D from CoinGecko."""
    try:
        async with httpx.AsyncClient(timeout=10) as cl:
            r = await cl.get("https://api.coingecko.com/api/v3/global",
                             headers={"Accept":"application/json"})
            if r.status_code == 200:
                d = r.json().get("data",{})
                dom = d.get("market_cap_percentage",{})
                ud = dom.get("usdt",7.0); bd = dom.get("btc",50.0)
                return {"usdt_d":round(ud,3),"btc_d":round(bd,2),
                        "crypto_bias":"BEAR" if ud>7.5 else "BULL" if ud<6.0 else "NEUTRAL",
                        "ok":True}
    except Exception as e:
        logger.warning(f"[Macro] {e}")
    return {"usdt_d":7.0,"btc_d":50.0,"crypto_bias":"NEUTRAL","ok":False}

# ── Signal generation (wraps backtest logic) ──────────────────────────────────
async def generate_signal(symbol: str,
                            engine_type: str = "auto") -> Optional[Dict]:
    """
    جلب البيانات الحية وتشغيل نفس الخوارزمية من backtest.
    يُعيد dict إشارة جاهزة أو None.
    """
    from backtesting.backtest_engine import (
        _build, _add_div, build_alt, build_macro,
        _elite_signal, alt_signal, macro_signal,
        BTConfig, AltConfig, MacroConfig,
        _alt_cfg_for, _macro_cfg_for, zigzag_dev,
        _confirmed, FIBO_RET, FIBO_EXT
    )

    sym_c = symbol.replace("/USDT:USDT","").replace("/USDT","").split(":")[0]

    # Determine engine
    if engine_type == "auto":
        if _is_macro(sym_c): engine_type = "MACRO"
        elif _is_alt(sym_c): engine_type  = "ALT"
        else:                engine_type  = "ELITE"

    # Fetch candles
    df_1h = await get_candles(symbol, "1h", 300)
    df_4h = await get_candles(symbol, "4h", 100)
    if df_1h is None or len(df_1h) < 100:
        logger.warning(f"[Live] {sym_c}: insufficient 1H data")
        return None

    # Reset index for backtest functions
    df_1h = df_1h.reset_index()
    if df_4h is not None: df_4h = df_4h.reset_index()

    # Build indicators (same as backtest)
    if engine_type == "ELITE":
        cfg = BTConfig()
        df  = _build(df_1h); df = _add_div(df)
        df_mtf = _build(df_4h) if df_4h is not None else None
        z = _confirmed(zigzag_dev(df), len(df)-1)
        i = len(df)-1
        sig, score, dets = _elite_signal(df, z, i, cfg, df_mtf)

    elif engine_type == "ALT":
        cfg = _alt_cfg_for(sym_c, AltConfig())
        df  = build_alt(df_1h); df = _add_div(df)
        df_mtf = build_alt(df_4h) if df_4h is not None else None
        z = _confirmed(zigzag_dev(df), len(df)-1)
        i = len(df)-1
        sig, score, dets = alt_signal(df, z, i, cfg, df_mtf)

    else:  # MACRO
        cfg = _macro_cfg_for(sym_c, MacroConfig())
        df  = build_macro(df_1h); df = _add_div(df)
        df_mtf = build_macro(df_4h) if df_4h is not None else None
        z = _confirmed(zigzag_dev(df), len(df)-1)
        i = len(df)-1
        sig, score, dets = macro_signal(df, z, i, cfg, df_mtf)

    if not sig or sig == "NEUTRAL":
        return None

    price = float(df.close.iloc[-1])
    atr   = float(df.atr.iloc[-1]) if "atr" in df.columns else price*0.015
    atr   = atr if atr > 0 else price*0.015

    # SL/TP using backtest functions
    if engine_type == "ELITE":
        from backtesting.backtest_engine import _fib_sl, _fib_tps
        hi60 = float(df.high.iloc[max(0,i-55):i+1].max())
        lo60 = float(df.low.iloc[max(0,i-55):i+1].min())
        sl, sl_d = _fib_sl(price, hi60, lo60, sig, df, i, atr)
        tp1, tp2, tp3 = _fib_tps(price, sl_d, sig, hi60, lo60)
    elif engine_type == "ALT":
        from backtesting.backtest_engine import alt_sl_tp
        # Find last pivot as SL anchor
        sl_anchor = float(df.slo14.iloc[i]) if sig=="LONG" else float(df.shi14.iloc[i])
        sl, tp1, tp2, tp3, sl_d = alt_sl_tp(price, sig, sl_anchor, atr, cfg, df, i)
    else:
        from backtesting.backtest_engine import macro_sl_tp
        sl_anchor = float(df.slo14.iloc[i]) if sig=="LONG" else float(df.shi14.iloc[i])
        sl, tp1, tp2, tp3, sl_d = macro_sl_tp(price, sig, sl_anchor, atr, cfg, df, i)

    rr = abs(tp1-price)/max(sl_d, 1e-10)
    if rr < 1.1:
        logger.debug(f"[Live] {sym_c}: RR={rr:.2f} too low")
        return None

    return {
        "symbol":      symbol,
        "symbol_clean":sym_c,
        "direction":   sig,
        "engine":      engine_type,
        "entry_price": round(price, 6),
        "sl_price":    round(sl,    6),
        "tp1_price":   round(tp1,   6),
        "tp2_price":   round(tp2,   6),
        "tp3_price":   round(tp3,   6),
        "sl_dist":     round(sl_d,  6),
        "rr":          round(rr,    2),
        "score":       round(score, 4),
        "atr":         round(atr,   6),
        "ts":          datetime.now(timezone.utc).isoformat(),
        "details":     dets,
    }

# ── Risk sizing ───────────────────────────────────────────────────────────────
def calc_size(balance: float, risk_frac: float,
               price: float, sl_dist: float) -> float:
    """نفس منطق الـ backtest: 1.5% من الرصيد على SL."""
    risk_usdt = balance * risk_frac
    size_usdt = risk_usdt / (sl_dist / price) if sl_dist > 0 else balance * 0.05
    return round(min(size_usdt, balance * 0.25), 2)  # max 25% per trade

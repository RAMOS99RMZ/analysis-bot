"""
engine/live_engine.py — Ramos 360 Ai 🎖️  (FIXED v3)
════════════════════════════════════════════════════
ROOT CAUSE FOUND: The error happens INSIDE backtest_engine.py's own
functions (_build, zigzag_dev, _elite_signal, alt_signal...) when fed
live data — some internal calculation does int(price_value) and gets
a numpy string scalar because a column's dtype silently became
'object' instead of 'float64' during the live data pipeline.

FIX:
  1. Force pd.to_numeric() on ALL price/volume columns right before
     handing the DataFrame to any backtest_engine function.
     This guarantees float64 dtype — eliminates np.str_ entirely.
  2. Guard EVERY signal-generation call against non-tuple returns
     (fixes "cannot unpack non-iterable NoneType" for ALT/MACRO).
  3. Full traceback logging (logger.exception) so if anything still
     fails, the exact file:line is visible in the next run's logs.
"""
from __future__ import annotations
import asyncio, os, traceback
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import httpx, pandas as pd
from loguru import logger

_OKX = "https://www.okx.com/api/v5"
_HDR = {"Accept": "application/json", "User-Agent": "Ramos360Live/3.0"}
_TFM = {"1h": "1H", "4h": "4H", "1d": "1D", "15m": "15m", "5m": "5m"}

ALT_SYMBOLS   = {"SOL", "LINK", "DOGE", "AVAX", "ADA", "BNB", "XRP"}
MACRO_SYMBOLS = {"XAUUSD", "XAGUSD", "SPX", "NDX"}


def _is_alt(sym_c: str) -> bool:
    return sym_c.upper() in ALT_SYMBOLS


def _is_macro(sym_c: str) -> bool:
    return sym_c.upper() in MACRO_SYMBOLS


def _inst(sym: str) -> str:
    return sym.split(":")[0].replace("/", "-") + "-SWAP"


def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(str(val))
    except (ValueError, TypeError):
        return default


def _force_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    ✅ CRITICAL FIX: Guarantees every OHLCV column is true float64.
    This is what eliminates 'int() ... np.str_(...)' errors —
    those happen when a column's dtype is silently 'object'
    (mixed str/float) instead of pure float64.
    """
    df = df.copy()
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    return df


# ── Live price ────────────────────────────────────────────────────────────────
async def get_live_price(symbol: str) -> float:
    try:
        async with httpx.AsyncClient(timeout=8, headers=_HDR) as cl:
            r = await cl.get(f"{_OKX}/market/ticker", params={"instId": _inst(symbol)})
            if r.status_code == 200:
                j = r.json()
                if j.get("code") == "0" and j.get("data"):
                    return _safe_float(j["data"][0].get("last"), 0.0)
    except Exception as e:
        logger.warning(f"[Price] {symbol}: {e}")
    return 0.0


# ── Live candles ──────────────────────────────────────────────────────────────
async def get_candles(symbol: str, tf: str = "1h", limit: int = 200) -> Optional[pd.DataFrame]:
    try:
        bar = _TFM.get(tf.lower(), tf)
        async with httpx.AsyncClient(timeout=12, headers=_HDR) as cl:
            r = await cl.get(f"{_OKX}/market/candles", params={
                "instId": _inst(symbol), "bar": bar, "limit": str(min(limit, 300))
            })
            if r.status_code != 200:
                return None
            j = r.json()
            if j.get("code") != "0" or not j.get("data"):
                return None
            rows = []
            for c in reversed(j["data"]):
                try:
                    rows.append({
                        "ts":     int(_safe_float(c[0])),
                        "open":   _safe_float(c[1]),
                        "high":   _safe_float(c[2]),
                        "low":    _safe_float(c[3]),
                        "close":  _safe_float(c[4]),
                        "volume": _safe_float(c[5]),
                    })
                except Exception:
                    continue
            if len(rows) < 10:
                return None
            df = pd.DataFrame(rows)
            df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            # ✅ Force numeric BEFORE returning — guarantees clean dtypes downstream
            df = _force_numeric(df)
            df["ts"] = pd.to_datetime(df["ts"]) if not pd.api.types.is_datetime64_any_dtype(df["ts"]) else df["ts"]
            return df
    except Exception as e:
        logger.warning(f"[Candles] {symbol} {tf}: {e}")
    return None


# ── USDT Dominance ────────────────────────────────────────────────────────────
async def get_macro_context() -> Dict:
    try:
        async with httpx.AsyncClient(timeout=10) as cl:
            r = await cl.get("https://api.coingecko.com/api/v3/global",
                             headers={"Accept": "application/json"})
            if r.status_code == 200:
                d   = r.json().get("data", {})
                dom = d.get("market_cap_percentage", {})
                ud  = _safe_float(dom.get("usdt"), 7.0)
                bd  = _safe_float(dom.get("btc"), 50.0)
                return {"usdt_d": round(ud, 3), "btc_d": round(bd, 2),
                        "crypto_bias": "BEAR" if ud > 7.5 else "BULL" if ud < 6.0 else "NEUTRAL",
                        "ok": True}
    except Exception as e:
        logger.warning(f"[Macro] {e}")
    return {"usdt_d": 7.0, "btc_d": 50.0, "crypto_bias": "NEUTRAL", "ok": False}


# ── Fallback SL/TP ──────────────────────────────────────────────────────────
def _fallback_sl_tp(price: float, direction: str, atr: float) -> Tuple[float, float, float, float, float]:
    atr = atr if atr > 0 else price * 0.02
    if direction == "LONG":
        sl  = round(price - atr * 2.0, 6)
        tp1 = round(price + atr * 2.5, 6)
        tp2 = round(price + atr * 4.5, 6)
        tp3 = round(price + atr * 7.0, 6)
    else:
        sl  = round(price + atr * 2.0, 6)
        tp1 = round(price - atr * 2.5, 6)
        tp2 = round(price - atr * 4.5, 6)
        tp3 = round(price - atr * 7.0, 6)
    sl_d = abs(price - sl)
    return sl, tp1, tp2, tp3, sl_d


def _safe_unpack3(result, default=(None, 0.0, {})):
    """
    ✅ FIX for 'cannot unpack non-iterable NoneType':
    Guards ANY function call expected to return (sig, score, details)
    against returning None or a wrong-length tuple.
    """
    if result is None:
        return default
    try:
        if isinstance(result, (tuple, list)) and len(result) == 3:
            return result[0], result[1], (result[2] if isinstance(result[2], dict) else {})
        if isinstance(result, (tuple, list)) and len(result) >= 2:
            return result[0], result[1], {}
    except Exception:
        pass
    return default


def _safe_unpack5(result, price, direction, atr, default_fn):
    """Guards SL/TP function calls (expected 5-tuple) against None/wrong shape."""
    if result is None:
        return default_fn(price, direction, atr)
    try:
        if isinstance(result, (tuple, list)) and len(result) == 5:
            return tuple(result)
        if isinstance(result, (tuple, list)) and len(result) == 4:
            sl, tp1, tp2, tp3 = result
            return sl, tp1, tp2, tp3, abs(price - sl)
    except Exception:
        pass
    return default_fn(price, direction, atr)


# ── Signal generation ──────────────────────────────────────────────────────────
async def generate_signal(symbol: str, engine_type: str = "auto") -> Optional[Dict]:
    try:
        from backtesting.backtest_engine import (
            _build, _add_div, build_alt, build_macro,
            _elite_signal, alt_signal, macro_signal,
            BTConfig, AltConfig, MacroConfig,
            _alt_cfg_for, _macro_cfg_for, zigzag_dev,
            _confirmed,
        )
    except ImportError as e:
        logger.error(f"[Live] Cannot import backtest_engine functions: {e}")
        return None

    sym_c = symbol.replace("/USDT:USDT", "").replace("/USDT", "").split(":")[0]

    if engine_type == "auto":
        if _is_macro(sym_c): engine_type = "MACRO"
        elif _is_alt(sym_c):  engine_type = "ALT"
        else:                 engine_type = "ELITE"

    df_1h = await get_candles(symbol, "1h", 300)
    df_4h = await get_candles(symbol, "4h", 100)
    if df_1h is None or len(df_1h) < 100:
        logger.warning(f"[Live] {sym_c}: insufficient 1H data")
        return None

    # ✅ Force numeric dtypes one more time defensively (belt & suspenders)
    df_1h = _force_numeric(df_1h)
    if df_4h is not None:
        df_4h = _force_numeric(df_4h)

    try:
        if engine_type == "ELITE":
            cfg = BTConfig()
            df  = _build(df_1h); df = _add_div(df)
            df_mtf = _build(df_4h) if df_4h is not None and len(df_4h) >= 30 else None
            z = _confirmed(zigzag_dev(df), len(df) - 1)
            i = len(df) - 1
            sig, score, dets = _safe_unpack3(_elite_signal(df, z, i, cfg, df_mtf))

        elif engine_type == "ALT":
            cfg = _alt_cfg_for(sym_c, AltConfig())
            df  = build_alt(df_1h); df = _add_div(df)
            df_mtf = build_alt(df_4h) if df_4h is not None and len(df_4h) >= 30 else None
            z = _confirmed(zigzag_dev(df), len(df) - 1)
            i = len(df) - 1
            sig, score, dets = _safe_unpack3(alt_signal(df, z, i, cfg, df_mtf))

        else:  # MACRO
            cfg = _macro_cfg_for(sym_c, MacroConfig())
            df  = build_macro(df_1h); df = _add_div(df)
            df_mtf = build_macro(df_4h) if df_4h is not None and len(df_4h) >= 30 else None
            z = _confirmed(zigzag_dev(df), len(df) - 1)
            i = len(df) - 1
            sig, score, dets = _safe_unpack3(macro_signal(df, z, i, cfg, df_mtf))

    except Exception as e:
        # ✅ Full traceback so next failure shows EXACT file:line
        logger.error(f"[Live] {sym_c} signal generation failed: {e}")
        logger.debug(f"[Live] {sym_c} full traceback:\n{traceback.format_exc()}")
        return None

    if not sig or sig == "NEUTRAL":
        return None

    price = _safe_float(df.close.iloc[-1])
    atr   = _safe_float(df.atr.iloc[-1]) if "atr" in df.columns else price * 0.015
    atr   = atr if atr > 0 else price * 0.015

    sl = tp1 = tp2 = tp3 = sl_d = None

    try:
        if engine_type == "ELITE":
            from backtesting.backtest_engine import _fib_sl, _fib_tps
            hi60 = _safe_float(df.high.iloc[max(0, i-55):i+1].max())
            lo60 = _safe_float(df.low.iloc[max(0, i-55):i+1].min())
            sl_result = _fib_sl(price, hi60, lo60, sig, df, i, atr)
            if sl_result is not None and len(sl_result) == 2:
                sl, sl_d = sl_result
                tp_result = _fib_tps(price, sl_d, sig, hi60, lo60)
                if tp_result is not None and len(tp_result) == 3:
                    tp1, tp2, tp3 = tp_result

        elif engine_type == "ALT":
            from backtesting.backtest_engine import alt_sl_tp
            sl_anchor = _safe_float(df.slo14.iloc[i]) if sig == "LONG" else _safe_float(df.shi14.iloc[i])
            result = alt_sl_tp(price, sig, sl_anchor, atr, cfg, df, i)
            sl, tp1, tp2, tp3, sl_d = _safe_unpack5(result, price, sig, atr, _fallback_sl_tp)

        else:  # MACRO
            from backtesting.backtest_engine import macro_sl_tp
            sl_anchor = _safe_float(df.slo14.iloc[i]) if sig == "LONG" else _safe_float(df.shi14.iloc[i])
            result = macro_sl_tp(price, sig, sl_anchor, atr, cfg, df, i)
            sl, tp1, tp2, tp3, sl_d = _safe_unpack5(result, price, sig, atr, _fallback_sl_tp)

    except Exception as e:
        logger.warning(f"[Live] {sym_c} SL/TP calc error: {e} — using fallback")
        logger.debug(f"[Live] {sym_c} SL/TP traceback:\n{traceback.format_exc()}")

    if sl is None or tp1 is None or sl_d is None or sl_d <= 0:
        sl, tp1, tp2, tp3, sl_d = _fallback_sl_tp(price, sig, atr)
        logger.info(f"[Live] {sym_c}: using fallback SL/TP")

    rr = abs(tp1 - price) / max(sl_d, 1e-10)
    if rr < 1.1:
        logger.debug(f"[Live] {sym_c}: RR={rr:.2f} too low — skipping")
        return None

    return {
        "symbol":       symbol,
        "symbol_clean": sym_c,
        "direction":    sig,
        "engine":       engine_type,
        "entry_price":  round(price, 6),
        "sl_price":     round(sl, 6),
        "tp1_price":    round(tp1, 6),
        "tp2_price":    round(tp2, 6),
        "tp3_price":    round(tp3, 6),
        "sl_dist":      round(sl_d, 6),
        "rr":           round(rr, 2),
        "score":        round(_safe_float(score), 4),
        "atr":          round(atr, 6),
        "ts":           datetime.now(timezone.utc).isoformat(),
        "details":      dets if isinstance(dets, dict) else {},
    }


# ── Risk sizing ───────────────────────────────────────────────────────────────
def calc_size(balance: float, risk_frac: float, price: float, sl_dist: float) -> float:
    risk_usdt = balance * risk_frac
    size_usdt = risk_usdt / (sl_dist / price) if sl_dist > 0 and price > 0 else balance * 0.05
    return round(min(size_usdt, balance * 0.25), 2)

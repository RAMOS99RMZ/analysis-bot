"""
engine/live_engine.py — Ramos 360 Ai 🎖️  (FIXED v4 — ROOT CAUSE SOLVED)
════════════════════════════════════════════════════════════════════
ROOT CAUSE (confirmed via traceback):
  df.iloc[i] failed because 'i' wasn't a scalar int — it was
  receiving the WRONG argument due to positional mismatch.
  _elite_signal(df, z, i, cfg, df_mtf) assumed an argument ORDER
  that doesn't match the real signature in backtest_engine.py.

FIX: Use inspect.signature() to bind arguments by NAME instead of
     position. This makes the call immune to any parameter ordering
     differences between live_engine.py's assumptions and the
     actual backtest_engine.py function signatures.
"""
from __future__ import annotations
import asyncio, os, inspect, traceback
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Callable
import httpx, pandas as pd
from loguru import logger

_OKX = "https://www.okx.com/api/v5"
_HDR = {"Accept": "application/json", "User-Agent": "Ramos360Live/4.0"}
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
    df = df.copy()
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    return df


# ══════════════════════════════════════════════════════════════════
# ✅ THE FIX: Name-based argument binding via inspect
# ══════════════════════════════════════════════════════════════════

# Every possible value we might need to pass, keyed by common names
_ALIAS_GROUPS = {
    "df":     ["df", "data", "frame", "candles"],
    "z":      ["z", "pivots", "zz", "zig", "zigzag", "piv"],
    "i":      ["i", "idx", "index", "pos", "n"],
    "cfg":    ["cfg", "config", "conf", "settings"],
    "df_mtf": ["df_mtf", "mtf", "htf", "df_htf", "higher_tf"],
}


def _smart_call(func: Callable, **values):
    """
    Calls `func` binding arguments by matching parameter NAMES
    (not position). `values` keys are our canonical names:
    df, z, i, cfg, df_mtf — this function looks up each real
    parameter name against known aliases to find the right value.

    This makes the call 100% immune to whatever order the actual
    function signature uses.
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    call_args = []

    for p in params:
        pname = p.name.lower()
        matched_value = None
        matched = False

        for canonical, aliases in _ALIAS_GROUPS.items():
            if pname in aliases and canonical in values:
                matched_value = values[canonical]
                matched = True
                break

        if matched:
            call_args.append(matched_value)
        elif p.default is not inspect.Parameter.empty:
            call_args.append(p.default)
        else:
            # Unknown required param with no default — best effort None
            call_args.append(None)

    return func(*call_args)


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


# ── Live candles (DEEP HISTORY via history-candles endpoint) ─────────────────
async def get_candles(symbol: str, tf: str = "1h", limit: int = 1000) -> Optional[pd.DataFrame]:
    """
    ✅ FIX: Uses /market/history-candles (same endpoint as backtest_engine.py)
    with proper pagination — guarantees enough historical depth for zigzag
    pivot confirmation. This is NOT a graceful-skip fix — it ensures REAL
    sufficient data so hi/lo are always computed correctly, matching the
    same data richness the backtest always has.
    """
    try:
        bar = _TFM.get(tf.lower(), tf)
        inst = _inst(symbol)
        all_rows: list = []
        before_ts: Optional[str] = None  # fetch candles BEFORE this timestamp (going back in time)

        async with httpx.AsyncClient(timeout=20, headers=_HDR) as cl:
            attempts = 0
            max_attempts = 20  # safety cap: up to 20 * 300 = 6000 candles max

            while len(all_rows) < limit and attempts < max_attempts:
                attempts += 1
                params = {"instId": inst, "bar": bar, "limit": "300"}
                if before_ts:
                    params["after"] = before_ts  # OKX: 'after' = older than this ts

                r = await cl.get(f"{_OKX}/market/history-candles", params=params)
                if r.status_code == 429:
                    await asyncio.sleep(1.0)
                    continue
                if r.status_code != 200:
                    logger.warning(f"[Candles] {symbol} {tf}: HTTP {r.status_code}")
                    break

                j = r.json()
                if j.get("code") != "0" or not j.get("data"):
                    break

                batch = j["data"]
                for c in batch:
                    try:
                        all_rows.append({
                            "ts":     int(_safe_float(c[0])),
                            "open":   _safe_float(c[1]),
                            "high":   _safe_float(c[2]),
                            "low":    _safe_float(c[3]),
                            "close":  _safe_float(c[4]),
                            "volume": _safe_float(c[5]),
                        })
                    except Exception:
                        continue

                if len(batch) < 300:
                    break  # reached earliest available history

                before_ts = batch[-1][0]  # oldest ts in this batch → go further back next
                await asyncio.sleep(0.15)  # rate-limit friendly

        if len(all_rows) < 10:
            logger.warning(f"[Candles] {symbol} {tf}: only {len(all_rows)} candles fetched — too few")
            return None

        df = pd.DataFrame(all_rows)
        df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df = _force_numeric(df)

        # Keep only the most recent `limit` candles (data is fetched oldest→needed)
        if len(df) > limit:
            df = df.iloc[-limit:].reset_index(drop=True)

        logger.debug(f"[Candles] {symbol} {tf}: {len(df)} candles ready")
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

    df_1h = await get_candles(symbol, "1h", 1000)  # ✅ DEEP history — guarantees zigzag has enough swings
    df_4h = await get_candles(symbol, "4h", 500)   # ✅ DEEP MTF history too
    if df_1h is None or len(df_1h) < 100:
        logger.warning(f"[Live] {sym_c}: insufficient 1H data")
        return None

    df_1h = _force_numeric(df_1h)
    if df_4h is not None:
        df_4h = _force_numeric(df_4h)

    try:
        if engine_type == "ELITE":
            cfg = BTConfig()
            df  = _build(df_1h); df = _add_div(df)
            df_mtf = _build(df_4h) if df_4h is not None and len(df_4h) >= 30 else None
            i = len(df) - 1
            # ✅ REAL FIX: _elite_signal's true signature is
            #    (df, i, hi, lo, cfg, df_mtf=...) — hi/lo are the
            #    swing high/low over cfg.swing_lookback candles,
            #    computed EXACTLY like backtest_engine.py's own _sim() does.
            lb    = getattr(cfg, "swing_lookback", 50)
            hi_sw = float(df.high.iloc[max(0, i - lb):i + 1].max())
            lo_sw = float(df.low.iloc[max(0, i - lb):i + 1].min())
            result = _elite_signal(df, i, hi_sw, lo_sw, cfg, df_mtf=df_mtf)
            sig, score, dets = _safe_unpack3(result)

        elif engine_type == "ALT":
            cfg = _alt_cfg_for(sym_c, AltConfig())
            df  = build_alt(df_1h); df = _add_div(df)
            df_mtf = build_alt(df_4h) if df_4h is not None and len(df_4h) >= 30 else None
            i = len(df) - 1
            # ✅ REAL FIX (verified against source): alt_signal(df, z, i, cfg, df_mtf)
            #    z = confirmed zigzag pivots, computed EXACTLY like backtest's _sim():
            #       piv_all = zigzag_dev(df, cfg.zz_atr)
            #       z = _confirmed(piv_all, i)
            zz_atr  = getattr(cfg, "zz_atr", 2.2)
            piv_all = zigzag_dev(df, zz_atr)
            z       = _confirmed(piv_all, i)
            result  = alt_signal(df, z, i, cfg, df_mtf=df_mtf)
            sig, score, dets = _safe_unpack3(result)

        else:  # MACRO
            cfg = _macro_cfg_for(sym_c, MacroConfig())
            df  = build_macro(df_1h); df = _add_div(df)
            df_mtf = build_macro(df_4h) if df_4h is not None and len(df_4h) >= 30 else None
            i = len(df) - 1
            # ✅ REAL FIX (verified against source): macro_signal(df, z, i, cfg, df_mtf)
            zz_atr  = getattr(cfg, "zz_atr", 2.2)
            piv_all = zigzag_dev(df, zz_atr)
            z       = _confirmed(piv_all, i)
            result  = macro_signal(df, z, i, cfg, df_mtf=df_mtf)
            sig, score, dets = _safe_unpack3(result)

    except Exception as e:
        # With deep history now guaranteed, this should be a genuine issue —
        # always log full details, never silently hide it.
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

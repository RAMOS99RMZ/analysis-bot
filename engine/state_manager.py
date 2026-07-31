"""
engine/state_manager.py — Ramos 360 Ai 🎖️  (FIXED)
════════════════════════════════════════════════════
FIX: [Errno -2] Name or service not known
     → SUPABASE_URL was empty/malformed, added validation + clear error
"""
from __future__ import annotations
import asyncio, os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import httpx
from loguru import logger

_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
_KEY = os.getenv("SUPABASE_KEY", "").strip()

_CONFIGURED = bool(_URL and _KEY and _URL.startswith("http"))

if not _CONFIGURED:
    logger.warning(
        f"[State] ⚠️  Supabase NOT configured properly. "
        f"URL={'set' if _URL else 'MISSING'} KEY={'set' if _KEY else 'MISSING'}. "
        f"Check GitHub Secrets: SUPABASE_URL must start with https:// "
        f"and SUPABASE_KEY must be the service_role key."
    )

_HDR = {
    "apikey":        _KEY,
    "Authorization": f"Bearer {_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=representation",
}


# ══════════════════════════════════════════════════════════════════
# LOW-LEVEL REST HELPERS
# ══════════════════════════════════════════════════════════════════

async def _get(table: str, params: str = "") -> List[Dict]:
    if not _CONFIGURED:
        return []
    try:
        async with httpx.AsyncClient(timeout=10) as cl:
            r = await cl.get(f"{_URL}/rest/v1/{table}?{params}", headers=_HDR)
            if r.status_code == 200:
                return r.json() or []
            logger.warning(f"[State] GET {table}: HTTP {r.status_code} — {r.text[:150]}")
    except httpx.ConnectError as e:
        logger.error(f"[State] GET {table}: Cannot connect — check SUPABASE_URL is correct: {e}")
    except Exception as e:
        logger.warning(f"[State] GET {table}: {e}")
    return []


async def _post(table: str, data: Dict) -> bool:
    if not _CONFIGURED:
        return False
    data.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    try:
        async with httpx.AsyncClient(timeout=10) as cl:
            r = await cl.post(f"{_URL}/rest/v1/{table}", json=data, headers=_HDR)
            if r.status_code in (200, 201):
                return True
            logger.warning(f"[State] POST {table}: HTTP {r.status_code} — {r.text[:150]}")
    except httpx.ConnectError as e:
        logger.error(f"[State] POST {table}: Cannot connect — check SUPABASE_URL: {e}")
    except Exception as e:
        logger.warning(f"[State] POST {table}: {e}")
    return False


async def _patch(table: str, filters: str, data: Dict) -> bool:
    if not _CONFIGURED:
        return False
    hdrs = {**_HDR, "Prefer": "return=minimal"}
    try:
        async with httpx.AsyncClient(timeout=10) as cl:
            r = await cl.patch(f"{_URL}/rest/v1/{table}?{filters}", json=data, headers=hdrs)
            return r.status_code in (200, 204)
    except httpx.ConnectError as e:
        logger.error(f"[State] PATCH {table}: Cannot connect: {e}")
    except Exception as e:
        logger.warning(f"[State] PATCH {table}: {e}")
    return False


# ══════════════════════════════════════════════════════════════════
# OPEN TRADES STATE
# ══════════════════════════════════════════════════════════════════

async def load_open_trades() -> List[Dict]:
    rows = await _get("trades", "status=eq.OPEN&order=created_at.asc&limit=100")
    logger.info(f"[State] Loaded {len(rows)} open trades from Supabase")
    return rows


async def save_trade(trade: Dict) -> bool:
    record = {
        "symbol":      trade.get("symbol"),
        "direction":   trade.get("direction"),
        "trade_type":  trade.get("trade_type", "Scalp"),
        "engine":      trade.get("engine", "ELITE"),
        "status":      "OPEN",
        "entry_price": trade.get("entry_price") or trade.get("entry"),
        "sl_price":    trade.get("sl_price")    or trade.get("sl"),
        "tp1_price":   trade.get("tp1_price")   or trade.get("tp1"),
        "tp2_price":   trade.get("tp2_price")   or trade.get("tp2"),
        "tp3_price":   trade.get("tp3_price")   or trade.get("tp3"),
        "trail_sl":    trade.get("trail_sl"),
        "partial_done":trade.get("partial_done", False),
        "hit_tp1":     False,
        "hit_tp2":     False,
        "size_usdt":   trade.get("size_usdt"),
        "risk_frac":   trade.get("risk_frac", 0.018),
        "score":       trade.get("score"),
        "run_id":      trade.get("run_id"),
        "tg_message_id": trade.get("tg_message_id"),
    }
    ok = await _post("trades", record)
    if ok:
        logger.info(f"[State] ✅ Saved OPEN trade: {record['symbol']} {record['direction']}")
    else:
        logger.warning(f"[State] ❌ Failed to save trade: {record['symbol']}")
    return ok


async def update_trade(trade_id: int, updates: Dict) -> bool:
    ok = await _patch("trades", f"id=eq.{trade_id}", updates)
    if ok:
        logger.info(f"[State] ✅ Updated trade {trade_id}: {updates}")
    return ok


async def close_trade(trade_id: int, exit_price: float,
                       pnl_pct: float, exit_type: str, result: str) -> bool:
    return await update_trade(trade_id, {
        "status":     "CLOSED",
        "exit_price": exit_price,
        "pnl_pct":    round(pnl_pct, 4),
        "exit_type":  exit_type,
        "result":     result,
        "closed_at":  datetime.now(timezone.utc).isoformat(),
    })


async def partial_close_trade(trade_id: int, tp_level: str, exit_price: float) -> bool:
    updates: Dict[str, Any] = {}
    if tp_level == "TP1":
        updates["hit_tp1"]      = True
        updates["partial_done"] = True
        updates["sl_price"]     = exit_price
    elif tp_level == "TP2":
        updates["hit_tp2"] = True
    updates["status"] = "PARTIAL"
    return await update_trade(trade_id, updates)


async def update_trailing_sl(trade_id: int, new_sl: float) -> bool:
    return await update_trade(trade_id, {"sl_price": new_sl, "trail_sl": new_sl})


# ══════════════════════════════════════════════════════════════════
# MONITOR ENGINE
# ══════════════════════════════════════════════════════════════════

async def monitor_open_trades(get_price_fn) -> Dict:
    trades = await load_open_trades()
    if not trades:
        logger.info("[Monitor] No open trades to check.")
        return {"checked": 0, "updates": []}

    updates = []
    for t in trades:
        tid       = t.get("id")
        sym       = t.get("symbol", "")
        direction = t.get("direction", "LONG")
        entry     = float(t.get("entry_price", 0) or 0)
        sl        = float(t.get("sl_price", 0)    or 0)
        tp1       = float(t.get("tp1_price", 0)   or 0)
        tp2       = float(t.get("tp2_price", 0)   or 0)
        tp3       = float(t.get("tp3_price", 0)   or 0)
        hit_tp1   = bool(t.get("hit_tp1", False))
        hit_tp2   = bool(t.get("hit_tp2", False))
        trail_sl  = float(t.get("trail_sl", 0)  or 0)

        if not sym or not entry or not tid:
            continue

        try:
            price = await get_price_fn(sym)
        except Exception as e:
            logger.warning(f"[Monitor] {sym}: price fetch failed: {e}")
            continue
        if not price or price <= 0:
            continue

        pnl_pct = ((price-entry)/entry*100) if direction == "LONG" \
                   else ((entry-price)/entry*100)

        action = None

        if direction == "LONG":
            if sl and price <= sl:
                etype  = "SL" if not hit_tp1 else ("BE_STOP" if not hit_tp2 else "TRAIL_STOP")
                result = "WIN" if pnl_pct > 0 else "LOSS"
                await close_trade(tid, price, pnl_pct, etype, result)
                action = f"CLOSED({etype}) @ ${price:.2f} PnL={pnl_pct:+.2f}%"
            elif tp3 and price >= tp3 and hit_tp1 and hit_tp2:
                await close_trade(tid, tp3, pnl_pct, "TP3", "WIN")
                action = f"TP3 @ ${tp3:.2f}"
            elif tp2 and price >= tp2 and hit_tp1 and not hit_tp2:
                await partial_close_trade(tid, "TP2", tp2)
                action = f"TP2 @ ${tp2:.2f}"
            elif tp1 and price >= tp1 and not hit_tp1:
                await partial_close_trade(tid, "TP1", entry)
                action = f"TP1 @ ${tp1:.2f} → SL moved to breakeven"
            elif hit_tp2 and price > entry:
                atr_approx = price * 0.015
                new_sl = round(price - 3 * atr_approx, 4)
                if new_sl > (trail_sl or sl):
                    await update_trailing_sl(tid, new_sl)
                    action = f"TRAIL_SL → ${new_sl:.2f}"
        else:
            if sl and price >= sl:
                etype  = "SL" if not hit_tp1 else ("BE_STOP" if not hit_tp2 else "TRAIL_STOP")
                result = "WIN" if pnl_pct > 0 else "LOSS"
                await close_trade(tid, price, pnl_pct, etype, result)
                action = f"CLOSED({etype}) @ ${price:.2f} PnL={pnl_pct:+.2f}%"
            elif tp3 and price <= tp3 and hit_tp1 and hit_tp2:
                await close_trade(tid, tp3, pnl_pct, "TP3", "WIN")
                action = f"TP3 @ ${tp3:.2f}"
            elif tp2 and price <= tp2 and hit_tp1 and not hit_tp2:
                await partial_close_trade(tid, "TP2", tp2)
                action = f"TP2 @ ${tp2:.2f}"
            elif tp1 and price <= tp1 and not hit_tp1:
                await partial_close_trade(tid, "TP1", entry)
                action = f"TP1 @ ${tp1:.2f} → SL moved to breakeven"
            elif hit_tp2 and price < entry:
                atr_approx = price * 0.015
                new_sl = round(price + 3 * atr_approx, 4)
                if new_sl < (trail_sl or sl):
                    await update_trailing_sl(tid, new_sl)
                    action = f"TRAIL_SL → ${new_sl:.2f}"

        if action:
            logger.info(f"[Monitor] {sym} {direction}: {action}")
            updates.append({"symbol": sym, "id": tid, "action": action})

    logger.info(f"[Monitor] ✅ Checked {len(trades)} trades, {len(updates)} updates")
    return {"checked": len(trades), "updates": updates}


# ══════════════════════════════════════════════════════════════════
# PORTFOLIO STATE
# ══════════════════════════════════════════════════════════════════

async def get_account_balance() -> float:
    rows = await _get("bot_state", "key=eq.balance&order=created_at.desc&limit=1")
    if rows:
        try:
            return float(rows[0].get("value", 10000))
        except Exception:
            pass
    return 10_000.0


async def save_account_balance(balance: float) -> bool:
    return await _post("bot_state", {"key": "balance", "value": str(round(balance, 2))})


async def get_daily_pnl() -> float:
    today = datetime.now(timezone.utc).date().isoformat()
    rows = await _get("trades", f"status=eq.CLOSED&closed_at=gte.{today}")
    return sum(float(t.get("pnl_pct", 0) or 0) for t in rows)


async def count_open_trades_for(symbol: str = None) -> int:
    if symbol:
        rows = await _get("trades", f"status=eq.OPEN&symbol=eq.{symbol}")
    else:
        rows = await _get("trades", "status=eq.OPEN")
    return len(rows)

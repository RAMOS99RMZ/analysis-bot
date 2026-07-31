"""
scheduler/jobs.py — Ramos 360 Ai 🎖️  (FINAL — bulletproof names)
════════════════════════════════════════════════════════════════
كل الدوال معرّفة مباشرة بأسمائها الحقيقية — لا توجد aliases معلّقة.
"""
from __future__ import annotations
import asyncio, os
from datetime import datetime, timezone, timedelta
from loguru import logger

from engine.live_engine import (
    get_live_price, get_candles, generate_signal,
    get_macro_context, calc_size,
    ALT_SYMBOLS, MACRO_SYMBOLS
)
from engine.state_manager import (
    load_open_trades, save_trade, close_trade,
    partial_close_trade, update_trailing_sl,
    get_account_balance, save_account_balance,
    get_daily_pnl, count_open_trades_for,
    monitor_open_trades, _get,
)

CRYPTO_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT",
    "SOL/USDT:USDT", "LINK/USDT:USDT",
    "DOGE/USDT:USDT",
]

RISK_FRAC    = 0.018
MAX_OPEN     = 3
DD_LIMIT_PCT = 12.0


def _build_signal_msg(sig: dict, trade_type: str = "⚡ Scalp") -> str:
    sym   = sig.get("symbol_clean", sig.get("symbol", "?"))
    d     = sig.get("direction", "?")
    eng   = sig.get("engine", "?")
    entry = sig.get("entry_price", 0)
    sl    = sig.get("sl_price", 0)
    tp1   = sig.get("tp1_price", 0)
    tp2   = sig.get("tp2_price", 0)
    tp3   = sig.get("tp3_price", 0)
    rr    = sig.get("rr", 0)
    score = sig.get("score", 0)
    size  = sig.get("size_usdt", 0)
    icon  = "🟢 LONG" if d == "LONG" else "🔴 SHORT"
    f = lambda v: f"${v:,.4f}" if v < 1000 else f"${v:,.2f}"
    return (
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{trade_type} · <b>{sym}</b> · [{eng}]\n"
        f"{icon}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry:  {f(entry)}\n"
        f"🛑 SL:     {f(sl)}\n"
        f"🎯 TP1:   {f(tp1)}\n"
        f"🎯 TP2:   {f(tp2)}\n"
        f"🎯 TP3:   {f(tp3)}\n"
        f"⚖️ RR:     {rr:.2f}\n"
        f"📊 Score:  {score:.3f}\n"
        f"💰 Size:   ${size:,.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>🎖️ Ramos 360 Ai</i>"
    )


# ══════════════════════════════════════════════════════════════════
# ✅ job_monitor — DEFINED DIRECTLY (not an alias)
# ══════════════════════════════════════════════════════════════════
async def job_monitor(notifier=None):
    """Runs every 1 minute. Checks all open trades for SL/TP hits."""
    logger.info("[Monitor] 🔍 Checking open trades …")
    result = await monitor_open_trades(get_price_fn=get_live_price)
    if not result["updates"]:
        logger.info(f"[Monitor] ✅ No changes. {result['checked']} trades active.")
        return
    if notifier:
        for u in result["updates"]:
            msg = f"📊 <b>{u['symbol']}</b>\n🔔 {u['action']}"
            try:
                await notifier.send(msg)
            except Exception as e:
                logger.warning(f"[Monitor] notify failed: {e}")


# ══════════════════════════════════════════════════════════════════
# ✅ job_scalp — DEFINED DIRECTLY
# ══════════════════════════════════════════════════════════════════
async def job_scalp(notifier=None):
    """Runs every 1h during London+NY sessions (07-16 UTC)."""
    hour = datetime.now(timezone.utc).hour
    if hour not in range(7, 17):
        logger.info(f"[Scalp] Outside trading hours ({hour}:00 UTC)")
        return

    balance    = await get_account_balance()
    daily_pnl  = await get_daily_pnl()
    open_count = len(await load_open_trades())

    if daily_pnl < -DD_LIMIT_PCT:
        logger.warning(f"[Scalp] Daily DD limit hit ({daily_pnl:.1f}%) — skipping")
        return
    if open_count >= MAX_OPEN * len(CRYPTO_SYMBOLS):
        logger.info(f"[Scalp] Max open trades ({open_count}) reached — skipping")
        return

    logger.info(f"[Scalp] Balance=${balance:.2f} OpenTrades={open_count}")

    for sym in CRYPTO_SYMBOLS:
        sym_c = sym.replace("/USDT:USDT", "")
        if await count_open_trades_for(sym) > 0:
            continue
        try:
            sig = await generate_signal(sym)
            if not sig:
                continue

            size_usdt = calc_size(balance, RISK_FRAC,
                                   sig["entry_price"], sig["sl_dist"])
            sig["size_usdt"]  = size_usdt
            sig["risk_frac"]  = RISK_FRAC
            sig["trade_type"] = "Scalp"
            sig["run_id"]     = f"SCALP_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M')}"

            if notifier:
                msg = _build_signal_msg(sig)
                try:
                    tg_id = await notifier.send_get_id(msg)
                    sig["tg_message_id"] = tg_id
                except Exception as e:
                    logger.warning(f"[Scalp] notify failed: {e}")

            await save_trade(sig)
            logger.info(
                f"[Scalp] ✅ {sym_c} {sig['direction']} "
                f"@ ${sig['entry_price']:.4f} RR={sig['rr']:.2f}"
            )
        except Exception as e:
            logger.error(f"[Scalp] {sym_c}: {e}")

    await save_account_balance(balance)


# ══════════════════════════════════════════════════════════════════
# ✅ job_swing — DEFINED DIRECTLY
# ══════════════════════════════════════════════════════════════════
async def job_swing(notifier=None):
    """Runs every 4h. No session restriction."""
    logger.info("[Swing] 🌊 Running swing analysis …")
    balance   = await get_account_balance()
    daily_pnl = await get_daily_pnl()
    if daily_pnl < -DD_LIMIT_PCT:
        logger.warning("[Swing] DD limit — skipping")
        return

    for sym in CRYPTO_SYMBOLS:
        sym_c = sym.replace("/USDT:USDT", "")
        if await count_open_trades_for(sym) > 0:
            continue
        try:
            sig = await generate_signal(sym)
            if not sig or sig.get("rr", 0) < 1.8:
                continue

            sig["trade_type"] = "Swing"
            sig["risk_frac"]  = RISK_FRAC * 1.5
            sig["run_id"]     = f"SWING_{datetime.now(timezone.utc).strftime('%Y%m%d%H')}"
            sig["size_usdt"]  = calc_size(balance, sig["risk_frac"],
                                           sig["entry_price"], sig["sl_dist"])
            if notifier:
                msg = _build_signal_msg(sig, trade_type="🌊 Swing")
                try:
                    tg_id = await notifier.send_get_id(msg)
                    sig["tg_message_id"] = tg_id
                except Exception as e:
                    logger.warning(f"[Swing] notify failed: {e}")

            await save_trade(sig)
            logger.info(f"[Swing] ✅ {sym_c} {sig['direction']} RR={sig['rr']:.2f}")
        except Exception as e:
            logger.error(f"[Swing] {sym_c}: {e}")


# ══════════════════════════════════════════════════════════════════
# ✅ job_daily — DEFINED DIRECTLY
# ══════════════════════════════════════════════════════════════════
async def job_daily(notifier=None):
    """Daily summary report at 00:00 UTC."""
    today = datetime.now(timezone.utc).date().isoformat()
    closed  = await _get("trades", f"status=eq.CLOSED&closed_at=gte.{today}")
    open_t  = await load_open_trades()
    balance = await get_account_balance()
    macro   = await get_macro_context()

    wins   = sum(1 for t in closed if t.get("result") == "WIN")
    losses = sum(1 for t in closed if t.get("result") == "LOSS")
    pnl    = sum(float(t.get("pnl_pct", 0) or 0) for t in closed)
    wr     = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0

    msg = (
        f"📊 <b>Ramos 360 Ai — Daily Report</b>\n"
        f"📅 {today}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💼 Balance:     ${balance:,.2f}\n"
        f"📈 Today PnL:   {pnl:+.2f}%\n"
        f"🎯 Win Rate:    {wr}% ({wins}W/{losses}L)\n"
        f"📂 Open Trades: {len(open_t)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 USDT.D: {macro.get('usdt_d', '?')}% "
        f"→ {macro.get('crypto_bias', '?')}\n"
        f"₿ BTC.D:  {macro.get('btc_d', '?')}%\n"
        f"<i>🎖️ Ramos 360 Ai</i>"
    )
    if notifier:
        try:
            await notifier.send(msg)
        except Exception as e:
            logger.warning(f"[Daily] notify failed: {e}")
    logger.info(f"[Daily] Report sent. PnL={pnl:+.2f}% WR={wr}%")


# ══════════════════════════════════════════════════════════════════
# ✅ job_weekly_report — DEFINED DIRECTLY
# ══════════════════════════════════════════════════════════════════
async def job_weekly_report(notifier=None):
    week_start = (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat()
    closed  = await _get("trades", f"status=eq.CLOSED&closed_at=gte.{week_start}")
    balance = await get_account_balance()

    wins   = sum(1 for t in closed if t.get("result") == "WIN")
    losses = sum(1 for t in closed if t.get("result") == "LOSS")
    pnl    = sum(float(t.get("pnl_pct", 0) or 0) for t in closed)
    wr     = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0

    msg = (
        f"📅 <b>Ramos 360 Ai — Weekly Report</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💼 Balance:  ${balance:,.2f}\n"
        f"📈 Week PnL: {pnl:+.2f}%\n"
        f"🎯 Win Rate: {wr}% ({wins}W/{losses}L)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>🎖️ Ramos 360 Ai</i>"
    )
    if notifier:
        try:
            await notifier.send(msg)
        except Exception as e:
            logger.warning(f"[Weekly] notify failed: {e}")


# ══════════════════════════════════════════════════════════════════
# ✅ job_self_learn — DEFINED DIRECTLY
# ══════════════════════════════════════════════════════════════════
async def job_self_learn(notifier=None):
    logger.info("[SelfLearn] Not yet implemented in live engine — skipping")
    if notifier:
        try:
            await notifier.send("🧠 <b>Self Learn</b>\nقيد التطوير — سيُفعَّل قريباً.")
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════
# ✅ job_layer1 — DEFINED DIRECTLY
# ══════════════════════════════════════════════════════════════════
async def job_layer1(notifier=None):
    macro = await get_macro_context()
    logger.info(f"[Layer1] USDT.D={macro.get('usdt_d')}% BTC.D={macro.get('btc_d')}%")
    if notifier:
        try:
            await notifier.send(
                f"🧠 <b>Layer 1 — Market Context</b>\n"
                f"USDT.D: {macro.get('usdt_d','?')}%\n"
                f"BTC.D: {macro.get('btc_d','?')}%\n"
                f"Bias: {macro.get('crypto_bias','?')}"
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════
# Legacy aliases (in case OTHER files reference old names)
# ══════════════════════════════════════════════════════════════════
job_monitor_positions = job_monitor
job_run_scalp          = job_scalp
job_run_swing          = job_swing
job_run_super_swing    = job_swing
job_daily_market       = job_daily
job_run_layer1         = job_layer1

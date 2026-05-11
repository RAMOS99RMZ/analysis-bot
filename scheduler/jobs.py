# scheduler/jobs.py
# All timed jobs: scalp (15m), swing (2h), super-swing (4h),
# monitor (5m), daily (00:00 UTC), weekly, heartbeat

from __future__ import annotations
import asyncio
from loguru import logger

from config import CONFIG
from engine.data_fetcher import DataFetcher
from engine.signal_generator import run_all_assets
from engine.risk_manager import calc_sltp, get_trade_type
from engine.indicator_engine import IndicatorEngine
from engine.analysis_engine import run_full_analysis   # ← جديد
from ai.confirmation import confirm_signal
from database import SupabaseLogger
from notifier import TelegramNotifier
from utils.helpers import (is_circuit_open, make_run_id,
                            utc_hour, record_sl_hit, get_session)

IE = IndicatorEngine()

# ── Shared singletons ─────────────────────────────────────────────
_db:       SupabaseLogger   = None
_notifier: TelegramNotifier = None
_fetcher:  DataFetcher      = None

def init(db: SupabaseLogger, notifier: TelegramNotifier, fetcher: DataFetcher):
    global _db, _notifier, _fetcher
    _db = db; _notifier = notifier; _fetcher = fetcher


# ══════════════════════════════════════════════════════════════════
# JOB 1 — MONITOR POSITIONS (every 5 min)
# ══════════════════════════════════════════════════════════════════
async def job_monitor_positions():
    if is_circuit_open(): return
    logger.info("[Monitor] 🔍 Checking positions …")
    try:
        open_trades = _db.get_open_trades()
        if not open_trades: return

        for trade in open_trades:
            symbol    = trade.get("symbol")
            direction = trade.get("direction")
            entry     = trade.get("entry_price", 0)
            sl        = trade.get("sl_price", 0)
            tp1       = trade.get("tp1_price", 0)
            tp2       = trade.get("tp2_price", 0)
            tp3       = trade.get("tp3_price", 0)
            if not symbol or not entry: continue

            price = await _fetcher.get_live_price(symbol)
            if not price: continue

            pnl_pct = ((price - entry) / entry * 100) if direction == "LONG" \
                       else ((entry - price) / entry * 100)

            event = None
            if direction == "LONG":
                if price <= sl:            event = "SL";  record_sl_hit(symbol, direction)
                elif tp3 and price >= tp3: event = "TP3"
                elif tp2 and price >= tp2: event = "TP2"
                elif tp1 and price >= tp1: event = "TP1"
            else:
                if price >= sl:            event = "SL";  record_sl_hit(symbol, direction)
                elif tp3 and price <= tp3: event = "TP3"
                elif tp2 and price <= tp2: event = "TP2"
                elif tp1 and price <= tp1: event = "TP1"

            if event:
                await _notifier.send_monitor_alert(symbol, direction, event, price, pnl_pct)
                status = "CLOSED" if event in ("SL", "TP3") else event
                await _db.log_trade({
                    "symbol": symbol, "direction": direction,
                    "trade_type": trade.get("trade_type", "?"),
                    "status": status, "entry_price": entry,
                    "exit_price": price, "pnl_pct": round(pnl_pct, 4),
                    "size_usdt": trade.get("size_usdt", 0),
                    "run_id": trade.get("run_id", ""),
                })

            daily_pnl = _db.get_daily_pnl()
            if daily_pnl < -CONFIG.MAX_DAILY_LOSS_PCT * 100:
                await _notifier.send_circuit_breaker(f"Daily PnL {daily_pnl:.2f}%")
                logger.warning(f"[Monitor] 🚨 Daily loss limit: {daily_pnl:.2f}%")
                return
    except Exception as e:
        logger.error(f"[Monitor] Error: {e}")


# ══════════════════════════════════════════════════════════════════
# CORE SIGNAL PIPELINE
# ══════════════════════════════════════════════════════════════════
async def _run_signal_pipeline(run_tag: str):
    if is_circuit_open():
        await _notifier.send_circuit_breaker("API circuit breaker open")
        return

    run_id  = make_run_id(run_tag)
    session = get_session()
    logger.info(f"[{run_tag}] 🚀 Starting — session={session['name']} run_id={run_id}")

    assets_data = await _fetcher.fetch_all_assets()
    balance     = await _fetcher.get_balance()
    fear_greed  = await _fetcher.get_fear_greed()

    for sym, d in assets_data.items():
        if d:
            d["fear_greed"]      = fear_greed
            d["funding"]         = await _fetcher.get_funding_rate(sym)
            d["usdt_dominance"]  = {}

    if balance <= 0:
        logger.warning(f"[{run_tag}] Balance=0, skipping")
        return

    open_trades = _db.get_open_trades()
    signals     = run_all_assets(assets_data, balance, open_trades, run_id)
    logger.info(f"[{run_tag}] Generated {len(signals)} signals before AI filter")

    if not signals:
        logger.info(f"[{run_tag}] No signals this cycle")
        return

    sent = 0
    for sig in signals:
        ai_ans = await confirm_signal(sig)
        sig["ai_confirmation"] = ai_ans
        if ai_ans == "NO":
            logger.info(f"[AI] Rejected: {sig['symbol']} {sig['direction']}")
            continue
        await _db.log_signal(sig)
        await _db.log_trade({
            "symbol": sig["symbol"], "direction": sig["direction"],
            "trade_type": sig["trade_type"], "status": "OPEN",
            "entry_price": sig["entry"], "exit_price": None,
            "pnl_pct": 0, "size_usdt": sig["size_usdt"], "run_id": run_id,
        })
        await _notifier.send_signal(sig)
        sent += 1
        await asyncio.sleep(0.3)

    logger.info(f"[{run_tag}] ✅ Done — {sent} signals sent")


# ══════════════════════════════════════════════════════════════════
# JOB 2 — SCALP (every 15 min)
# ══════════════════════════════════════════════════════════════════
async def job_run_scalp():
    await _run_signal_pipeline("SCALP")


# ══════════════════════════════════════════════════════════════════
# JOB 3 — SWING (every 2 hours)
# ══════════════════════════════════════════════════════════════════
async def job_run_swing():
    await _run_signal_pipeline("SWING")


# ══════════════════════════════════════════════════════════════════
# JOB 4 — SUPER SWING (every 4 hours)
# ══════════════════════════════════════════════════════════════════
async def job_run_super_swing():
    await _run_signal_pipeline("SUPER_SWING")


# ══════════════════════════════════════════════════════════════════
# JOB 5 — DAILY MARKET ANALYSIS (00:00 UTC)
# ══════════════════════════════════════════════════════════════════
async def job_daily_market():
    """
    التحليل اليومي الكامل:
      - يُشغّل E10 (BTC/ETH شارت) + E11 (USDT.D شارت)
      - يرسل رسالة Telegram شاملة عبر analysis_engine
      - يُسجّل الأداء اليومي في Supabase
    """
    logger.info("[Daily] 📊 بدء التحليل اليومي الكامل …")
    try:
        # ── 1. تحليل E10 + E11 وإرسال رسالة Telegram ────────────────
        await run_full_analysis(_db, _notifier, _fetcher)

        # ── 2. تسجيل الأداء اليومي في Supabase ──────────────────────
        daily_pnl  = _db.get_daily_pnl()
        open_t     = _db.get_open_trades()
        stats = {
            "wins":      0,
            "losses":    0,
            "total":     len(open_t),
            "win_rate":  0,
            "total_pnl": daily_pnl,
            "max_dd":    0,
        }
        await _db.log_performance(stats)
        await _db.heartbeat(CONFIG.VERSION, CONFIG.ASSETS)
        logger.success("[Daily] ✅ التحليل اليومي اكتمل")

    except Exception as e:
        logger.error(f"[Daily] Error: {e}")
        # إرسال تحذير للـ Telegram إذا فشل التحليل
        try:
            await _notifier.send(f"⚠️ <b>خطأ في التحليل اليومي</b>\n<code>{e}</code>")
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════
# JOB 6 — WEEKLY REPORT (Monday 08:00 UTC)
# ══════════════════════════════════════════════════════════════════
async def job_weekly_report():
    logger.info("[Weekly] 📋 Sending weekly report …")
    try:
        msg = (
            f"📋 <b>Weekly Report — {CONFIG.NAME}</b>\n"
            f"Bot is running normally ✅\n"
            f"Assets: {len(CONFIG.ASSETS)}\n"
            f"<i>Detailed stats available in Supabase dashboard.</i>"
        )
        await _notifier.send(msg)
    except Exception as e:
        logger.error(f"[Weekly] Error: {e}")


# ══════════════════════════════════════════════════════════════════
# JOB 7 — HEARTBEAT (every 6 hours)
# ══════════════════════════════════════════════════════════════════
async def job_heartbeat():
    await _db.heartbeat(CONFIG.VERSION, CONFIG.ASSETS)

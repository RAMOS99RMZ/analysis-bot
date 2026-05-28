# scheduler/jobs.py — Clean Final Version
from __future__ import annotations
import asyncio
from loguru import logger

from config import CONFIG
from engine.data_fetcher import DataFetcher
from engine.signal_generator import run_all_assets
from engine.indicator_engine import IndicatorEngine
from engine.analysis_engine import run_full_analysis
from ai.confirmation import confirm_signal
from database import SupabaseLogger
from notifier import TelegramNotifier
from utils.helpers import (is_circuit_open, make_run_id,
                            record_sl_hit, get_session)

IE = IndicatorEngine()

_db:       SupabaseLogger   = None
_notifier: TelegramNotifier = None
_fetcher:  DataFetcher      = None

def init(db: SupabaseLogger, notifier: TelegramNotifier, fetcher: DataFetcher):
    global _db, _notifier, _fetcher
    _db = db; _notifier = notifier; _fetcher = fetcher


# ══════════════════════════════════════════════════════════════════
# JOB 1 — MONITOR POSITIONS
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
                # إرسال Telegram
                try:
                    await _notifier.send_monitor_alert(
                        symbol, direction, event, price, pnl_pct
                    )
                except Exception as e:
                    logger.error(f"[Monitor] Telegram: {e}")

                status = "CLOSED" if event in ("SL", "TP3") else event

                # ✅ Adaptive Learning — يسجّل النتيجة بأمان
                if status == "CLOSED":
                    try:
                        from engine.signal_generator import record_trade_result
                        record_trade_result(symbol, 0.0, pnl_pct)
                    except Exception:
                        pass   # إذا لم يكن موجوداً في الملف القديم → تجاهل بصمت

                # حفظ في DB
                try:
                    await _db.log_trade({
                        "symbol":      symbol,
                        "direction":   direction,
                        "trade_type":  trade.get("trade_type", "?"),
                        "status":      status,
                        "entry_price": entry,
                        "exit_price":  price,
                        "pnl_pct":     round(pnl_pct, 4),
                        "size_usdt":   trade.get("size_usdt", 0),
                        "run_id":      trade.get("run_id", ""),
                    })
                except Exception as e:
                    logger.warning(f"[Monitor] DB: {e}")

            # Daily loss check
            try:
                daily_pnl = _db.get_daily_pnl()
                if daily_pnl < -CONFIG.MAX_DAILY_LOSS * 100:
                    await _notifier.send_circuit_breaker(
                        f"Daily PnL {daily_pnl:.2f}%"
                    )
                    return
            except Exception:
                pass

    except Exception as e:
        logger.error(f"[Monitor] Error: {e}")


# ══════════════════════════════════════════════════════════════════
# CORE SIGNAL PIPELINE
# ══════════════════════════════════════════════════════════════════
async def _run_signal_pipeline(run_tag: str):
    if is_circuit_open():
        try:
            await _notifier.send_circuit_breaker("API circuit breaker open")
        except Exception:
            pass
        return

    run_id  = make_run_id(run_tag)
    session = get_session()
    logger.info(f"[{run_tag}] 🚀 Starting — session={session['name']} run_id={run_id}")

    assets_data = await _fetcher.fetch_all_assets()
    balance     = await _fetcher.get_balance()

    try:
        fear_greed = await _fetcher.get_fear_greed()
    except Exception:
        fear_greed = {}

    for sym, d in assets_data.items():
        if d:
            d["fear_greed"]     = fear_greed
            d["usdt_dominance"] = {}
            try:
                d["funding"] = await _fetcher.get_funding_rate(sym)
            except Exception:
                d["funding"] = {}

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

        # ── AI Confirmation ───────────────────────────────────────────
        try:
            ai_ans = await confirm_signal(sig)
        except Exception:
            ai_ans = "SKIP"
        sig["ai_confirmation"] = ai_ans
        if ai_ans == "NO":
            logger.info(f"[AI] Rejected: {sig['symbol']} {sig['direction']}")
            continue

        # ── ✅ Gemini Vision (مرة كل 4 ساعات لكل عملة) ───────────────
        try:
            from ai.gemini_vision import analyze_pattern, parse_vision_decision
            c4h_data = assets_data.get(sig["symbol"], {}).get("c4h", [])
            if c4h_data:
                vision_text = await analyze_pattern(sig["symbol"], c4h_data, sig)
                vision_dec  = parse_vision_decision(vision_text)
                sig["gemini_vision"] = vision_dec
                logger.info(f"[Vision] {sig['symbol']} → {vision_dec}")
                if vision_dec == "REJECT":
                    logger.info(f"[Vision] {sig['symbol']} رُفض بواسطة Gemini")
                    continue
            else:
                sig["gemini_vision"] = "NEUTRAL"
        except Exception as e:
            logger.debug(f"[Vision] {sig.get('symbol','?')}: {e}")
            sig["gemini_vision"] = "NEUTRAL"

        # ── ✅ Telegram أولاً دائماً ──────────────────────────────────
        try:
            await _notifier.send_signal(sig)
            sent += 1
            logger.info(
                f"[{run_tag}] ✅ Sent: {sig['symbol']} {sig['direction']} "
                f"score={sig.get('score',0):.3f}"
            )
        except Exception as e:
            logger.error(f"[{run_tag}] Telegram FAILED: {e}")

        # ── حفظ في DB ─────────────────────────────────────────────────
        try:
            await _db.log_signal(sig)
        except Exception as e:
            logger.warning(f"[Jobs] log_signal: {e}")

        try:
            await _db.log_trade({
                "symbol":      sig["symbol"],
                "direction":   sig["direction"],
                "trade_type":  sig["trade_type"],
                "status":      "OPEN",
                "entry_price": sig["entry"],
                "exit_price":  None,
                "pnl_pct":     0,
                "size_usdt":   sig["size_usdt"],
                "run_id":      run_id,
            })
        except Exception as e:
            logger.warning(f"[Jobs] log_trade: {e}")

        await asyncio.sleep(0.3)

    logger.info(f"[{run_tag}] ✅ Done — {sent}/{len(signals)} signals sent")


# ══════════════════════════════════════════════════════════════════
async def job_run_scalp():       await _run_signal_pipeline("SCALP")
async def job_run_swing():       await _run_signal_pipeline("SWING")
async def job_run_super_swing(): await _run_signal_pipeline("SUPER_SWING")


# ══════════════════════════════════════════════════════════════════
# JOB 5 — DAILY
# ══════════════════════════════════════════════════════════════════
async def job_daily_market():
    logger.info("[Daily] 📊 بدء التحليل اليومي …")
    try:
        try:
            from ai.daily_consensus import run_daily_consensus
            await run_daily_consensus(_notifier)
        except Exception as e:
            logger.warning(f"[Daily] Gemini consensus: {e}")

        try:
            await run_full_analysis(_db, _notifier, _fetcher)
        except Exception as e:
            logger.warning(f"[Daily] analysis_engine: {e}")

        try:
            daily_pnl = _db.get_daily_pnl()
            open_t    = _db.get_open_trades()
            await _db.log_performance({
                "wins": 0, "losses": 0,
                "total": len(open_t),
                "win_rate": 0,
                "total_pnl": daily_pnl,
                "max_dd": 0,
            })
            await _db.heartbeat(CONFIG.VERSION, CONFIG.ASSETS)
        except Exception as e:
            logger.warning(f"[Daily] DB: {e}")

        logger.success("[Daily] ✅ اكتمل")
    except Exception as e:
        logger.error(f"[Daily] Error: {e}")
        try:
            await _notifier.send(
                f"⚠️ <b>خطأ في التحليل اليومي</b>\n<code>{e}</code>"
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════
# JOB 6 — WEEKLY
# ══════════════════════════════════════════════════════════════════
async def job_weekly_report():
    try:
        await _notifier.send(
            f"📋 <b>Weekly Report — {CONFIG.NAME}</b>\n"
            f"✅ Bot running | Assets: {len(CONFIG.ASSETS)}"
        )
    except Exception as e:
        logger.error(f"[Weekly] {e}")


# ══════════════════════════════════════════════════════════════════
# JOB 7 — HEARTBEAT
# ══════════════════════════════════════════════════════════════════
async def job_heartbeat():
    try:
        await _db.heartbeat(CONFIG.VERSION, CONFIG.ASSETS)
    except Exception:
        pass

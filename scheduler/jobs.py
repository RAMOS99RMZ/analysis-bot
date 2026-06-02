# scheduler/jobs.py — Final version with Telegram Reply support
# ✅ يحفظ message_id عند إرسال الإشارة
# ✅ يرد على رسالة الإشارة الأصلية عند TP/SL
# ✅ Telegram يُرسل دائماً حتى لو Supabase فشل
from __future__ import annotations
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional
from loguru import logger

from config import CONFIG
from engine.data_fetcher import DataFetcher
from engine.signal_generator import run_all_assets
from engine.indicator_engine import IndicatorEngine
from engine.memory_system import MEMORY
from engine.correlation_matrix import update_closes
from engine.trade_manager import TRADE_MGR
from engine.smart_money import SMART_MONEY
from engine.weekly_backtest import BACKTEST
from ai.confirmation import confirm_signal
from database import SupabaseLogger
from notifier import TelegramNotifier
from utils.helpers import (is_circuit_open, make_run_id,
                            record_sl_hit, get_session)

try:
    from engine.analysis_engine import run_full_analysis
    _HAS_ANALYSIS = True
except ImportError:
    _HAS_ANALYSIS = False

IE = IndicatorEngine()

_db:       SupabaseLogger   = None
_notifier: TelegramNotifier = None
_fetcher:  DataFetcher      = None
_memory_loaded: bool        = False

# ✅ ذاكرة محلية للصفقات — تحفظ message_id للرد لاحقاً
_LOCAL_OPEN_TRADES: List[Dict] = []


def init(db: SupabaseLogger, notifier: TelegramNotifier, fetcher: DataFetcher):
    global _db, _notifier, _fetcher
    _db = db; _notifier = notifier; _fetcher = fetcher
    SMART_MONEY._fetcher = fetcher
    BACKTEST._db = db


async def _ensure_memory():
    global _memory_loaded
    if not _memory_loaded:
        MEMORY._db = _db
        await MEMORY.load()
        _memory_loaded = True


def _get_open_trades() -> List[Dict]:
    """DB أولاً، ثم الذاكرة المحلية."""
    try:
        db_trades = _db.get_open_trades()
        if db_trades:
            # نُضيف message_id من الذاكرة المحلية إذا كان موجوداً
            local_map = {
                (t.get("symbol"), t.get("direction")): t.get("tg_message_id")
                for t in _LOCAL_OPEN_TRADES
            }
            for t in db_trades:
                key = (t.get("symbol"), t.get("direction"))
                if key in local_map and local_map[key]:
                    t["tg_message_id"] = local_map[key]
            return db_trades
    except Exception:
        pass
    return _LOCAL_OPEN_TRADES


# ══════════════════════════════════════════════════════════════════
# JOB 1 — MONITOR POSITIONS (every 1 min)
# ══════════════════════════════════════════════════════════════════
async def job_monitor_positions():
    if is_circuit_open(): return
    try:
        open_trades = _get_open_trades()
        if not open_trades: return

        for trade in list(open_trades):
            symbol    = trade.get("symbol")
            direction = trade.get("direction")
            entry     = float(trade.get("entry_price", 0))
            msg_id    = trade.get("tg_message_id")   # ✅ للرد على الإشارة الأصلية
            if not symbol or not entry: continue

            price = await _fetcher.get_live_price(symbol)
            if not price or price <= 0: continue

            eval_result = TRADE_MGR.evaluate(trade, price)
            action      = eval_result.get("action", "HOLD")
            pnl_pct     = eval_result["pnl_pct"]

            if action == "HOLD": continue

            # ✅ إرسال رد على رسالة الإشارة الأصلية
            try:
                await _notifier.send_trade_update(
                    symbol, direction, action,
                    price, pnl_pct,
                    reply_to_msg_id=msg_id   # ← الرد على الإشارة الأصلية
                )
            except Exception as e:
                logger.error(f"[Monitor] Telegram reply failed: {e}")

            # Trailing SL — تحديث فقط
            if action == "TRAIL_SL":
                trade["sl_price"] = eval_result["new_sl"]
                trade["trail_sl"] = eval_result["new_sl"]
                continue

            # Partial TP1
            if action == "PARTIAL_TP1":
                trade["sl_price"]     = eval_result["new_sl"]
                trade["partial_done"] = True
                continue

            # إغلاق كامل
            if action in ("CLOSE_SL", "CLOSE_TP2", "CLOSE_TP3"):
                if "SL" in action:
                    record_sl_hit(symbol, direction)
                MEMORY.record(symbol, direction, pnl_pct,
                              datetime.now(timezone.utc).hour)
                # إزالة من الذاكرة المحلية
                _LOCAL_OPEN_TRADES[:] = [
                    t for t in _LOCAL_OPEN_TRADES
                    if not (t.get("symbol") == symbol
                            and t.get("direction") == direction)
                ]
                # تحديث DB
                try:
                    await _db.log_trade({
                        "symbol": symbol, "direction": direction,
                        "trade_type": trade.get("trade_type", "?"),
                        "status": "CLOSED",
                        "entry_price": entry,
                        "exit_price": price,
                        "pnl_pct": round(pnl_pct, 4),
                        "size_usdt": trade.get("size_usdt", 0),
                        "run_id": trade.get("run_id", ""),
                    })
                except Exception:
                    pass

        # Daily loss check
        try:
            daily_pnl = _db.get_daily_pnl()
            if daily_pnl < -CONFIG.MAX_DAILY_LOSS * 100:
                await _notifier.send_circuit_breaker(
                    f"Daily PnL {daily_pnl:.2f}%"
                )
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

    await _ensure_memory()
    run_id  = make_run_id(run_tag)
    session = get_session()
    logger.info(f"[{run_tag}] 🚀 Starting — session={session['name']} run_id={run_id}")

    assets_data = await _fetcher.fetch_all_assets()
    balance     = await _fetcher.get_balance()

    # بيانات إضافية
    fear_greed, usdt_dominance = {}, {}
    try: fear_greed     = await _fetcher.get_fear_greed()
    except Exception: pass
    try: usdt_dominance = await SMART_MONEY._get_global() or {}
    except Exception: pass

    for sym, d in assets_data.items():
        if not d: continue
        d["fear_greed"]     = fear_greed
        d["usdt_dominance"] = usdt_dominance
        try: d["funding"]   = await _fetcher.get_funding_rate(sym)
        except Exception: d["funding"] = {}
        if d.get("c4h"):
            update_closes(sym, d["c4h"])
        d["memory_mult"] = MEMORY.get_confidence_multiplier(
            sym, datetime.now(timezone.utc).hour
        )

    if balance <= 0:
        logger.warning(f"[{run_tag}] Balance=0, skipping")
        return

    open_trades = _get_open_trades()
    signals     = run_all_assets(assets_data, balance, open_trades, run_id)
    logger.info(f"[{run_tag}] Generated {len(signals)} signals before AI filter")

    if not signals:
        logger.info(f"[{run_tag}] No signals this cycle")
        return

    sent = 0
    for sig in signals:
        # AI confirmation
        try: ai_ans = await confirm_signal(sig)
        except Exception: ai_ans = "SKIP"
        sig["ai_confirmation"] = ai_ans
        if ai_ans == "NO":
            logger.info(f"[AI] Rejected: {sig['symbol']} {sig['direction']}")
            continue

        # ✅ 1. إرسال Telegram أولاً — يحفظ message_id
        tg_msg_id: Optional[int] = None
        try:
            tg_msg_id = await _notifier.send_signal(sig)
            sent += 1
            logger.info(
                f"[{run_tag}] ✅ Sent: {sig['symbol']} {sig['direction']} "
                f"msg_id={tg_msg_id}"
            )
        except Exception as e:
            logger.error(f"[{run_tag}] Telegram FAILED: {e}")

        # ✅ 2. حفظ في الذاكرة المحلية مع message_id
        _LOCAL_OPEN_TRADES.append({
            "symbol":         sig["symbol"],
            "direction":      sig["direction"],
            "trade_type":     sig["trade_type"],
            "entry_price":    sig["entry"],
            "sl_price":       sig["sl"],
            "tp1_price":      sig["tp1"],
            "tp2_price":      sig["tp2"],
            "tp3_price":      sig["tp3"],
            "trail_sl":       sig["sl"],
            "partial_done":   False,
            "size_usdt":      sig["size_usdt"],
            "run_id":         run_id,
            "tg_message_id":  tg_msg_id,   # ✅ للرد لاحقاً
        })

        # ✅ 3. حفظ في DB (لا يوقف أي شيء إذا فشل)
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
# JOB 5 — DAILY ANALYSIS
# ══════════════════════════════════════════════════════════════════
async def job_daily_market():
    from ai.daily_consensus import run_daily_consensus
    logger.info("[Daily] 📊 بدء التحليل اليومي …")
    
    # ✅ Gemini Daily Consensus
    try:
        await run_daily_consensus(_notifier)
    except Exception as e:
        logger.warning(f"[Daily] Gemini: {e}")
        
    try:
        if _HAS_ANALYSIS:
            await run_full_analysis(_db, _notifier, _fetcher)
    except Exception as e:
        logger.error(f"[Daily] Analysis Engine Error: {e}")
        
    try:
        daily_pnl = _db.get_daily_pnl()
        await _db.log_performance({
            "wins": 0, 
            "losses": 0,
            "total": len(_LOCAL_OPEN_TRADES),  # ✅ تم الإصلاح الجذري هنا
            "win_rate": 0, 
            "total_pnl": daily_pnl, 
            "max_dd": 0,
        })
        await _db.heartbeat(CONFIG.VERSION, CONFIG.ASSETS)
    except Exception as e:
        logger.warning(f"[Daily] DB Logging Error: {e}")
        
    logger.success("[Daily] ✅ اكتمل")


# ══════════════════════════════════════════════════════════════════
# JOB 6 — SELF LEARN + BACKTEST
# ══════════════════════════════════════════════════════════════════
async def job_self_learn():
    logger.info("[SelfLearn] 🧠 بدء …")
    try:
        bt_result = await BACKTEST.run()
        msg = BACKTEST.format_report(bt_result, CONFIG.NAME)
        await _notifier.send(msg)
    except Exception as e:
        logger.error(f"[SelfLearn] {e}")


# ══════════════════════════════════════════════════════════════════
# JOB 7 — WEEKLY REPORT
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
# JOB 8 — HEARTBEAT
# ══════════════════════════════════════════════════════════════════
async def job_heartbeat():
    try:
        await _db.heartbeat(CONFIG.VERSION, CONFIG.ASSETS)
    except Exception:
        pass

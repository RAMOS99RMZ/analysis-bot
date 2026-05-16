# scheduler/jobs.py
# ══════════════════════════════════════════════════════════════════════════════
# BUG FIX v1.1:
#   ERROR: cannot import name 'job_self_learn' from 'scheduler.jobs'
#   CAUSE: الدالة مُستدعاة من main.py لكنها غير موجودة في هذا الملف
#   FIX:   تمت إضافة job_self_learn() كاملة أسفل هذا الملف
# ══════════════════════════════════════════════════════════════════════════════

from __future__ import annotations
import asyncio
from datetime import datetime, timezone, timedelta
from loguru import logger

from config import CONFIG
from engine.data_fetcher import DataFetcher
from engine.signal_generator import run_all_assets
from engine.risk_manager import calc_sltp, get_trade_type
from engine.indicator_engine import IndicatorEngine
from engine.analysis_engine import run_full_analysis
from ai.confirmation import confirm_signal
from database import SupabaseLogger
from notifier import TelegramNotifier
from utils.helpers import (
    is_circuit_open, make_run_id, utc_hour, record_sl_hit, get_session
)
from strategies import EXPERT_NAMES, EXPERT_WEIGHTS

IE = IndicatorEngine()

# ── Shared singletons ──────────────────────────────────────────────────────────
_db:       SupabaseLogger   = None
_notifier: TelegramNotifier = None
_fetcher:  DataFetcher      = None


def init(db: SupabaseLogger, notifier: TelegramNotifier, fetcher: DataFetcher):
    global _db, _notifier, _fetcher
    _db = db
    _notifier = notifier
    _fetcher  = fetcher


# ══════════════════════════════════════════════════════════════════════════════
# JOB 1 — MONITOR POSITIONS (every 1 min)
# ══════════════════════════════════════════════════════════════════════════════

async def job_monitor_positions():
    if is_circuit_open():
        return
    logger.info("[Monitor] 🔍 Checking positions …")
    try:
        open_trades = _db.get_open_trades()
        if not open_trades:
            return

        for trade in open_trades:
            symbol    = trade.get("symbol")
            direction = trade.get("direction")
            entry     = trade.get("entry_price", 0)
            sl        = trade.get("sl_price",    0)
            tp1       = trade.get("tp1_price",   0)
            tp2       = trade.get("tp2_price",   0)
            tp3       = trade.get("tp3_price",   0)
            if not symbol or not entry:
                continue

            price = await _fetcher.get_live_price(symbol)
            if not price:
                continue

            pnl_pct = (
                (price - entry) / entry * 100 if direction == "LONG"
                else (entry - price) / entry * 100
            )

            event = None
            if direction == "LONG":
                if price <= sl:              event = "SL";  record_sl_hit(symbol, direction)
                elif tp3 and price >= tp3:   event = "TP3"
                elif tp2 and price >= tp2:   event = "TP2"
                elif tp1 and price >= tp1:   event = "TP1"
            else:
                if price >= sl:              event = "SL";  record_sl_hit(symbol, direction)
                elif tp3 and price <= tp3:   event = "TP3"
                elif tp2 and price <= tp2:   event = "TP2"
                elif tp1 and price <= tp1:   event = "TP1"

            if event:
                await _notifier.send_monitor_alert(
                    symbol, direction, event, price, pnl_pct
                )
                status = "CLOSED" if event in ("SL", "TP3") else event
                await _db.log_trade({
                    "symbol":     symbol,
                    "direction":  direction,
                    "trade_type": trade.get("trade_type", "?"),
                    "status":     status,
                    "entry_price": entry,
                    "exit_price":  price,
                    "pnl_pct":    round(pnl_pct, 4),
                    "size_usdt":  trade.get("size_usdt", 0),
                    "run_id":     trade.get("run_id", ""),
                })

            daily_pnl = _db.get_daily_pnl()
            if daily_pnl < -CONFIG.MAX_DAILY_LOSS_PCT * 100:
                await _notifier.send_circuit_breaker(
                    f"Daily PnL {daily_pnl:.2f}%"
                )
                logger.warning(f"[Monitor] 🚨 Daily loss limit: {daily_pnl:.2f}%")
                return

    except Exception as e:
        logger.error(f"[Monitor] Error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# CORE SIGNAL PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

async def _run_signal_pipeline(run_tag: str):
    if is_circuit_open():
        await _notifier.send_circuit_breaker("API circuit breaker open")
        return

    run_id  = make_run_id(run_tag)
    session = get_session()
    logger.info(
        f"[{run_tag}] 🚀 Starting — session={session['name']} run_id={run_id}"
    )

    assets_data = await _fetcher.fetch_all_assets()
    balance     = await _fetcher.get_balance()
    fear_greed  = await _fetcher.get_fear_greed()

    for sym, d in assets_data.items():
        if not d:
            continue
        d["fear_greed"]     = fear_greed
        d["funding"]        = await _fetcher.get_funding_rate(sym)
        d["usdt_dominance"] = {}
        # ── OI + L/S data for Expert 13 & 14 ────────────────────────────────
        try:
            d["oi_data"]  = await _fetcher.get_oi_data(sym)
        except Exception:
            d["oi_data"]  = {"oi_value": 0, "slope": 0.0, "ok": False}
        try:
            d["ls_ratio"] = await _fetcher.get_ls_ratio(sym)
        except Exception:
            d["ls_ratio"] = None

    if balance <= 0:
        logger.warning(f"[{run_tag}] Balance=0, skipping")
        return

    open_trades = _db.get_open_trades()
    signals     = run_all_assets(assets_data, balance, open_trades, run_id)
    logger.info(
        f"[{run_tag}] Generated {len(signals)} signals before AI filter"
    )

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
        await _notifier.send_signal(sig)
        sent += 1
        await asyncio.sleep(0.3)

    logger.info(f"[{run_tag}] ✅ Done — {sent} signals sent")


# ══════════════════════════════════════════════════════════════════════════════
# JOB 2 — SCALP (every 5 min)
# ══════════════════════════════════════════════════════════════════════════════

async def job_run_scalp():
    await _run_signal_pipeline("SCALP")


# ══════════════════════════════════════════════════════════════════════════════
# JOB 3 — SWING (every 2 hours)
# ══════════════════════════════════════════════════════════════════════════════

async def job_run_swing():
    await _run_signal_pipeline("SWING")


# ══════════════════════════════════════════════════════════════════════════════
# JOB 4 — SUPER SWING (every 4 hours)
# ══════════════════════════════════════════════════════════════════════════════

async def job_run_super_swing():
    await _run_signal_pipeline("SUPER_SWING")


# ══════════════════════════════════════════════════════════════════════════════
# JOB 5 — DAILY MARKET ANALYSIS (00:00 UTC)
# ══════════════════════════════════════════════════════════════════════════════

async def job_daily_market():
    """E10 + E11 complete daily analysis."""
    logger.info("[Daily] 📊 بدء التحليل اليومي الكامل …")
    try:
        await run_full_analysis(_db, _notifier, _fetcher)

        daily_pnl = _db.get_daily_pnl()
        open_t    = _db.get_open_trades()
        await _db.log_performance({
            "wins":      0,
            "losses":    0,
            "total":     len(open_t),
            "win_rate":  0,
            "total_pnl": daily_pnl,
            "max_dd":    0,
        })
        await _db.heartbeat(CONFIG.VERSION, CONFIG.ASSETS)
        logger.success("[Daily] ✅ التحليل اليومي اكتمل")

    except Exception as e:
        logger.error(f"[Daily] Error: {e}")
        try:
            await _notifier.send(
                f"⚠️ <b>خطأ في التحليل اليومي</b>\n<code>{e}</code>"
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# JOB 6 — WEEKLY REPORT (Monday 08:00 UTC)
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# JOB 7 — HEARTBEAT (every 6 hours)
# ══════════════════════════════════════════════════════════════════════════════

async def job_heartbeat():
    await _db.heartbeat(CONFIG.VERSION, CONFIG.ASSETS)


# ══════════════════════════════════════════════════════════════════════════════
# JOB 8 — SELF LEARN (Sunday 10:00 UTC)
# ══════════════════════════════════════════════════════════════════════════════
# ✅ FIX: هذه الدالة كانت مفقودة تماماً — تسببت في:
#    ImportError: cannot import name 'job_self_learn' from 'scheduler.jobs'
# ══════════════════════════════════════════════════════════════════════════════

async def job_self_learn():
    """
    🧠 Self-Learning Engine — يحلل أداء كل خبير ويرسل تقرير Telegram.

    يعمل كل أحد 10:00 UTC تلقائياً (self_learn.yml).
    الخوارزمية:
      1. جلب آخر 30 يوماً من صفقات Supabase
      2. ربط كل صفقة بالإشارة التي أنتجتها عبر run_id
      3. حساب win-rate و avg PnL لكل خبير
      4. حساب أوزان مُحسَّنة مقترحة
      5. إرسال تقرير كامل على Telegram
      6. تسجيل ملخص الأداء في Supabase
    """
    logger.info("[SelfLearn] 🧠 Starting expert weight optimization …")

    try:
        # ── 1. جلب البيانات من Supabase ──────────────────────────────────────
        trades  = _fetch_recent_trades(days=30)
        signals = _fetch_recent_signals(days=30)

        if not trades:
            msg = (
                f"🧠 <b>Self-Learn — {CONFIG.NAME}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ لا توجد صفقات مُسجَّلة خلال آخر 30 يوماً.\n"
                f"سيبدأ التعلم عندما تتراكم بيانات كافية."
            )
            await _notifier.send(msg)
            return

        # ── 2. بناء خريطة run_id → نتيجة الصفقة ──────────────────────────────
        outcome_map: dict = {}
        for t in trades:
            rid    = t.get("run_id", "")
            status = t.get("status", "")
            pnl    = float(t.get("pnl_pct", 0) or 0)
            if not rid or not status:
                continue
            # صفقة رابحة = أي TP أو CLOSED بـ pnl > 0
            win = status in ("TP1", "TP2", "TP3", "CLOSED") and pnl > 0
            outcome_map[rid] = {"win": win, "pnl": pnl, "status": status}

        # ── 3. تجميع إحصائيات كل خبير ─────────────────────────────────────────
        stats: dict = {
            name: {"wins": 0, "losses": 0, "pnl_sum": 0.0}
            for name in EXPERT_NAMES
        }

        matched = 0
        for sig in signals:
            rid      = sig.get("run_id", "")
            outcome  = outcome_map.get(rid)
            if not outcome:
                continue
            matched += 1
            fired = sig.get("experts_fired") or []
            if isinstance(fired, str):
                # Supabase قد يُرجع TEXT[] كنص — نحوّله
                import json
                try:
                    fired = json.loads(fired)
                except Exception:
                    fired = [x.strip().strip('"')
                             for x in fired.strip("{}").split(",")
                             if x.strip()]
            for name in fired:
                if name in stats:
                    stats[name]["pnl_sum"] += outcome["pnl"]
                    if outcome["win"]:
                        stats[name]["wins"]   += 1
                    else:
                        stats[name]["losses"] += 1

        # ── 4. بناء التقرير ────────────────────────────────────────────────────
        total_trades = len(trades)
        total_wins   = sum(1 for t in trades
                           if float(t.get("pnl_pct", 0) or 0) > 0)
        overall_wr   = total_wins / total_trades * 100 if total_trades else 0
        overall_pnl  = sum(float(t.get("pnl_pct", 0) or 0) for t in trades)

        lines = [
            f"🧠 <b>Self-Learn Report — {CONFIG.NAME}</b>",
            f"━━━━━━━━━━━━━━━━━━━━",
            f"📅 آخر 30 يوماً | صفقات: <b>{total_trades}</b> "
            f"| ✅ {total_wins} ❌ {total_trades - total_wins}",
            f"🎯 Win-Rate إجمالي: <b>{overall_wr:.1f}%</b> "
            f"| PnL: <b>{overall_pnl:+.2f}%</b>",
            f"🔗 إشارات مُرتبطة: {matched}/{len(signals)}",
            "",
            "<b>📊 أداء كل خبير:</b>",
        ]

        adjustments: dict = {}
        for i, name in enumerate(EXPERT_NAMES):
            st    = stats[name]
            total = st["wins"] + st["losses"]
            if total < 2:
                # بيانات غير كافية — احتفظ بالوزن الحالي
                adjustments[name] = EXPERT_WEIGHTS[i]
                continue

            wr      = st["wins"] / total
            avg_pnl = st["pnl_sum"] / total
            cur_w   = EXPERT_WEIGHTS[i]

            # حساب الوزن المُقترح
            if wr >= 0.65 and avg_pnl > 0.5:
                new_w = min(round(cur_w * 1.15, 2), 2.0)
                icon  = "📈"
            elif wr <= 0.35 or avg_pnl < -0.5:
                new_w = max(round(cur_w * 0.85, 2), 0.3)
                icon  = "📉"
            else:
                new_w = cur_w
                icon  = "➡️"

            adjustments[name] = new_w
            lines.append(
                f"  {icon} <b>{name}</b> "
                f"WR={wr*100:.0f}% ({st['wins']}/{total}) "
                f"avgPnL={avg_pnl:+.2f}% | W: {cur_w}→{new_w}"
            )

        # ── 5. أفضل وأسوأ خبراء ───────────────────────────────────────────────
        ranked = sorted(
            [(n, adjustments[n]) for n in EXPERT_NAMES
             if stats[n]["wins"] + stats[n]["losses"] >= 2],
            key=lambda x: x[1], reverse=True
        )
        if ranked:
            lines += [
                "",
                "🏆 <b>أفضل 3 خبراء:</b>",
            ]
            for name, w in ranked[:3]:
                lines.append(f"  🥇 {name}: w={w}")

            worst = ranked[-3:][::-1]
            lines += ["", "⚠️ <b>أضعف 3 خبراء (يحتاجون مراجعة):</b>"]
            for name, w in worst:
                lines.append(f"  🔻 {name}: w={w}")

        lines += [
            "",
            "💡 <i>الأوزان المقترحة للتطبيق اليدوي في config.py → EXPERT_WEIGHTS</i>",
            f"\n<i>🎖️ {CONFIG.NAME} — Self-Learn Engine v1.1</i>",
        ]

        await _notifier.send("\n".join(lines))

        # ── 6. تسجيل الأداء في Supabase ──────────────────────────────────────
        await _db.log_performance({
            "symbol":    "ALL",
            "wins":      total_wins,
            "losses":    total_trades - total_wins,
            "total":     total_trades,
            "win_rate":  round(overall_wr / 100, 4),
            "total_pnl": round(overall_pnl, 4),
            "max_dd":    0.0,
        })

        logger.success(
            f"[SelfLearn] ✅ Done — {total_trades} trades analyzed, "
            f"{len([v for v in adjustments.values() if v])} experts scored"
        )

    except Exception as e:
        logger.error(f"[SelfLearn] Error: {e}")
        try:
            await _notifier.send(
                f"⚠️ <b>ERROR — SELF_LEARN</b>\n<code>{e}</code>\n"
                f"<i>{CONFIG.NAME}</i>"
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# PRIVATE HELPERS — Supabase queries for job_self_learn
# (مُعرَّفة هنا مباشرة لتجنب تعديل supabase_client.py)
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_recent_trades(days: int = 30) -> list:
    """جلب الصفقات المُغلقة خلال آخر N يوماً من Supabase."""
    if not _db or not _db._enabled or not _db._client:
        return []
    try:
        since = (
            datetime.now(timezone.utc) - timedelta(days=days)
        ).isoformat()
        res = (
            _db._client
            .table("trades")
            .select("run_id, status, pnl_pct, symbol, direction, trade_type")
            .neq("status", "OPEN")
            .gte("created_at", since)
            .execute()
        )
        return res.data or []
    except Exception as e:
        logger.warning(f"[SelfLearn] _fetch_recent_trades: {e}")
        return []


def _fetch_recent_signals(days: int = 30) -> list:
    """جلب الإشارات المُسجَّلة خلال آخر N يوماً من Supabase."""
    if not _db or not _db._enabled or not _db._client:
        return []
    try:
        since = (
            datetime.now(timezone.utc) - timedelta(days=days)
        ).isoformat()
        res = (
            _db._client
            .table("signals")
            .select("run_id, experts_fired, direction, symbol")
            .gte("created_at", since)
            .execute()
        )
        return res.data or []
    except Exception as e:
        logger.warning(f"[SelfLearn] _fetch_recent_signals: {e}")
        return []

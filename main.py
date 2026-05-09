"""
╔══════════════════════════════════════════════════════════════════════╗
║        🎖️ Ramos Ai 360 ♾️🎖️ — main.py                         ║
║        Router المركزي — يستلم --mode ويوجّه للدالة الصحيحة       ║
║                                                                      ║
║  طريقة التشغيل:                                                     ║
║    python main.py --mode monitor                                     ║
║    python main.py --mode scalp                                       ║
║    python main.py --mode swing                                       ║
║    python main.py --mode super_swing                                 ║
║    python main.py --mode analysis                                    ║
║    python main.py --mode weekly_report                               ║
║    python main.py --mode self_learn                                  ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import argparse
import asyncio
import sys
import time
from loguru import logger

from config import CONFIG, Secrets, setup_logging
from database import SupabaseLogger
from notifier import TelegramNotifier
from engine.data_fetcher import DataFetcher


# ══════════════════════════════════════════════════════════════════════
# STARTUP — تهيئة مشتركة لكل الـ modes
# ══════════════════════════════════════════════════════════════════════

async def _startup() -> tuple[SupabaseLogger, TelegramNotifier, DataFetcher]:
    """يُشغَّل مرة واحدة في بداية كل mode."""
    Secrets.validate()
    db       = SupabaseLogger()
    notifier = TelegramNotifier()
    fetcher  = DataFetcher()
    return db, notifier, fetcher


# ══════════════════════════════════════════════════════════════════════
# MODE 1 — MONITOR  (كل دقيقة)
# ══════════════════════════════════════════════════════════════════════

async def run_monitor():
    """
    🛡️ الحارس الشخصي للصفقات.
    - يجلب الأسعار الحية
    - يتحقق من TP / SL لكل صفقة مفتوحة
    - يغلق فوراً ويرسل تنبيه Telegram
    - يطبّق Flash Crash Protection
    """
    db, notifier, fetcher = await _startup()
    logger.info("🛡️ [MONITOR] بدء فحص المراكز المفتوحة …")
    try:
        from scheduler.jobs import job_monitor_positions, init
        init(db, notifier, fetcher)
        await job_monitor_positions()
        logger.success("🛡️ [MONITOR] انتهى الفحص بنجاح.")
    except Exception as e:
        logger.error(f"🛡️ [MONITOR] خطأ: {e}")
        await notifier.send_error("MONITOR", str(e))
    finally:
        await fetcher.close()
        await notifier.close()


# ══════════════════════════════════════════════════════════════════════
# MODE 2 — SCALP  (كل 5 دقائق)
# ══════════════════════════════════════════════════════════════════════

async def run_scalp():
    """
    ⚡ صياد الفرص السريعة.
    - Quick Scalp: ارتدادات Fibonacci
    - Classic Scalp: استشارة الـ 11 خبير
    - تأكيد AI (Grok → Gemini)
    """
    db, notifier, fetcher = await _startup()
    logger.info("⚡ [SCALP] بدء تحليل Scalp …")
    try:
        from scheduler.jobs import job_run_scalp, init
        init(db, notifier, fetcher)
        await job_run_scalp()
        logger.success("⚡ [SCALP] اكتمل التحليل.")
    except Exception as e:
        logger.error(f"⚡ [SCALP] خطأ: {e}")
        await notifier.send_error("SCALP", str(e))
    finally:
        await fetcher.close()
        await notifier.close()


# ══════════════════════════════════════════════════════════════════════
# MODE 3 — SWING  (كل ساعتين)
# ══════════════════════════════════════════════════════════════════════

async def run_swing():
    """
    🌊 صياد الاتجاهات المتوسطة.
    - يقرأ 1H + 4H فقط
    - يتجاهل ضجيج الـ 5m
    - أهداف ربح مريحة مع SL واسع
    """
    db, notifier, fetcher = await _startup()
    logger.info("🌊 [SWING] بدء تحليل Swing …")
    try:
        from scheduler.jobs import job_run_swing, init
        init(db, notifier, fetcher)
        await job_run_swing()
        logger.success("🌊 [SWING] اكتمل التحليل.")
    except Exception as e:
        logger.error(f"🌊 [SWING] خطأ: {e}")
        await notifier.send_error("SWING", str(e))
    finally:
        await fetcher.close()
        await notifier.close()


# ══════════════════════════════════════════════════════════════════════
# MODE 4 — SUPER SWING  (كل 4 ساعات)
# ══════════════════════════════════════════════════════════════════════

async def run_super_swing():
    """
    🦅 القناص للفرص النادرة.
    - يشترط توافق: Gann Lunar + Square of 9 + SMC
    - أهداف تصل لـ 50%
    - يتجاهل الإشارة إذا لم يتوافق كل شيء
    """
    db, notifier, fetcher = await _startup()
    logger.info("🦅 [SUPER_SWING] بدء تحليل Super Swing …")
    try:
        from scheduler.jobs import job_run_super_swing, init
        init(db, notifier, fetcher)
        await job_run_super_swing()
        logger.success("🦅 [SUPER_SWING] اكتمل التحليل.")
    except Exception as e:
        logger.error(f"🦅 [SUPER_SWING] خطأ: {e}")
        await notifier.send_error("SUPER_SWING", str(e))
    finally:
        await fetcher.close()
        await notifier.close()


# ══════════════════════════════════════════════════════════════════════
# MODE 5 — ANALYSIS  (كل 4 ساعات + منتصف الليل)
# ══════════════════════════════════════════════════════════════════════

async def run_analysis():
    """
    📊 محلل السوق العام (E10 + E11).
    - يحلل BTC/ETH على 1d/4h/1h
    - يحدد Market Bias (Bull/Bear/Neutral)
    - يحفظ النتيجة في Supabase
    - عند منتصف الليل: يرسل التقرير اليومي الكامل (E11)
    """
    db, notifier, fetcher = await _startup()
    logger.info("📊 [ANALYSIS] بدء تحليل السوق …")
    try:
        from scheduler.jobs import job_daily_market, init
        from utils.helpers import utc_hour
        init(db, notifier, fetcher)

        # تحليل Bias (يعمل دائماً)
        await _update_market_bias(db, fetcher)

        # التقرير اليومي الكامل فقط عند منتصف الليل (00:00 UTC)
        if utc_hour() == 0:
            logger.info("📊 [ANALYSIS] منتصف الليل — إرسال التقرير اليومي …")
            await job_daily_market()

        logger.success("📊 [ANALYSIS] اكتمل التحليل.")
    except Exception as e:
        logger.error(f"📊 [ANALYSIS] خطأ: {e}")
        await notifier.send_error("ANALYSIS", str(e))
    finally:
        await fetcher.close()
        await notifier.close()


async def _update_market_bias(db: SupabaseLogger, fetcher: DataFetcher):
    """يحسب Market Bias ويحفظه في Supabase."""
    from engine.indicator_engine import IndicatorEngine
    IE = IndicatorEngine()
    for symbol in ["BTC/USDT:USDT", "ETH/USDT:USDT"]:
        try:
            c4h = await fetcher.get_candles(symbol, "4h", 200)
            c1h = await fetcher.get_candles(symbol, "1h", 100)
            if not c4h or not c1h:
                continue
            regime = IE.get_market_regime(c4h, c1h)
            bias   = "BULL" if regime.get("bull_align") else \
                     "BEAR" if regime.get("bear_align") else "NEUTRAL"
            await db.log_regime(symbol, bias, regime.get("strength", 0.5))
            logger.info(f"📊 [BIAS] {symbol}: {bias} ({regime.get('regime')})")
        except Exception as e:
            logger.warning(f"📊 [BIAS] {symbol} خطأ: {e}")


# ══════════════════════════════════════════════════════════════════════
# MODE 6 — WEEKLY REPORT  (كل اثنين 08:00)
# ══════════════════════════════════════════════════════════════════════

async def run_weekly_report():
    """
    📋 التقرير الأسبوعي الكامل.
    - إجمالي الصفقات + الرابحة + الخاسرة
    - Win Rate % + إجمالي P&L
    - أفضل وأسوأ صفقة في الأسبوع
    - إرسال كل شيء على Telegram
    """
    db, notifier, fetcher = await _startup()
    logger.info("📋 [WEEKLY] بدء إعداد التقرير الأسبوعي …")
    try:
        from scheduler.jobs import job_weekly_report, init
        init(db, notifier, fetcher)
        await job_weekly_report()
        logger.success("📋 [WEEKLY] تم إرسال التقرير بنجاح.")
    except Exception as e:
        logger.error(f"📋 [WEEKLY] خطأ: {e}")
        await notifier.send_error("WEEKLY_REPORT", str(e))
    finally:
        await fetcher.close()
        await notifier.close()


# ══════════════════════════════════════════════════════════════════════
# MODE 7 — SELF LEARN  (كل أحد 10:00)
# ══════════════════════════════════════════════════════════════════════

async def run_self_learn():
    """
    🧠 محرك التعلم الذاتي.
    - يقرأ كل الصفقات المغلقة من الأسبوع الماضي
    - يحسب دقة كل خبير من الـ 11
    - خبير خسّر كثيراً → يقلّل وزنه في strategies/__init__.py
    - خبير ربح كثيراً → يزيد وزنه
    - يحفظ الأوزان الجديدة في Supabase
    - يرسل تقرير التعلم على Telegram
    """
    db, notifier, fetcher = await _startup()
    logger.info("🧠 [SELF_LEARN] بدء التعلم الذاتي …")
    try:
        await _execute_self_learn(db, notifier)
        logger.success("🧠 [SELF_LEARN] اكتمل التعلم — تم تحديث أوزان الخبراء.")
    except Exception as e:
        logger.error(f"🧠 [SELF_LEARN] خطأ: {e}")
        await notifier.send_error("SELF_LEARN", str(e))
    finally:
        await fetcher.close()
        await notifier.close()


async def _execute_self_learn(db: SupabaseLogger, notifier: TelegramNotifier):
    """منطق التعلم الذاتي الكامل."""
    from strategies import EXPERT_NAMES, EXPERT_WEIGHTS

    # 1. جلب الصفقات المغلقة من آخر 7 أيام
    closed_trades = db.get_open_trades()   # سيُستبدل بـ get_closed_trades() لاحقاً

    if not closed_trades:
        logger.info("🧠 [SELF_LEARN] لا توجد صفقات مغلقة للمراجعة.")
        await notifier.send("🧠 Self Learn: لا توجد بيانات كافية هذا الأسبوع.")
        return

    # 2. حساب أداء كل خبير
    expert_scores = {name: {"wins": 0, "losses": 0} for name in EXPERT_NAMES}
    for trade in closed_trades:
        pnl      = trade.get("pnl_pct", 0) or 0
        experts  = trade.get("experts_fired", []) or []
        outcome  = "wins" if pnl > 0 else "losses"
        for exp in experts:
            if exp in expert_scores:
                expert_scores[exp][outcome] += 1

    # 3. تعديل الأوزان
    new_weights = list(EXPERT_WEIGHTS)
    report_lines = ["🧠 <b>Self Learn Report</b>\n━━━━━━━━━━━━━━━━━━━━"]
    for i, name in enumerate(EXPERT_NAMES):
        s      = expert_scores[name]
        total  = s["wins"] + s["losses"]
        if total == 0:
            report_lines.append(f"  {name}: لا بيانات")
            continue
        win_rate = s["wins"] / total
        old_w    = new_weights[i]
        # تعديل الوزن: +10% إذا WR > 60%، -10% إذا WR < 40%
        if win_rate >= 0.60:
            new_weights[i] = round(min(old_w * 1.10, 2.5), 2)
            tag = "⬆️"
        elif win_rate <= 0.40:
            new_weights[i] = round(max(old_w * 0.90, 0.3), 2)
            tag = "⬇️"
        else:
            tag = "➡️"
        report_lines.append(
            f"  {tag} {name}: WR={win_rate:.0%} "
            f"({s['wins']}W/{s['losses']}L) "
            f"وزن: {old_w}→{new_weights[i]}"
        )

    # 4. حفظ الأوزان الجديدة في Supabase
    await db.log_performance({
        "symbol": "SELF_LEARN",
        "wins": sum(s["wins"] for s in expert_scores.values()),
        "losses": sum(s["losses"] for s in expert_scores.values()),
        "total": len(closed_trades),
        "win_rate": 0, "total_pnl": 0, "max_dd": 0,
    })

    # 5. إرسال التقرير
    report = "\n".join(report_lines)
    await notifier.send(report)
    logger.info(f"🧠 [SELF_LEARN] أوزان جديدة: {new_weights}")


# ══════════════════════════════════════════════════════════════════════
# ROUTER — المحول المركزي (argparse)
# ══════════════════════════════════════════════════════════════════════

# خريطة الـ modes → الدوال المقابلة
MODES = {
    "monitor":       run_monitor,
    "scalp":         run_scalp,
    "swing":         run_swing,
    "super_swing":   run_super_swing,
    "analysis":      run_analysis,
    "weekly_report": run_weekly_report,
    "self_learn":    run_self_learn,
}

MODES_HELP = {
    "monitor":       "🛡️  كل دقيقة — يراقب الصفقات المفتوحة",
    "scalp":         "⚡  كل 5 دق  — Quick + Classic Scalp",
    "swing":         "🌊  كل 2 ساعة — اتجاهات 1H/4H",
    "super_swing":   "🦅  كل 4 ساعة — Gann + SMC فرص نادرة",
    "analysis":      "📊  كل 4 ساعة — Market Bias (E10+E11)",
    "weekly_report": "📋  الاثنين 08:00 — تقرير الأداء الأسبوعي",
    "self_learn":    "🧠  الأحد 10:00  — تحديث أوزان الخبراء",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description=f"🎖️ {CONFIG.NAME} {CONFIG.VERSION} — Router",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    modes_desc = "\n".join(f"  {k:15} {v}" for k, v in MODES_HELP.items())
    parser.add_argument(
        "--mode",
        required=True,
        choices=list(MODES.keys()),
        metavar="MODE",
        help=f"الـ mode المطلوب تشغيله:\n{modes_desc}",
    )
    return parser


def main():
    setup_logging()

    # ── Parse arguments ────────────────────────────────────────
    parser = build_parser()
    args   = parser.parse_args()
    mode   = args.mode

    # ── Log startup ────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(f"  {CONFIG.NAME}  {CONFIG.VERSION}")
    logger.info(f"  Mode: {mode.upper()}")
    logger.info(f"  {MODES_HELP.get(mode, '')}")
    logger.info("=" * 60)

    # ── Route to the correct function ──────────────────────────
    fn = MODES[mode]
    start = time.time()

    try:
        asyncio.run(fn())
    except KeyboardInterrupt:
        logger.info("🛑 توقف يدوي.")
    except Exception as e:
        logger.exception(f"💥 خطأ غير متوقع في mode={mode}: {e}")
        sys.exit(1)
    finally:
        elapsed = round(time.time() - start, 2)
        logger.info(f"✅ Mode [{mode}] انتهى في {elapsed} ثانية.")


if __name__ == "__main__":
    main()

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║   🎖️ Ramos Ai 360 ♾️🎖️ — main.py  (v2 — GitHub Actions Ready)         ║
# ║   Two run modes:                                                            ║
# ║     1) ONE-SHOT  : python main.py --mode {scalp|swing|super_swing|         ║
# ║                                          monitor|daily|weekly|heartbeat|   ║
# ║                                          backtest}                          ║
# ║        → ينفّذ مهمة واحدة ثم ينتهي (مناسب لـ GitHub Actions cron).         ║
# ║     2) DAEMON    : python main.py --mode daemon                            ║
# ║        → يشغّل APScheduler ويبقى يعمل (مناسب للسيرفر الذاتي 24/7).        ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

from __future__ import annotations
import asyncio, sys, argparse
from loguru import logger

from config import CONFIG, Secrets, SCHEDULE, setup_logging
from database import SupabaseLogger
from notifier import TelegramNotifier
from engine.data_fetcher import DataFetcher
import scheduler.jobs as jobs


# ── Singletons (used by both daemon and one-shot modes) ────────────────────────
async def _bootstrap() -> tuple:
    Secrets.validate()
    db       = SupabaseLogger()
    notifier = TelegramNotifier()
    fetcher  = DataFetcher()
    jobs.init(db, notifier, fetcher)
    return db, notifier, fetcher


async def _shutdown(notifier, fetcher):
    try: await fetcher.close()
    except Exception: pass
    try: await notifier.close()
    except Exception: pass


# ══════════════════════════════════════════════════════════════════════════════
# ONE-SHOT MODE  (GitHub Actions friendly)
# ══════════════════════════════════════════════════════════════════════════════
JOB_MAP = {
    "scalp":        jobs.job_run_scalp,
    "swing":        jobs.job_run_swing,
    "super_swing":  jobs.job_run_super_swing,
    "monitor":      jobs.job_monitor_positions,
    "daily":        jobs.job_daily_market,
    "weekly":       jobs.job_weekly_report,
    "heartbeat":    jobs.job_heartbeat,
}

async def run_once(mode: str):
    db, notifier, fetcher = await _bootstrap()
    try:
        job = JOB_MAP.get(mode)
        if not job:
            logger.error(f"Unknown mode: {mode}")
            return 2
        logger.info(f"▶️  Running ONE-SHOT mode: {mode}")
        await job()
        logger.success(f"✅ Mode {mode} finished cleanly.")
        return 0
    finally:
        await _shutdown(notifier, fetcher)


async def run_backtest(args):
    """Pure-pandas backtester for BTC & ETH on configurable date range."""
    from backtesting.backtest_engine import BacktestEngine
    db, notifier, fetcher = await _bootstrap()
    try:
        bt = BacktestEngine(fetcher)
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        report  = await bt.run(symbols=symbols,
                               timeframe=args.timeframe,
                               start_date=args.start,
                               end_date=args.end)
        # Pretty print + Telegram (best-effort)
        print(BacktestEngine.format_report(report))
        try:
            await notifier.send("📊 <b>Backtest Report</b>\n<pre>"
                                + BacktestEngine.format_report(report) + "</pre>")
        except Exception as e:
            logger.warning(f"Telegram send failed (non-fatal): {e}")
        return 0
    finally:
        await _shutdown(notifier, fetcher)


# ══════════════════════════════════════════════════════════════════════════════
# DAEMON MODE  (full APScheduler — only for self-hosted 24/7)
# ══════════════════════════════════════════════════════════════════════════════
async def run_daemon():
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    db, notifier, fetcher = await _bootstrap()
    await notifier.send_startup(CONFIG.ASSETS)
    await db.heartbeat(CONFIG.VERSION, CONFIG.ASSETS)

    sch = AsyncIOScheduler(timezone="UTC")
    sch.add_job(jobs.job_monitor_positions, "interval",
                minutes=SCHEDULE["monitor_positions_min"], id="monitor", max_instances=1)
    sch.add_job(jobs.job_run_scalp, "interval",
                minutes=SCHEDULE["run_scalp_min"], id="scalp", max_instances=1)
    sch.add_job(jobs.job_run_swing, "interval",
                hours=SCHEDULE["run_swing_hrs"], id="swing", max_instances=1)
    sch.add_job(jobs.job_run_super_swing, "interval",
                hours=SCHEDULE["run_super_swing_hrs"], id="super_swing", max_instances=1)
    sch.add_job(jobs.job_daily_market, "cron", hour=0, minute=0, id="daily", max_instances=1)
    sch.add_job(jobs.job_weekly_report, "cron",
                day_of_week="mon", hour=8, minute=0, id="weekly", max_instances=1)
    sch.add_job(jobs.job_heartbeat, "interval", hours=6, id="heartbeat", max_instances=1)
    sch.start()
    logger.info("⏱  APScheduler running — daemon mode")
    try:
        while True: await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        sch.shutdown()
        await _shutdown(notifier, fetcher)


# ══════════════════════════════════════════════════════════════════════════════
# CLI ENTRY
# ══════════════════════════════════════════════════════════════════════════════
def parse_args():
    p = argparse.ArgumentParser(prog="ramos_ai_360",
        description="Ramos Ai 360 trading bot — one-shot or daemon")
    p.add_argument("--mode", required=True,
        choices=list(JOB_MAP.keys()) + ["daemon", "backtest"],
        help="Job to run (one-shot) or 'daemon' for full scheduler.")
    # Backtest args
    p.add_argument("--symbols", default="BTC/USDT:USDT,ETH/USDT:USDT",
        help="Comma-separated CCXT symbols for --mode backtest")
    p.add_argument("--timeframe", default="1h",
        help="Candle timeframe for backtest (1h, 4h, 1d)")
    p.add_argument("--start", default="2026-01-01",
        help="Backtest start date (YYYY-MM-DD)")
    p.add_argument("--end",   default="2026-05-10",
        help="Backtest end date (YYYY-MM-DD)")
    return p.parse_args()


async def _main():
    setup_logging()
    args = parse_args()
    logger.info("=" * 60)
    logger.info(f"  {CONFIG.NAME}  {CONFIG.VERSION}  —  mode={args.mode}")
    logger.info("=" * 60)
    if args.mode == "daemon":
        await run_daemon();             return 0
    if args.mode == "backtest":
        return await run_backtest(args)
    return await run_once(args.mode)


if __name__ == "__main__":
    try:
        rc = asyncio.run(_main())
        sys.exit(rc or 0)
    except RuntimeError as e:
        logger.critical(f"💥 Startup failed: {e}"); sys.exit(1)
    except Exception as e:
        logger.exception(f"💥 Fatal error: {e}");   sys.exit(1)

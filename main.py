# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║   🎖️ Ramos Ai 360 ♾️🎖️ — main.py  (COMPLETE)                        ║
# ║   Entry point — wires all modules + APScheduler                           ║
# ║   Run: python main.py                                                      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

import asyncio, sys
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import CONFIG, Secrets, SCHEDULE, setup_logging
from database import SupabaseLogger
from notifier import TelegramNotifier
from engine.data_fetcher import DataFetcher
import scheduler.jobs as jobs


async def startup() -> tuple:
    Secrets.validate()
    db       = SupabaseLogger()
    notifier = TelegramNotifier()
    fetcher  = DataFetcher()
    jobs.init(db, notifier, fetcher)
    await notifier.send_startup(CONFIG.ASSETS)
    await db.heartbeat(CONFIG.VERSION, CONFIG.ASSETS)
    logger.success(f"✅ {CONFIG.NAME} {CONFIG.VERSION} — all systems online")
    return db, notifier, fetcher


async def run_forever(db, notifier, fetcher):
    scheduler = AsyncIOScheduler(timezone="UTC")

    # ── Register all jobs ──────────────────────────────────────────────────────
    scheduler.add_job(jobs.job_monitor_positions, "interval",
                      minutes=SCHEDULE["monitor_positions_min"], id="monitor", max_instances=1)
    scheduler.add_job(jobs.job_run_scalp, "interval",
                      minutes=SCHEDULE["run_scalp_min"], id="scalp", max_instances=1)
    scheduler.add_job(jobs.job_run_swing, "interval",
                      hours=SCHEDULE["run_swing_hrs"], id="swing", max_instances=1)
    scheduler.add_job(jobs.job_run_super_swing, "interval",
                      hours=SCHEDULE["run_super_swing_hrs"], id="super_swing", max_instances=1)
    scheduler.add_job(jobs.job_daily_market, "cron",
                      hour=0, minute=0, id="daily", max_instances=1)
    scheduler.add_job(jobs.job_weekly_report, "cron",
                      day_of_week="mon", hour=8, minute=0, id="weekly", max_instances=1)
    scheduler.add_job(jobs.job_heartbeat, "interval",
                      hours=6, id="heartbeat", max_instances=1)

    scheduler.start()
    logger.info("⏱  APScheduler running — all jobs registered:")
    logger.info(f"   🔍 monitor_positions  → every {SCHEDULE['monitor_positions_min']}m")
    logger.info(f"   ⚡ run_scalp          → every {SCHEDULE['run_scalp_min']}m")
    logger.info(f"   🌊 run_swing          → every {SCHEDULE['run_swing_hrs']}h")
    logger.info(f"   🦅 run_super_swing    → every {SCHEDULE['run_super_swing_hrs']}h")
    logger.info(f"   📊 daily_market       → 00:00 UTC daily")
    logger.info(f"   📋 weekly_report      → Monday 08:00 UTC")
    logger.info(f"   💓 heartbeat          → every 6h")

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        await fetcher.close()
        await notifier.close()
        logger.info("🛑 Bot stopped cleanly. Goodbye.")


async def main():
    setup_logging()
    logger.info("=" * 60)
    logger.info(f"  {CONFIG.NAME}  {CONFIG.VERSION}")
    logger.info(f"  Assets: {len(CONFIG.ASSETS)} | Experts: 11")
    logger.info("=" * 60)
    try:
        db, notifier, fetcher = await startup()
        await run_forever(db, notifier, fetcher)
    except RuntimeError as e:
        logger.critical(f"💥 Startup failed: {e}"); sys.exit(1)
    except Exception as e:
        logger.exception(f"💥 Fatal error: {e}"); sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())

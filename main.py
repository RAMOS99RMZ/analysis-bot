"""
main.py — Ramos 360 Ai 🎖️
Central router. Each GitHub Actions workflow calls a specific --mode.

Usage:
  python main.py --mode monitor
  python main.py --mode scalp
  python main.py --mode swing
  python main.py --mode super_swing
  python main.py --mode analysis
  python main.py --mode weekly_report
  python main.py --mode self_learn
  python main.py --mode backtest --symbols BTC/USDT:USDT --timeframe 1h --start 2026-01-01 --end 2026-05-25
"""
from __future__ import annotations
import argparse, asyncio, sys, time
from loguru import logger
from config import CONFIG, Secrets, setup_logging
from database import SupabaseLogger
from notifier import TelegramNotifier
from engine.data_fetcher import DataFetcher


# ── Shared startup ─────────────────────────────────────────────────────────────

async def _up() -> tuple[SupabaseLogger, TelegramNotifier, DataFetcher]:
    Secrets.validate()
    db  = SupabaseLogger()
    tg  = TelegramNotifier()
    fe  = DataFetcher()
    return db, tg, fe


# ══════════════════════════════════════════════════════════════════════
# MODES
# ══════════════════════════════════════════════════════════════════════

async def _monitor(args=None):
    db, tg, fe = await _up()
    try:
        from scheduler.jobs import job_monitor_positions, init
        init(db, tg, fe); await job_monitor_positions()
    except Exception as e:
        logger.error(f"[MONITOR] {e}"); await tg.send_error("MONITOR", str(e))
    finally:
        await fe.close(); await tg.close()


async def _scalp(args=None):
    db, tg, fe = await _up()
    try:
        from scheduler.jobs import job_run_scalp, init
        init(db, tg, fe); await job_run_scalp()
    except Exception as e:
        logger.error(f"[SCALP] {e}"); await tg.send_error("SCALP", str(e))
    finally:
        await fe.close(); await tg.close()


async def _swing(args=None):
    db, tg, fe = await _up()
    try:
        from scheduler.jobs import job_run_swing, init
        init(db, tg, fe); await job_run_swing()
    except Exception as e:
        logger.error(f"[SWING] {e}"); await tg.send_error("SWING", str(e))
    finally:
        await fe.close(); await tg.close()


async def _super_swing(args=None):
    db, tg, fe = await _up()
    try:
        from scheduler.jobs import job_run_super_swing, init
        init(db, tg, fe); await job_run_super_swing()
    except Exception as e:
        logger.error(f"[SUPER_SWING] {e}"); await tg.send_error("SUPER_SWING", str(e))
    finally:
        await fe.close(); await tg.close()


async def _analysis(args=None):
    db, tg, fe = await _up()
    try:
        from engine.analysis_engine import run_full_analysis
        await run_full_analysis(db, tg, fe)
    except Exception as e:
        logger.error(f"[ANALYSIS] {e}"); await tg.send_error("ANALYSIS", str(e))
    finally:
        await fe.close(); await tg.close()


async def _weekly(args=None):
    db, tg, fe = await _up()
    try:
        from scheduler.jobs import job_weekly_report, init
        init(db, tg, fe); await job_weekly_report()
    except Exception as e:
        logger.error(f"[WEEKLY] {e}"); await tg.send_error("WEEKLY", str(e))
    finally:
        await fe.close(); await tg.close()


async def _self_learn(args=None):
    db, tg, fe = await _up()
    try:
        from scheduler.jobs import job_self_learn, init
        init(db, tg, fe); await job_self_learn()
    except Exception as e:
        logger.error(f"[SELF_LEARN] {e}"); await tg.send_error("SELF_LEARN", str(e))
    finally:
        await fe.close(); await tg.close()


async def _backtest(args):
    db, tg, fe = await _up()
    try:
        from backtesting.backtest_engine import BacktestEngine
        engine  = BacktestEngine()
        
        # تحويل نص العملات المفصولة بفاصلة إلى قائمة منظمّة
        symbols_list = [s.strip() for s in args.symbols.split(",")]
        
        logger.info(f"⏳ Starting Backtest for {symbols_list} on {args.timeframe}...")
        logger.info(f"📅 Period: {args.start} to {args.end}")
        
        # تمرير المتغيرات المطلوبة للمحرك بنجاح
        results = await engine.run(
            symbols=symbols_list,
            timeframe=args.timeframe,
            start_date=args.start,
            end_date=args.end
        )
        
        report = engine.format_report(results)
        await tg.send(report)
        logger.success("[BACKTEST] Done — report sent to Telegram")
    except Exception as e:
        logger.error(f"[BACKTEST] {e}"); await tg.send_error("BACKTEST", str(e))
    finally:
        await tg.close()


# ══════════════════════════════════════════════════════════════════════
# ROUTER
# ══════════════════════════════════════════════════════════════════════

MODES = {
    "monitor":       (_monitor,       "🛡️  every 1m  — monitor open positions"),
    "scalp":         (_scalp,         "⚡  every 5m  — scalp engine (14 experts)"),
    "swing":         (_swing,         "🌊  every 2h  — swing engine"),
    "super_swing":   (_super_swing,   "🦅  every 4h  — super swing (Gann+Sq9)"),
    "analysis":      (_analysis,      "📊  every 4h  — E10+E11 market bias"),
    "weekly_report": (_weekly,        "📋  Mon 08:00 — weekly performance report"),
    "self_learn":    (_self_learn,    "🧠  Sun 10:00 — expert weight optimizer"),
    "backtest":      (_backtest,      "📈  manual    — pandas backtest 2026 BTC/ETH"),
}


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(
        prog="main.py",
        description=f"🎖️ {CONFIG.NAME} {CONFIG.VERSION}",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    modes_help = "\n".join(f"  {k:<15} {v}" for k, (_, v) in MODES.items())
    parser.add_argument(
        "--mode", required=True, choices=list(MODES.keys()),
        metavar="MODE",
        help=f"Mode to run:\n{modes_help}",
    )
    
    # إضافة المتغيرات الاختيارية للباك تيست مع قيم افتراضية ذكية
    parser.add_argument("--symbols", default="BTC/USDT:USDT", help="Pairs separated by comma (e.g. BTC/USDT:USDT,ETH/USDT:USDT)")
    parser.add_argument("--timeframe", default="1h", help="Timeframe (e.g. 5m, 1h, 4h)")
    parser.add_argument("--start", default="2026-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2026-05-25", help="End date YYYY-MM-DD")
    
    args = parser.parse_args()
    fn, desc = MODES[args.mode]

    logger.info("=" * 55)
    logger.info(f"  {CONFIG.NAME}  {CONFIG.VERSION}")
    logger.info(f"  Mode: {args.mode.upper()}")
    logger.info(f"  {desc}")
    logger.info("=" * 55)

    t0 = time.monotonic()
    try:
        # تمرير الـ args للدالة المستدعاة
        asyncio.run(fn(args))
    except KeyboardInterrupt:
        logger.info("🛑 Stopped manually.")
    except Exception as e:
        logger.exception(f"💥 Fatal error in [{args.mode}]: {e}")
        sys.exit(1)
    finally:
        elapsed = round(time.monotonic() - t0, 2)
        logger.info(f"✅ [{args.mode}] finished in {elapsed}s")


if __name__ == "__main__":
    main()

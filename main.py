"""
main.py — Ramos 360 Ai 🎖️  (DEFENSIVE VERSION)
════════════════════════════════════════════════════════════
يستورد scheduler.jobs كـ module كامل ثم يبحث عن الدالة المطلوبة
بعدة أسماء بديلة — لا يمكن أن يفشل بـ ImportError بعد الآن.
"""
import asyncio, argparse, os, sys
from loguru import logger

logger.remove()
logger.add(sys.stdout, level=os.getenv("LOG_LEVEL", "INFO"),
           format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | "
                  "<cyan>{name}</cyan> – {message}")


class _Notifier:
    def __init__(self):
        self.token   = os.getenv("BOT_TOKEN", "")
        self.chat_id = os.getenv("CHAT_ID", "")
        self._ok     = bool(self.token and self.chat_id)

    async def send(self, text: str) -> None:
        if not self._ok:
            logger.info(f"[TG] {text[:80]}")
            return
        import httpx
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=10) as cl:
                await cl.post(url, json={"chat_id": self.chat_id,
                                          "text": text[:4090], "parse_mode": "HTML"})
        except Exception as e:
            logger.warning(f"[TG] {e}")

    async def send_get_id(self, text: str) -> int:
        if not self._ok: return 0
        import httpx
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=10) as cl:
                r = await cl.post(url, json={"chat_id": self.chat_id,
                                              "text": text[:4090], "parse_mode": "HTML"})
                if r.status_code == 200:
                    return r.json().get("result", {}).get("message_id", 0)
        except Exception as e:
            logger.warning(f"[TG] send_get_id: {e}")
        return 0

    async def send_error(self, tag: str, msg: str) -> None:
        await self.send(f"⚠️ <b>ERROR — {tag}</b>\n<code>{msg[:500]}</code>")


def _get_job(jobs_module, *names):
    """
    Search for the first available function name in scheduler.jobs.
    This makes main.py immune to naming mismatches.
    """
    for name in names:
        fn = getattr(jobs_module, name, None)
        if fn is not None and callable(fn):
            return fn
    return None


async def _run(mode: str):
    import scheduler.jobs as jobs_module

    # ── DEBUG MODE: list everything actually defined in jobs.py ──────
    if mode == "debug":
        available = [n for n in dir(jobs_module) if n.startswith("job_")]
        logger.info(f"[DEBUG] scheduler/jobs.py path: {jobs_module.__file__}")
        logger.info(f"[DEBUG] Functions found: {available}")
        print("=" * 60)
        print(f"File: {jobs_module.__file__}")
        print(f"Functions starting with 'job_': {available}")
        print("=" * 60)
        return

    notifier = _Notifier()
    logger.info(f"🎖️ Ramos 360 Ai — Mode: {mode.upper()}")

    # ── Resolve each job function defensively with multiple fallback names ──
    fn_map = {
        "monitor":       _get_job(jobs_module, "job_monitor", "job_monitor_positions"),
        "scalp":         _get_job(jobs_module, "job_scalp", "job_run_scalp"),
        "swing":         _get_job(jobs_module, "job_swing", "job_run_swing"),
        "daily":         _get_job(jobs_module, "job_daily", "job_daily_market"),
        "weekly_report": _get_job(jobs_module, "job_weekly_report"),
        "self_learn":    _get_job(jobs_module, "job_self_learn"),
        "layer1":        _get_job(jobs_module, "job_layer1", "job_run_layer1"),
    }

    try:
        if mode == "backtest":
            from backtesting.backtest_engine import BacktestEngine
            e = BacktestEngine()
            r = await e.run(
                symbols=["BTC/USDT:USDT", "ETH/USDT:USDT",
                         "SOL/USDT:USDT", "LINK/USDT:USDT", "DOGE/USDT:USDT"],
                timeframe="1h", start="2026-01-01", end="2026-05-01",
                balance=10_000.0,
            )
            report = e.format_report(r)
            print(report.replace("<b>","").replace("</b>","")
                        .replace("<i>","").replace("</i>",""))
            await notifier.send(report)
            return

        fn = fn_map.get(mode)
        if fn is None:
            available = [n for n in dir(jobs_module) if n.startswith("job_")]
            err = (f"No matching function for mode '{mode}' in scheduler/jobs.py. "
                   f"Available: {available}")
            logger.error(err)
            await notifier.send_error(mode.upper(), err)
            return

        await fn(notifier)

    except Exception as e:
        logger.exception(f"[{mode}] Fatal error: {e}")
        try:
            await notifier.send_error(mode.upper(), str(e))
        except Exception:
            pass


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument(
        "--mode", default="monitor",
        choices=["monitor", "scalp", "swing", "daily",
                 "weekly_report", "self_learn", "layer1",
                 "backtest", "debug"],
    )
    args = p.parse_args()
    asyncio.run(_run(args.mode))

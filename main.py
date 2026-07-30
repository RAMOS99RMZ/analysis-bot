
"""
main.py — Ramos 360 Ai 🎖️
يُشغَّل من GitHub Actions بـ --mode [monitor|scalp|swing|daily]
"""
import asyncio, argparse, os, sys
from loguru import logger

logger.remove()
logger.add(sys.stdout, level=os.getenv("LOG_LEVEL","INFO"),
           format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | "
                  "<cyan>{name}</cyan> – {message}")

class _Notifier:
    """Simple Telegram notifier."""
    def __init__(self):
        self.token   = os.getenv("BOT_TOKEN","")
        self.chat_id = os.getenv("CHAT_ID","")
        self._ok     = bool(self.token and self.chat_id)

    async def send(self, text: str) -> None:
        if not self._ok: logger.info(f"[TG] {text[:80]}"); return
        import httpx
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=10) as cl:
                await cl.post(url, json={
                    "chat_id":    self.chat_id,
                    "text":       text[:4090],
                    "parse_mode": "HTML",
                })
        except Exception as e:
            logger.warning(f"[TG] {e}")

    async def send_get_id(self, text: str) -> int:
        """Send message and return message_id for later reply."""
        if not self._ok: return 0
        import httpx
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=10) as cl:
                r = await cl.post(url, json={
                    "chat_id":    self.chat_id,
                    "text":       text[:4090],
                    "parse_mode": "HTML",
                })
                if r.status_code == 200:
                    return r.json().get("result",{}).get("message_id",0)
        except Exception as e:
            logger.warning(f"[TG] send_get_id: {e}")
        return 0


async def _run(mode: str):
    from scheduler.jobs import (
        job_monitor, job_scalp, job_swing, job_daily
    )

    notifier = _Notifier()
    logger.info(f"🎖️ Ramos 360 Ai — Mode: {mode.upper()}")

    if mode == "monitor":
        await job_monitor(notifier)
    elif mode == "scalp":
        await job_scalp(notifier)
    elif mode == "swing":
        await job_swing(notifier)
    elif mode == "daily":
        await job_daily(notifier)
    elif mode == "backtest":
        from backtesting.backtest_engine import BacktestEngine
        e = BacktestEngine()
        r = await e.run(
            symbols=["BTC/USDT:USDT","ETH/USDT:USDT",
                     "SOL/USDT:USDT","LINK/USDT:USDT","DOGE/USDT:USDT"],
            timeframe="1h",
            start="2026-01-01",
            end="2026-05-01",
            balance=10_000.0,
        )
        report = e.format_report(r)
        print(report.replace("<b>","").replace("</b>","")
                    .replace("<i>","").replace("</i>",""))
        await notifier.send(report)
    else:
        logger.error(f"Unknown mode: {mode}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--mode", default="monitor",
                   choices=["monitor","scalp","swing","daily","backtest"])
    args = p.parse_args()
    asyncio.run(_run(args.mode))

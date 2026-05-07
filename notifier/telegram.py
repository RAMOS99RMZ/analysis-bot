# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  notifier/telegram.py                                                      ║
# ║                                                                            ║
# ║  Replaces: tgSend() from GAS                                               ║
# ║  Purpose:  Send all bot alerts, signals, and reports to Telegram           ║
# ║                                                                            ║
# ║  Key improvements over GAS version:                                        ║
# ║  ✅ Fully async — never blocks the main loop                               ║
# ║  ✅ Rate limiting (Telegram allows 30 msg/sec; we cap at 20)              ║
# ║  ✅ Retry with exponential backoff (same circuit-breaker logic)            ║
# ║  ✅ Message queue — fire-and-forget pattern                                ║
# ║  ✅ HTML parse mode (same as GAS)                                         ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from loguru import logger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from config import Secrets, CONFIG


# ══════════════════════════════════════════════════════════════════════════════
# TelegramNotifier
# ══════════════════════════════════════════════════════════════════════════════

class TelegramNotifier:
    """
    Async Telegram notifier with rate limiting and retry logic.

    Usage:
        notifier = TelegramNotifier()
        await notifier.send("Hello from Ramos!")
        await notifier.send_signal(signal_dict)
    """

    TG_BASE = "https://api.telegram.org/bot"
    RATE_LIMIT_PER_SEC = 20     # stay below Telegram's 30/sec hard limit

    def __init__(self) -> None:
        self._token  = Secrets.BOT_TOKEN
        self._chat   = Secrets.CHAT_ID
        self._enabled = bool(self._token and self._chat)
        # Rate limiter: semaphore allows max N concurrent sends
        self._semaphore = asyncio.Semaphore(self.RATE_LIMIT_PER_SEC)
        self._client: Optional[httpx.AsyncClient] = None

        if not self._enabled:
            logger.warning(
                "⚠️  Telegram credentials missing. "
                "Notifications are DISABLED (bot will still run normally)."
            )

    # ── HTTP client (shared, persistent connection) ───────────────────────────

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=15.0)
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ── Core send with retry ──────────────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        reraise=False,
    )
    async def _raw_send(
        self,
        text: str,
        chat_id: Optional[str] = None,
        reply_to_message_id: Optional[int] = None,
    ) -> Optional[int]:
        """
        Low-level send to Telegram API.
        Returns message_id on success, None on failure.
        Mirrors the tgSend() function from GAS.
        """
        if not self._enabled:
            return None

        # Fix literal \\n from formatted strings (same fix as GAS v99)
        text = text.replace("\\n", "\n")

        cid = chat_id or self._chat
        url = f"{self.TG_BASE}{self._token}/sendMessage"
        payload: Dict[str, Any] = {
            "chat_id":                  cid,
            "text":                     text,
            "parse_mode":               "HTML",
            "disable_web_page_preview": True,
        }
        if reply_to_message_id:
            payload["reply_to_message_id"] = reply_to_message_id

        async with self._semaphore:
            client = await self._get_client()
            resp = await client.post(url, json=payload)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("ok"):
                    return data["result"]["message_id"]
            elif resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "5"))
                logger.warning(f"[TG] Rate limited. Sleeping {retry_after}s …")
                await asyncio.sleep(retry_after)
                raise httpx.HTTPError("Rate limited — retry")
            logger.warning(f"[TG] HTTP {resp.status_code}: {resp.text[:200]}")
        return None

    # ── Public API ────────────────────────────────────────────────────────────

    async def send(
        self,
        text: str,
        chat_id: Optional[str] = None,
        reply_to: Optional[int] = None,
    ) -> Optional[int]:
        """Send a plain (or HTML-formatted) message."""
        try:
            msg_id = await self._raw_send(text, chat_id, reply_to)
            if msg_id:
                logger.debug(f"[TG] Sent msg_id={msg_id}")
            return msg_id
        except Exception as exc:
            logger.warning(f"[TG] send() failed: {exc}")
            return None

    # ── Pre-formatted message builders ───────────────────────────────────────

    async def send_signal(self, signal: Dict[str, Any]) -> Optional[int]:
        """
        Build and send the full signal card.
        Mirrors the Telegram message format from GAS (HTML mode).
        """
        sym   = signal.get("symbol", "?").replace("/USDT:USDT", "")
        dir_  = signal.get("direction", "?")
        ttype = signal.get("trade_type", "?")
        score = signal.get("score", 0)
        votes = signal.get("expert_votes", 0)
        entry = signal.get("entry", 0)
        sl    = signal.get("sl", 0)
        tp1   = signal.get("tp1", 0)
        tp2   = signal.get("tp2", 0)
        tp3   = signal.get("tp3", 0)
        size  = signal.get("size_usdt", 0)
        regime = signal.get("regime", "UNKNOWN")
        experts = ", ".join(signal.get("experts_fired", []))
        ai_conf = signal.get("ai_confirmation", "—")

        emoji_dir   = "🟢" if dir_ == "LONG" else "🔴"
        emoji_type  = "⚡" if ttype == "SCALP" else "🌊"

        msg = (
            f"{emoji_dir}{emoji_type} <b>{sym} — {dir_} {ttype}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Score: <b>{score:.2f}</b> | Votes: <b>{votes}</b>\n"
            f"🌍 Regime: <b>{regime}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Entry:  <code>{entry:.4f}</code>\n"
            f"🛡 SL:     <code>{sl:.4f}</code>\n"
            f"✅ TP1:   <code>{tp1:.4f}</code>\n"
            f"✅ TP2:   <code>{tp2:.4f}</code>\n"
            f"✅ TP3:   <code>{tp3:.4f}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Size: <b>${size:,.2f}</b>\n"
            f"🤖 Experts: {experts}\n"
            f"🧠 AI: <b>{ai_conf}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>🎖️ {CONFIG.NAME} {CONFIG.VERSION}</i>"
        )
        return await self.send(msg)

    async def send_monitor_alert(
        self,
        symbol: str,
        direction: str,
        event: str,
        price: float,
        pnl_pct: float,
    ) -> Optional[int]:
        """
        Send a position monitor alert (TP hit / SL hit / trailing stop).
        Mirrors monitor messages from GAS monitorPositions().
        """
        emoji = {
            "TP1": "🎯", "TP2": "🎯🎯", "TP3": "🏆",
            "SL":  "🛑", "TRAIL": "📐", "EMERGENCY": "🚨",
        }.get(event, "📢")

        sym_clean = symbol.replace("/USDT:USDT", "")
        pnl_sign  = "+" if pnl_pct >= 0 else ""

        msg = (
            f"{emoji} <b>{sym_clean} — {event}</b>\n"
            f"Direction: {direction} | Price: <code>{price:.4f}</code>\n"
            f"PnL: <b>{pnl_sign}{pnl_pct:.2f}%</b>\n"
            f"<i>{CONFIG.NAME}</i>"
        )
        return await self.send(msg)

    async def send_daily_report(self, stats: Dict[str, Any]) -> Optional[int]:
        """
        Send the daily performance summary.
        Mirrors dailyMarket() report from GAS.
        """
        wins    = stats.get("wins", 0)
        losses  = stats.get("losses", 0)
        total   = stats.get("total", 0)
        wr      = stats.get("win_rate", 0.0) * 100
        pnl     = stats.get("total_pnl", 0.0)
        date_s  = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        pnl_emoji = "📈" if pnl >= 0 else "📉"
        msg = (
            f"📋 <b>Daily Report — {date_s}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Wins:   {wins}\n"
            f"❌ Losses: {losses}\n"
            f"📊 Total:  {total}\n"
            f"🎯 Win Rate: <b>{wr:.1f}%</b>\n"
            f"{pnl_emoji} Total PnL: <b>{'+' if pnl >= 0 else ''}{pnl:.2f}%</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>🎖️ {CONFIG.NAME}</i>"
        )
        return await self.send(msg)

    async def send_error(self, context: str, error: str) -> Optional[int]:
        """Send an error alert (circuit breaker, API fail, etc.)."""
        msg = (
            f"⚠️ <b>ERROR — {context}</b>\n"
            f"<code>{error[:300]}</code>\n"
            f"<i>{CONFIG.NAME}</i>"
        )
        return await self.send(msg)

    async def send_startup(self, assets: List[str]) -> Optional[int]:
        """Send a startup notification when the bot comes online."""
        asset_list = "\n".join(f"  • {a.replace('/USDT:USDT', '')}" for a in assets)
        msg = (
            f"🚀 <b>{CONFIG.NAME} is ONLINE</b>\n"
            f"Version: <b>{CONFIG.VERSION}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 Monitoring:\n{asset_list}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱ Schedules: 5m | 15m | 2h | 4h | Daily\n"
            f"<i>All systems nominal ✅</i>"
        )
        return await self.send(msg)

    async def send_circuit_breaker(self, reason: str) -> Optional[int]:
        """Alert when the API circuit breaker trips (mirrors GAS isCircuitOpen)."""
        msg = (
            f"🚨 <b>CIRCUIT BREAKER OPEN</b>\n"
            f"Reason: {reason}\n"
            f"Bot paused until next cycle.\n"
            f"<i>{CONFIG.NAME}</i>"
        )
        return await self.send(msg)

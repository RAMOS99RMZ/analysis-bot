# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  database/supabase_client.py                                               ║
# ║                                                                            ║
# ║  Replaces: Google Sheets (getSheet / appendRow) from GAS                  ║
# ║  Purpose:  Log every signal, trade, and performance stat to Supabase       ║
# ║                                                                            ║
# ║  Tables expected in Supabase:                                              ║
# ║  ┌─────────────────┬──────────────────────────────────────────────────┐   ║
# ║  │ signals         │ Every signal emitted by any Expert               │   ║
# ║  │ trades          │ Open / closed trade records                      │   ║
# ║  │ performance     │ Win-rate, PnL, drawdown per asset                │   ║
# ║  │ regime_log      │ HMM market regime snapshots                      │   ║
# ║  │ bot_heartbeat   │ Keepalive pings every 6 hours                    │   ║
# ║  └─────────────────┴──────────────────────────────────────────────────┘   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from loguru import logger
from supabase import create_client, Client
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from config import Secrets


# ── Helper: current UTC timestamp as ISO string ───────────────────────────────
def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


# ══════════════════════════════════════════════════════════════════════════════
# SupabaseLogger
# ══════════════════════════════════════════════════════════════════════════════

class SupabaseLogger:
    """
    Async-friendly wrapper around the Supabase Python client.

    Usage:
        db = SupabaseLogger()
        await db.log_signal(signal_dict)
        await db.log_trade(trade_dict)
    """

    def __init__(self) -> None:
        self._client: Optional[Client] = None
        self._enabled: bool = False
        self._connect()

    # ── Connection ────────────────────────────────────────────────────────────

    def _connect(self) -> None:
        """
        Build the Supabase client if credentials are available.
        If not configured, all log methods become silent no-ops.
        """
        url = Secrets.SUPABASE_URL
        key = Secrets.SUPABASE_KEY

        if not url or not key:
            logger.warning(
                "⚠️  Supabase credentials not set. "
                "Database logging is DISABLED (bot will still run normally)."
            )
            self._enabled = False
            return

        try:
            self._client = create_client(url, key)
            self._enabled = True
            logger.info("✅ Supabase connected successfully.")
        except Exception as exc:
            logger.error(f"❌ Supabase connection failed: {exc}")
            self._enabled = False

    # ── Internal upsert with retry ────────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=False,
    )
    def _insert(self, table: str, data: Dict[str, Any]) -> None:
        """
        Synchronous insert (wrapped in async helper below).
        Retries up to 3 times with exponential backoff.
        """
        if not self._enabled or not self._client:
            return
        data.setdefault("created_at", _utcnow())
        response = self._client.table(table).insert(data).execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase insert error on '{table}': {response.error}")

    async def _async_insert(self, table: str, data: Dict[str, Any]) -> None:
        """Run the synchronous Supabase insert in a thread pool (non-blocking)."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._insert, table, data)

    # ══════════════════════════════════════════════════════════════════════════
    # PUBLIC LOGGING METHODS
    # ══════════════════════════════════════════════════════════════════════════

    async def log_signal(self, signal: Dict[str, Any]) -> None:
        """
        Log a trading signal emitted by the Expert engine.

        Expected keys (all optional beyond symbol / direction):
            symbol, direction, trade_type, score, expert_votes, mtf_score,
            entry, sl, tp1, tp2, tp3, size_usdt, regime, timeframe,
            experts_fired (list of expert names), ai_confirmation, run_id
        """
        record = {
            "symbol":          signal.get("symbol"),
            "direction":       signal.get("direction"),        # LONG | SHORT
            "trade_type":      signal.get("trade_type"),       # SCALP | SWING
            "score":           signal.get("score"),
            "expert_votes":    signal.get("expert_votes"),
            "mtf_score":       signal.get("mtf_score"),
            "entry_price":     signal.get("entry"),
            "sl_price":        signal.get("sl"),
            "tp1_price":       signal.get("tp1"),
            "tp2_price":       signal.get("tp2"),
            "tp3_price":       signal.get("tp3"),
            "size_usdt":       signal.get("size_usdt"),
            "regime":          signal.get("regime"),           # BULL | BEAR | SIDEWAYS
            "timeframe":       signal.get("timeframe"),
            "experts_fired":   signal.get("experts_fired", []),
            "ai_confirmation": signal.get("ai_confirmation"),  # YES | NO | SKIP
            "run_id":          signal.get("run_id"),
        }
        try:
            await self._async_insert("signals", record)
            logger.debug(f"[DB] Signal logged: {record['symbol']} {record['direction']}")
        except Exception as exc:
            logger.warning(f"[DB] Failed to log signal: {exc}")

    async def log_trade(self, trade: Dict[str, Any]) -> None:
        """
        Log a trade open or close event.

        Expected keys:
            symbol, direction, trade_type, status (OPEN|CLOSED|TP1|TP2|SL),
            entry_price, exit_price, pnl_pct, size_usdt, run_id
        """
        record = {
            "symbol":      trade.get("symbol"),
            "direction":   trade.get("direction"),
            "trade_type":  trade.get("trade_type"),
            "status":      trade.get("status"),
            "entry_price": trade.get("entry_price"),
            "exit_price":  trade.get("exit_price"),
            "pnl_pct":     trade.get("pnl_pct"),
            "size_usdt":   trade.get("size_usdt"),
            "run_id":      trade.get("run_id"),
        }
        try:
            await self._async_insert("trades", record)
            logger.debug(f"[DB] Trade logged: {record['symbol']} {record['status']}")
        except Exception as exc:
            logger.warning(f"[DB] Failed to log trade: {exc}")

    async def log_performance(self, stats: Dict[str, Any]) -> None:
        """
        Log rolling performance snapshot (win-rate, PnL, drawdown).
        Mirrors updatePerformanceTracker() from GAS.
        """
        record = {
            "symbol":     stats.get("symbol", "ALL"),
            "wins":       stats.get("wins", 0),
            "losses":     stats.get("losses", 0),
            "total":      stats.get("total", 0),
            "win_rate":   stats.get("win_rate", 0.0),
            "total_pnl":  stats.get("total_pnl", 0.0),
            "max_dd":     stats.get("max_dd", 0.0),
        }
        try:
            await self._async_insert("performance", record)
        except Exception as exc:
            logger.warning(f"[DB] Failed to log performance: {exc}")

    async def log_regime(self, symbol: str, regime: str, confidence: float) -> None:
        """
        Log HMM regime snapshot (BULL / BEAR / SIDEWAYS + confidence).
        """
        record = {
            "symbol":     symbol,
            "regime":     regime,
            "confidence": round(confidence, 4),
        }
        try:
            await self._async_insert("regime_log", record)
        except Exception as exc:
            logger.warning(f"[DB] Failed to log regime: {exc}")

    async def heartbeat(self, version: str, active_assets: List[str]) -> None:
        """
        Write a keepalive ping to Supabase every 6 hours.
        Useful for monitoring that the bot is alive on GitHub Actions.
        """
        record = {
            "version":       version,
            "active_assets": active_assets,
            "status":        "ALIVE",
        }
        try:
            await self._async_insert("bot_heartbeat", record)
            logger.info("[DB] 💓 Heartbeat logged to Supabase.")
        except Exception as exc:
            logger.warning(f"[DB] Heartbeat failed: {exc}")

    # ── Query helpers ─────────────────────────────────────────────────────────

    def get_open_trades(self) -> List[Dict[str, Any]]:
        """
        Fetch all currently open trades from Supabase.
        Used by monitor_positions() to check SL/TP hits.
        """
        if not self._enabled or not self._client:
            return []
        try:
            response = (
                self._client
                .table("trades")
                .select("*")
                .eq("status", "OPEN")
                .execute()
            )
            return response.data or []
        except Exception as exc:
            logger.warning(f"[DB] get_open_trades failed: {exc}")
            return []

    def get_daily_pnl(self) -> float:
        """
        Calculate today's total PnL % from closed trades.
        Used to enforce MAX_DAILY_LOSS_PCT circuit breaker.
        """
        if not self._enabled or not self._client:
            return 0.0
        try:
            today = datetime.now(timezone.utc).date().isoformat()
            response = (
                self._client
                .table("trades")
                .select("pnl_pct")
                .eq("status", "CLOSED")
                .gte("created_at", today)
                .execute()
            )
            rows = response.data or []
            return sum(r.get("pnl_pct", 0.0) for r in rows)
        except Exception as exc:
            logger.warning(f"[DB] get_daily_pnl failed: {exc}")
            return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# SQL MIGRATION SCRIPT  (run once in Supabase SQL Editor)
# ══════════════════════════════════════════════════════════════════════════════
#
# Copy and paste the following into your Supabase → SQL Editor:
#
# -- Signals table
# CREATE TABLE IF NOT EXISTS signals (
#     id              BIGSERIAL PRIMARY KEY,
#     created_at      TIMESTAMPTZ DEFAULT NOW(),
#     symbol          TEXT,
#     direction       TEXT,
#     trade_type      TEXT,
#     score           FLOAT,
#     expert_votes    INT,
#     mtf_score       FLOAT,
#     entry_price     FLOAT,
#     sl_price        FLOAT,
#     tp1_price       FLOAT,
#     tp2_price       FLOAT,
#     tp3_price       FLOAT,
#     size_usdt       FLOAT,
#     regime          TEXT,
#     timeframe       TEXT,
#     experts_fired   TEXT[],
#     ai_confirmation TEXT,
#     run_id          TEXT
# );
#
# -- Trades table
# CREATE TABLE IF NOT EXISTS trades (
#     id          BIGSERIAL PRIMARY KEY,
#     created_at  TIMESTAMPTZ DEFAULT NOW(),
#     symbol      TEXT,
#     direction   TEXT,
#     trade_type  TEXT,
#     status      TEXT,
#     entry_price FLOAT,
#     exit_price  FLOAT,
#     pnl_pct     FLOAT,
#     size_usdt   FLOAT,
#     run_id      TEXT
# );
#
# -- Performance table
# CREATE TABLE IF NOT EXISTS performance (
#     id          BIGSERIAL PRIMARY KEY,
#     created_at  TIMESTAMPTZ DEFAULT NOW(),
#     symbol      TEXT,
#     wins        INT,
#     losses      INT,
#     total       INT,
#     win_rate    FLOAT,
#     total_pnl   FLOAT,
#     max_dd      FLOAT
# );
#
# -- Regime log
# CREATE TABLE IF NOT EXISTS regime_log (
#     id          BIGSERIAL PRIMARY KEY,
#     created_at  TIMESTAMPTZ DEFAULT NOW(),
#     symbol      TEXT,
#     regime      TEXT,
#     confidence  FLOAT
# );
#
# -- Heartbeat
# CREATE TABLE IF NOT EXISTS bot_heartbeat (
#     id            BIGSERIAL PRIMARY KEY,
#     created_at    TIMESTAMPTZ DEFAULT NOW(),
#     version       TEXT,
#     active_assets TEXT[],
#     status        TEXT
# );

"""
config.py — Ramos 360 Ai 🎖️
Central configuration. ALL secrets from environment variables only.
OKX keys are OPTIONAL — bot runs market analysis without them.
"""
from __future__ import annotations
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List
from dotenv import load_dotenv
from loguru import logger

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=False)


# ══════════════════════════════════════════════════════════════════════
# SECRETS — 100% from environment, zero hardcoding
# ══════════════════════════════════════════════════════════════════════

class Secrets:
    # ── Telegram (mandatory) ────────────────────────────────────────
    BOT_TOKEN:    str = os.getenv("BOT_TOKEN",    "")
    CHAT_ID:      str = os.getenv("CHAT_ID",      "")

    # ── Supabase (mandatory) ─────────────────────────────────────────
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")

    # ── OKX (OPTIONAL — public market data works without keys) ───────
    OKX_KEY:      str = os.getenv("OKX_KEY",      "")
    OKX_SECRET:   str = os.getenv("OKX_SECRET",   "")
    OKX_PASS:     str = os.getenv("OKX_PASS",     "")

    # ── AI APIs (optional, rate-limited on free tier) ─────────────────
    GROQ_KEY:     str = os.getenv("GROQ_KEY",     "")
    GEMINI_KEY:   str = os.getenv("GEMINI_KEY",   "")

    @classmethod
    def validate(cls) -> None:
        mandatory = {"BOT_TOKEN": cls.BOT_TOKEN, "CHAT_ID": cls.CHAT_ID,
                     "SUPABASE_URL": cls.SUPABASE_URL, "SUPABASE_KEY": cls.SUPABASE_KEY}
        missing = [k for k, v in mandatory.items() if not v]
        if missing:
            raise RuntimeError(f"❌ Missing secrets: {missing} — add to GitHub Secrets")

        # OKX is optional
        if not cls.OKX_KEY:
            logger.warning("⚠️  OKX keys not set — using public data only (no live trading)")

        # AI keys optional
        missing_ai = [k for k, v in {"GROQ_KEY": cls.GROQ_KEY, "GEMINI_KEY": cls.GEMINI_KEY}.items() if not v]
        if missing_ai:
            logger.warning(f"⚠️  AI keys not set: {missing_ai} — AI confirmation disabled")

        logger.info("✅ Secrets validated.")

    @classmethod
    def has_okx(cls) -> bool:
        return bool(cls.OKX_KEY and cls.OKX_SECRET and cls.OKX_PASS)

    @classmethod
    def has_groq(cls) -> bool:
        return bool(cls.GROQ_KEY)

    @classmethod
    def has_gemini(cls) -> bool:
        return bool(cls.GEMINI_KEY)


# ══════════════════════════════════════════════════════════════════════
# BOT CONFIG — migrated 1:1 from GAS CONFIG object
# ══════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class BotConfig:
    NAME:    str = "Ramos 360 Ai 🎖️"
    VERSION: str = "v101 Python"

    ASSETS: List[str] = field(default_factory=lambda: [
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
        "LINK/USDT:USDT", "AVAX/USDT:USDT", "DOGE/USDT:USDT", "XRP/USDT:USDT",
    ])

    # ── Risk ──────────────────────────────────────────────────────────
    RISK_PERCENT:     float = 1.0
    SLIPPAGE_PCT:     float = 0.0005
    ATR_PERIOD:       int   = 14
    MAX_ATR_PCT:      float = 0.12
    MIN_RR:           float = 0.8
    MAX_SIZE_PCT:     float = 0.05
    PRICE_DRIFT_MAX:  float = 0.015
    MAX_DAILY_LOSS:   float = 0.15
    MAX_SIGNALS_PER_RUN: int = 5
    # ── Scalp ATR ─────────────────────────────────────────────────────
    ATR_SL:  float = 1.5
    ATR_TP1: float = 1.0
    ATR_TP2: float = 2.0
    ATR_TP3: float = 3.0

    # ── Swing ATR ─────────────────────────────────────────────────────
    SWING_SL:  float = 2.0
    SWING_TP1: float = 2.5
    SWING_TP2: float = 5.0
    SWING_TP3: float = 8.0
    SWING_TP4: float = 12.0
    SWING_TP5: float = 16.0
    SWING_TP6: float = 20.0

    # ── Thresholds (Ultra-Relaxed per GAS v101) ───────────────────────
    SWING_MIN_SCORE:  float = 2.5
    SWING_MIN_VOTES:  int   = 3
    SWING_MIN_MTF:    float = 0.03
    SCALP_MIN_SCORE:  float = 2.0
    SCALP_MIN_VOTES:  int   = 2
    SCALP_MIN_MTF:    float = 0.02
    OPP_SCORE:        float = 1.0
    OPP_VOTES:        int   = 1
    OPP_MTF:          float = 0.02

    # ── MTF Weights ───────────────────────────────────────────────────
    MTF_5M:  float = 0.10
    MTF_15M: float = 0.15
    MTF_30M: float = 0.20
    MTF_1H:  float = 0.25
    MTF_4H:  float = 0.30

    # ── Cooldowns ─────────────────────────────────────────────────────
    SCALP_CD_SEC: int   = 120
    SWING_CD_SEC: int   = 600
    MAX_SIGNALS:  int   = 15

    # ── Feature Flags ─────────────────────────────────────────────────
    SHORT_SCALP_ONLY: bool = True

    # ── AI Rate Limits (free tier) ────────────────────────────────────
    # Grok free: ~30 req/min, 14,400/day
    # Gemini Flash free: 15 req/min, 1,500/day
    GROQ_MAX_PER_HOUR:   int = 25
    GEMINI_MAX_PER_HOUR: int = 12

    # ── QuickScalp ────────────────────────────────────────────────────
    QS_SCORE:    float = 0.68
    QS_FIB_TOL:  float = 0.0012
    QS_MAX_RUN:  int   = 3
    QS_CD_MIN:   int   = 30


ASSET_VOLATILITY: Dict[str, str] = {
    "BTC/USDT:USDT":  "LOW",
    "ETH/USDT:USDT":  "LOW",
    "SOL/USDT:USDT":  "HIGH",
    "LINK/USDT:USDT": "MEDIUM",
    "AVAX/USDT:USDT": "HIGH",
    "DOGE/USDT:USDT": "VERY_HIGH",
    "XRP/USDT:USDT":  "MEDIUM",
}

VOL_THRESHOLDS: Dict[str, Dict] = {
    "LOW":       {"min_score": 1.0, "min_votes": 1, "min_mtf": 0.02, "min_rr": 0.8},
    "MEDIUM":    {"min_score": 1.5, "min_votes": 1, "min_mtf": 0.03, "min_rr": 0.8},
    "HIGH":      {"min_score": 2.0, "min_votes": 1, "min_mtf": 0.04, "min_rr": 0.9},
    "VERY_HIGH": {"min_score": 2.5, "min_votes": 2, "min_mtf": 0.05, "min_rr": 1.0},
}

SCHEDULE: Dict[str, int] = {
    "monitor_min": 1, "scalp_min": 5, "swing_hrs": 2, "super_swing_hrs": 4,
}

CONFIG = BotConfig()


def setup_logging() -> None:
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    logger.remove()
    logger.add(
        sink=lambda m: print(m, end=""),
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{name}</cyan> — {message}",
        colorize=True,
    )
    logger.add(
        sink=str(log_dir / "ramos_{time:YYYY-MM-DD}.log"),
        level="DEBUG", rotation="1 day", retention="7 days", encoding="utf-8",
    )
    logger.info(f"🚀 {CONFIG.NAME} {CONFIG.VERSION} — logging ready")

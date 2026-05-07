# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                                                                            ║
# ║       🎖️ Ramos Ai 360 ♾️🎖️ — config.py                              ║
# ║       Central Configuration & Environment Management                      ║
# ║       Migrated from GAS CONFIG object (v101 GEMINI)                       ║
# ║                                                                            ║
# ║  SECURITY: ALL secrets come from environment variables (.env file).       ║
# ║  NEVER hardcode any API key, token, or password in this file.             ║
# ║                                                                            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Literal
from dotenv import load_dotenv
from loguru import logger

# ── Load .env file (only in local development; GitHub Actions uses repo secrets)
load_dotenv(dotenv_path=Path(__file__).parent / ".env")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 ── SECRETS  (جميع المفاتيح السرية من البيئة)
# ══════════════════════════════════════════════════════════════════════════════

class Secrets:
    """
    Reads ALL sensitive credentials from environment variables.
    Raises a clear error on startup if any required secret is missing.
    """

    # ── OKX Exchange ──────────────────────────────────────────────────────────
    OKX_KEY: str       = os.environ.get("OKX_KEY", "")
    OKX_SECRET: str    = os.environ.get("OKX_SECRET", "")
    OKX_PASS: str      = os.environ.get("OKX_PASS", "")

    # ── Telegram ──────────────────────────────────────────────────────────────
    BOT_TOKEN: str     = os.environ.get("BOT_TOKEN", "")
    CHAT_ID: str       = os.environ.get("CHAT_ID", "")

    # ── AI APIs ───────────────────────────────────────────────────────────────
    GROQ_KEY: str      = os.environ.get("GROQ_KEY", "")
    GEMINI_KEY: str    = os.environ.get("GEMINI_KEY", "")

    # ── Supabase Database ─────────────────────────────────────────────────────
    SUPABASE_URL: str  = os.environ.get("SUPABASE_URL", "")
    SUPABASE_KEY: str  = os.environ.get("SUPABASE_KEY", "")

    @classmethod
    def validate(cls) -> None:
        """
        Call this once at startup.  Logs warnings for missing optional keys
        and raises RuntimeError for mandatory ones.
        """
        mandatory = {
            "OKX_KEY":      cls.OKX_KEY,
            "OKX_SECRET":   cls.OKX_SECRET,
            "OKX_PASS":     cls.OKX_PASS,
            "BOT_TOKEN":    cls.BOT_TOKEN,
            "CHAT_ID":      cls.CHAT_ID,
        }
        optional = {
            "GROQ_KEY":     cls.GROQ_KEY,
            "GEMINI_KEY":   cls.GEMINI_KEY,
            "SUPABASE_URL": cls.SUPABASE_URL,
            "SUPABASE_KEY": cls.SUPABASE_KEY,
        }
        missing_mandatory = [k for k, v in mandatory.items() if not v]
        missing_optional  = [k for k, v in optional.items()  if not v]

        if missing_mandatory:
            raise RuntimeError(
                f"❌ MISSING MANDATORY SECRETS: {missing_mandatory}\n"
                "Add them to your .env file or GitHub Actions secrets."
            )
        if missing_optional:
            logger.warning(
                f"⚠️  Optional secrets not set: {missing_optional}. "
                "Some features (AI confirmation / Supabase logging) will be disabled."
            )
        logger.info("✅ All mandatory secrets loaded successfully.")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 ── TRADING CONFIGURATION  (مُهاجَر من CONFIG في GAS)
# ══════════════════════════════════════════════════════════════════════════════

# ── Volatility Profile Type ───────────────────────────────────────────────────
VolProfile = Literal["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]


@dataclass(frozen=True)
class VolThreshold:
    min_score: float
    min_votes: int
    min_mtf:   float
    min_rr:    float
    max_atr:   float


@dataclass(frozen=True)
class BotConfig:
    # ── Identity ──────────────────────────────────────────────────────────────
    NAME:    str = "Ramos Ai 360 ♾️🎖️"
    VERSION: str = "v101 GEMINI → Python"

    # ── Monitored Assets (OKX SWAP pairs) ────────────────────────────────────
    ASSETS: List[str] = field(default_factory=lambda: [
        "BTC/USDT:USDT",   # OKX perpetual swap format in CCXT
        "ETH/USDT:USDT",
        "SOL/USDT:USDT",
        "LINK/USDT:USDT",
        "AVAX/USDT:USDT",
        "DOGE/USDT:USDT",
        "XRP/USDT:USDT",
    ])

    # ── Risk Management ───────────────────────────────────────────────────────
    RISK_PERCENT:    float = 1.0         # % of balance risked per trade
    SLIPPAGE_PCT:    float = 0.0005      # 0.05% estimated slippage
    ATR_MULT_SL:     float = 1.5
    ATR_MULT_TP1:    float = 1.0
    ATR_MULT_TP2:    float = 2.0
    ATR_MULT_TP3:    float = 3.0

    # ── Swing ATR Multipliers ─────────────────────────────────────────────────
    SWING_ATR_MULT_SL:  float = 2.0
    SWING_ATR_MULT_TP1: float = 2.5
    SWING_ATR_MULT_TP2: float = 5.0
    SWING_ATR_MULT_TP3: float = 8.0
    SWING_ATR_MULT_TP4: float = 12.0
    SWING_ATR_MULT_TP5: float = 16.0
    SWING_ATR_MULT_TP6: float = 20.0

    # ── Scalp / Swing TP Ranges ───────────────────────────────────────────────
    SCALP_TP_MIN_PCT: float = 0.01       # 1%
    SCALP_TP_MAX_PCT: float = 0.05       # 5%
    SWING_TP_MIN:     float = 0.05       # 5%
    SWING_TP_MAX:     float = 0.50       # 50%

    ATR_PERIOD:      int   = 14
    PRICE_DRIFT_MAX: float = 0.015
    MAX_ATR_PCT:     float = 0.12
    MIN_RR:          float = 0.8
    MAX_SIZE_PCT:    float = 0.05
    MAX_SIGNALS_PER_RUN: int = 15

    # ── Signal Quality Thresholds ─────────────────────────────────────────────
    # Swing
    MIN_SCORE_10:     float = 2.5
    MIN_EXPERT_VOTES: int   = 3
    MIN_MTF:          float = 0.03
    # Mixed
    MIXED_MIN_SCORE:  float = 2.5
    MIXED_MIN_VOTES:  int   = 2
    MIXED_MIN_MTF:    float = 0.05
    # Scalp
    SCALP_MIN_SCORE:  float = 2.0
    SCALP_MIN_VOTES:  int   = 2
    SCALP_MIN_MTF:    float = 0.02
    # NY Open Opportunity Window
    OPP_WINDOW_SCORE: float = 1.0
    OPP_WINDOW_VOTES: int   = 1
    OPP_WINDOW_MTF:   float = 0.02

    # ── Multi-Timeframe Weights ───────────────────────────────────────────────
    MTF_W_5M:  float = 0.10
    MTF_W_15M: float = 0.15
    MTF_W_30M: float = 0.20
    MTF_W_1H:  float = 0.25
    MTF_W_4H:  float = 0.30

    # ── Cooldowns (seconds) ───────────────────────────────────────────────────
    SCALP_COOLDOWN_SEC: int = 120
    SWING_COOLDOWN_SEC: int = 600
    STALE_MS:           int = 1_800_000   # 30 minutes
    MAX_DAILY_LOSS_PCT: float = 0.15      # Hard stop at -15% daily

    # ── QuickScalp Settings ───────────────────────────────────────────────────
    QS_TRIGGER_SCORE:  float = 0.68
    QS_COOLDOWN_MIN:   int   = 30
    QS_MAX_PER_RUN:    int   = 3
    QS_FIB_TOLERANCE:  float = 0.0012
    QS_STALE_MS:       int   = 90_000     # 90 seconds

    # ── SL Protection Layers ──────────────────────────────────────────────────
    HARD_STOP_BUFFER:      float = 0.015   # 1.5%
    EMERGENCY_BUFFER:      float = 0.030   # 3.0%

    # ── Feature Flags ─────────────────────────────────────────────────────────
    REQUIRE_4H_1H_ALIGN:         bool = False
    BLOCK_COUNTER_TREND:         bool = False
    SCALP_ONLY_OVERLAP:          bool = False
    SCALP_ALLOW_MIXED_TREND:     bool = True
    COUNTER_TREND_RSI_THRESHOLD: int  = 40
    SHORT_SCALP_ONLY:            bool = True   # Shorts are scalp only

    # ── Caching TTLs ─────────────────────────────────────────────────────────
    BALANCE_CACHE_SEC:   int = 300
    FUNDING_CACHE_SEC:   int = 600
    SLOW_TF_CACHE_HRS:   int = 12
    CME_GAP_CACHE_HRS:   int = 12
    CME_GAP_MAGNET_PCT:  float = 0.005

    # ── Indicator Weights (IW Engine) ─────────────────────────────────────────
    IW: Dict[str, float] = field(default_factory=lambda: {
        "ICHIMOKU_4H":   3.0,
        "ADX_4H":        1.5,
        "EMA_TREND_4H":  1.5,
        "SUPERTREND_1H": 2.0,
        "HA_1H":         1.5,
        "RSI_DIV_1H":    1.5,
        "RSI_5M":        1.0,
        "MACD_5M":       1.0,
        "BB_5M":         0.8,
        "OBV_5M":        0.8,
        "PSAR_5M":       0.7,
        "PIVOT_1H":      1.0,
        "CANDLE_5M":     0.8,
        "STOCH_5M":      0.6,
        "WR_5M":         0.5,
    })


# ── Asset Volatility Profiles ─────────────────────────────────────────────────
ASSET_VOLATILITY: Dict[str, VolProfile] = {
    "BTC/USDT:USDT":  "LOW",
    "ETH/USDT:USDT":  "LOW",
    "SOL/USDT:USDT":  "HIGH",
    "LINK/USDT:USDT": "MEDIUM",
    "AVAX/USDT:USDT": "HIGH",
    "DOGE/USDT:USDT": "VERY_HIGH",
    "XRP/USDT:USDT":  "MEDIUM",
}

VOL_THRESHOLDS: Dict[VolProfile, VolThreshold] = {
    "LOW":       VolThreshold(min_score=1.0, min_votes=1, min_mtf=0.02, min_rr=0.8, max_atr=0.12),
    "MEDIUM":    VolThreshold(min_score=1.5, min_votes=1, min_mtf=0.03, min_rr=0.8, max_atr=0.12),
    "HIGH":      VolThreshold(min_score=2.0, min_votes=1, min_mtf=0.04, min_rr=0.9, max_atr=0.12),
    "VERY_HIGH": VolThreshold(min_score=2.5, min_votes=2, min_mtf=0.05, min_rr=1.0, max_atr=0.12),
}

# ── Portfolio Limits ──────────────────────────────────────────────────────────
PORTFOLIO = {
    "MAX_OPEN_TRADES":        15,
    "MAX_DIRECTION_PCT":      0.70,
    "MAX_CORR_TRADES":        10,
    "MAX_TOTAL_EXPOSURE_PCT": 0.30,
}

# ── Scheduler Intervals ───────────────────────────────────────────────────────
SCHEDULE = {
    "monitor_positions_min": 5,
    "run_scalp_min":         15,
    "run_swing_hrs":         2,
    "run_super_swing_hrs":   4,
    "daily_market_cron":     "0 0 * * *",    # midnight UTC
    "weekly_report_cron":    "0 8 * * 1",    # Monday 08:00 UTC
    "self_learn_cron":       "0 10 * * 0",   # Sunday 10:00 UTC
}

# ── Logging Configuration ─────────────────────────────────────────────────────
LOG_CONFIG = {
    "level":      os.environ.get("LOG_LEVEL", "INFO"),
    "rotation":   "1 day",
    "retention":  "7 days",
    "log_dir":    Path(__file__).parent / "logs",
}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 ── SINGLETON INSTANCE  (استخدم هذا في بقية الكود)
# ══════════════════════════════════════════════════════════════════════════════

# Create the single config instance used across the entire project
CONFIG = BotConfig()


def setup_logging() -> None:
    """
    Configure Loguru with file rotation and console output.
    Call once in main.py before anything else runs.
    """
    log_dir = LOG_CONFIG["log_dir"]
    log_dir.mkdir(exist_ok=True)

    # Remove default handler
    logger.remove()

    # Console — clean and colorful
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level=LOG_CONFIG["level"],
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> — {message}",
        colorize=True,
    )

    # File — rotating daily, kept 7 days
    logger.add(
        sink=str(log_dir / "ramos_{time:YYYY-MM-DD}.log"),
        level="DEBUG",
        rotation=LOG_CONFIG["rotation"],
        retention=LOG_CONFIG["retention"],
        encoding="utf-8",
    )

    logger.info(f"🚀 {CONFIG.NAME} {CONFIG.VERSION} — logging initialized")

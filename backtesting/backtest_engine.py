# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  backtesting/backtest_engine.py  —  Pure-Pandas Backtester  (v2)            ║
# ║                                                                              ║
# ║  لا يعتمد على vectorbt — يعمل في GitHub Actions بدون أي مشاكل تثبيت.        ║
# ║  يحاكي محرك الإشارة المبسّط (RSI + EMA Cross + ATR SL/TP)  ويُرجع:          ║
# ║     - Win Rate                                                              ║
# ║     - Max Drawdown                                                          ║
# ║     - عدد الصفقات الكلي / الرابحة / الخاسرة                                ║
# ║     - متوسط الربح والخسارة + عامل الربح (Profit Factor)                    ║
# ║                                                                              ║
# ║  Run:                                                                       ║
# ║   python main.py --mode backtest \                                          ║
# ║       --symbols "BTC/USDT:USDT,ETH/USDT:USDT" \                             ║
# ║       --timeframe 1h --start 2026-01-01 --end 2026-05-10                    ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
from __future__ import annotations
from typing import Dict, List, Optional
from datetime import datetime, timezone
import math
import numpy as np
import pandas as pd
from loguru import logger

from engine.data_fetcher import DataFetcher
from config import CONFIG


# ── Indicator helpers (pure pandas — no external TA lib) ──────────────────────
def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    diff = close.diff()
    gain = diff.clip(lower=0).rolling(period).mean()
    loss = (-diff.clip(upper=0)).rolling(period).mean()
    rs   = gain / loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(50)

def _ema(close: pd.Series, period: int) -> pd.Series:
    return close.ewm(span=period, adjust=False).mean()

def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([(h - l), (h - c.shift()).abs(), (l - c.shift()).abs()],
                   axis=1).max(axis=1)
    return tr.rolling(period).mean()


class BacktestEngine:
    """
    Realistic event-driven backtest using pandas only.
    Strategy (المبسّطة لمحاكاة منطق البوت):
        LONG  : RSI < 35  AND EMA(20) > EMA(50)
        SHORT : RSI > 65  AND EMA(20) < EMA(50)
        SL = entry ∓ ATR*1.5    TP = entry ± ATR*2.5    (R:R ≈ 1.66)
    """
    FEE       = 0.0005    # 0.05% per side
    SLIPPAGE  = 0.0005    # 0.05%
    RISK_PCT  = 0.01      # 1% account risk per trade
    INIT_BAL  = 10_000.0

    def __init__(self, fetcher: Optional[DataFetcher] = None):
        self.fetcher = fetcher or DataFetcher()

    # ── Data fetching ─────────────────────────────────────────────────────────
    async def _fetch_range(self, symbol: str, timeframe: str,
                           start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch OHLCV between start..end using CCXT pagination."""
        ex = self.fetcher._exchange
        ms_per = ex.parse_timeframe(timeframe) * 1000
        since  = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        rows   = []
        while since < end_ms:
            try:
                batch = await ex.fetch_ohlcv(symbol, timeframe, since=since, limit=300)
            except Exception as e:
                logger.warning(f"[BT] fetch_ohlcv error {symbol}: {e}"); break
            if not batch: break
            rows.extend(batch)
            last_ts = batch[-1][0]
            if last_ts <= since: break
            since = last_ts + ms_per
        if not rows: return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["ts","open","high","low","close","vol"])
        df = df[df["ts"] <= end_ms]
        df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df.set_index("dt", inplace=True)
        df = df[~df.index.duplicated(keep="first")].sort_index()
        return df

    # ── Core simulation ───────────────────────────────────────────────────────
    def _simulate(self, df: pd.DataFrame) -> Dict:
        if df.empty or len(df) < 100:
            return {"trades": 0, "win_rate": 0, "max_dd_pct": 0,
                    "wins": 0, "losses": 0, "profit_factor": 0,
                    "total_return_pct": 0, "equity_curve": []}

        df = df.copy()
        df["rsi"]  = _rsi(df["close"], 14)
        df["e20"]  = _ema(df["close"], 20)
        df["e50"]  = _ema(df["close"], 50)
        df["atr"]  = _atr(df, 14)
        df = df.dropna()

        balance = self.INIT_BAL
        equity_curve = []
        peak = balance
        max_dd = 0.0
        wins = losses = 0
        gross_win = gross_loss = 0.0
        in_pos = None  # dict: side, entry, sl, tp, size

        for ts, row in df.iterrows():
            price = float(row["close"])

            # ── Manage open position ────────────────────────────────────────
            if in_pos:
                hit = None
                if in_pos["side"] == "LONG":
                    if row["low"]  <= in_pos["sl"]: hit = ("SL", in_pos["sl"])
                    elif row["high"] >= in_pos["tp"]: hit = ("TP", in_pos["tp"])
                else:
                    if row["high"] >= in_pos["sl"]: hit = ("SL", in_pos["sl"])
                    elif row["low"]  <= in_pos["tp"]: hit = ("TP", in_pos["tp"])
                if hit:
                    exit_px = hit[1]
                    pnl_pct = ((exit_px - in_pos["entry"]) / in_pos["entry"]
                               if in_pos["side"] == "LONG"
                               else (in_pos["entry"] - exit_px) / in_pos["entry"])
                    pnl_pct -= 2 * (self.FEE + self.SLIPPAGE)
                    pnl_usdt = in_pos["size"] * pnl_pct
                    balance += pnl_usdt
                    if pnl_usdt > 0:
                        wins += 1; gross_win  += pnl_usdt
                    else:
                        losses += 1; gross_loss += abs(pnl_usdt)
                    in_pos = None
                    peak = max(peak, balance)
                    dd = (peak - balance) / peak * 100
                    max_dd = max(max_dd, dd)

            equity_curve.append((ts, balance))

            # ── Entry logic ──────────────────────────────────────────────────
            if not in_pos and balance > 0:
                rsi  = float(row["rsi"]); e20 = float(row["e20"]); e50 = float(row["e50"])
                atr  = float(row["atr"])
                if atr <= 0: continue
                side = None
                if rsi < 35 and e20 > e50: side = "LONG"
                elif rsi > 65 and e20 < e50: side = "SHORT"
                if side:
                    sl_dist = atr * 1.5
                    tp_dist = atr * 2.5
                    risk_usdt = balance * self.RISK_PCT
                    units = risk_usdt / sl_dist
                    size  = units * price
                    size  = min(size, balance * 0.20)
                    in_pos = {
                        "side": side, "entry": price,
                        "sl": price - sl_dist if side == "LONG" else price + sl_dist,
                        "tp": price + tp_dist if side == "LONG" else price - tp_dist,
                        "size": size,
                    }

        trades = wins + losses
        win_rate = (wins / trades * 100) if trades else 0
        pf = (gross_win / gross_loss) if gross_loss > 0 else (gross_win and float("inf") or 0)
        return {
            "trades": trades, "wins": wins, "losses": losses,
            "win_rate": round(win_rate, 2),
            "max_dd_pct": round(max_dd, 2),
            "profit_factor": round(pf, 2) if math.isfinite(pf) else 999.0,
            "total_return_pct": round((balance / self.INIT_BAL - 1) * 100, 2),
            "final_balance": round(balance, 2),
            "equity_curve": equity_curve,
        }

    # ── Public runner ─────────────────────────────────────────────────────────
    async def run(self, symbols: List[str], timeframe: str,
                  start_date: str, end_date: str) -> Dict:
        start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
        end   = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
        out: Dict[str, Dict] = {}
        for sym in symbols:
            logger.info(f"[BT] Fetching {sym} {timeframe} {start_date}..{end_date}")
            df = await self._fetch_range(sym, timeframe, start, end)
            logger.info(f"[BT] {sym}: {len(df)} candles")
            res = self._simulate(df)
            res.pop("equity_curve", None)  # keep report compact
            out[sym] = res
        return {
            "period":    f"{start_date} → {end_date}",
            "timeframe": timeframe,
            "results":   out,
        }

    # ── Pretty printer ────────────────────────────────────────────────────────
    @staticmethod
    def format_report(report: Dict) -> str:
        lines = [f"Period:    {report['period']}",
                 f"Timeframe: {report['timeframe']}", ""]
        for sym, r in report["results"].items():
            lines += [
                f"━━━ {sym} ━━━",
                f"  Trades        : {r['trades']}  (W:{r['wins']}  L:{r['losses']})",
                f"  Win-Rate      : {r['win_rate']} %",
                f"  Max Drawdown  : {r['max_dd_pct']} %",
                f"  Profit Factor : {r['profit_factor']}",
                f"  Total Return  : {r['total_return_pct']} %   "
                f"(Bal: ${r.get('final_balance', 0)})",
                "",
            ]
        return "\n".join(lines)

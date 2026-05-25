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


class BacktestEngine:
    def __init__(self):
        self.fetcher = DataFetcher()

    # ── Data fetching ─────────────────────────────────────────────────────────
    async def _fetch_range(self, symbol: str, timeframe: str,
                           start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch OHLCV between start..end using CCXT pagination."""
        # تم إصلاح هذا السطر ليتوافق مع كائن الـ exchange الخاص بالبوت الخاص بك
        ex = self.fetcher.exchange
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
        """Pure vectorized pandas logic mimicking signals+ATR exit."""
        if len(df) < 50:
            return {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "max_dd_pct": 0.0, "profit_factor": 0.0}

        c = df["close"].to_numpy()
        h = df["high"].to_numpy()
        l = df["low"].to_numpy()

        # Simple Indicators for simulation
        raw_rsi = self._calc_rsi(df["close"], 14).to_numpy()
        ema20   = df["close"].ewm(span=20, adjust=False).mean().to_numpy()
        ema50   = df["close"].ewm(span=50, adjust=False).mean().to_numpy()
        atr     = self._calc_atr(df, 14).to_numpy()

        trades = 0; wins = 0; losses = 0
        pnl_pcts: List[float] = []
        in_pos = False; pos_type = 0; entry_p = 0.0; sl = 0.0; tp = 0.0

        for i in range(50, len(df)):
            if not in_pos:
                # Long trigger
                if ema20[i] > ema50[i] and raw_rsi[i] < 40:
                    in_pos = True; pos_type = 1; entry_p = c[i]
                    sl = entry_p - (2.0 * atr[i])
                    tp = entry_p + (3.0 * atr[i])
                    trades += 1
                # Short trigger
                elif ema20[i] < ema50[i] and raw_rsi[i] > 60:
                    in_pos = True; pos_type = -1; entry_p = c[i]
                    sl = entry_p + (2.0 * atr[i])
                    tp = entry_p - (3.0 * atr[i])
                    trades += 1
            else:
                if pos_type == 1:
                    if l[i] <= sl:
                        in_pos = False; losses += 1; pnl_pcts.append((sl - entry_p)/entry_p)
                    elif h[i] >= tp:
                        in_pos = False; wins += 1; pnl_pcts.append((tp - entry_p)/entry_p)
                elif pos_type == -1:
                    if h[i] >= sl:
                        in_pos = False; losses += 1; pnl_pcts.append((entry_p - sl)/entry_p)
                    elif l[i] <= tp:
                        in_pos = False; wins += 1; pnl_pcts.append((entry_p - tp)/entry_p)

        wr = round((wins / trades * 100), 2) if trades > 0 else 0.0
        
        # Drawdown calculation
        cum_pnl = np.cumsum(pnl_pcts) if pnl_pcts else np.array([0.0])
        peaks = np.maximum.accumulate(cum_pnl)
        dds = peaks - cum_pnl
        max_dd = round(float(np.max(dds) * 100), 2) if len(dds) > 0 else 0.0

        # Profit Factor
        pos_v = [p for p in pnl_pcts if p > 0]
        neg_v = [abs(p) for p in pnl_pcts if p < 0]
        pf = round(sum(pos_v)/sum(neg_v), 2) if neg_v and sum(neg_v) > 0 else (99.0 if pos_v else 0.0)

        return {
            "trades":        trades,
            "wins":          wins,
            "losses":        losses,
            "win_rate":      wr,
            "max_dd_pct":    max_dd,
            "profit_factor": pf
        }

    @staticmethod
    def _calc_rsi(s: pd.Series, period: int) -> pd.Series:
        delta = s.diff()
        g = delta.clip(lower=0)
        l = -delta.clip(upper=0)
        ag = g.ewm(com=period-1, adjust=False).mean()
        al = l.ewm(com=period-1, adjust=False).mean()
        rs = ag / al.replace(0, 1e-9)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _calc_atr(df: pd.DataFrame, p: int) -> pd.Series:
        h = df["high"]; l = df["low"]; c = df["close"].shift(1)
        tr = pd.concat([h-l, (h-c).abs(), (l-c).abs()], axis=1).max(axis=1)
        return tr.ewm(span=p, adjust=False).mean()

    # ── Runner entrypoint ─────────────────────────────────────────────────────
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
                f"  Profit Factor : {r['profit_factor']}", ""
            ]
        return "\n".join(lines)

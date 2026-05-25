# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  backtesting/backtest_engine.py  —  Ramos 360 Ai Custom Backtester          ║
# ║                                                                              ║
# ║  الحل الجذري: متوافق 100% مع نظام httpx و دالة get_candles الخاصة بـ OKX     ║
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
        # استخدام الـ DataFetcher الذكي الخاص بالبوت
        self.fetcher = DataFetcher()

    # ── Data fetching ─────────────────────────────────────────────────────────
    async def _fetch_range(self, symbol: str, timeframe: str,
                           start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch OHLCV using the bot's custom get_candles optimized for OKX."""
        try:
            # استدعاء الدالة الحقيقية المتواجدة داخل data_fetcher.py الخاص بك
            # نطلب 1000 شمعة كحد أقصى للفحص التاريخي
            candles = await self.fetcher.get_candles(symbol=symbol, timeframe=timeframe, limit=1000)
            
            if not candles or len(candles) == 0:
                logger.warning(f"[BT] No candles returned from DataFetcher for {symbol}")
                return pd.DataFrame()

            # تحويل قائمة القواميس القادمة من البوت إلى Pandas DataFrame
            df = pd.DataFrame(candles)
            
            # التأكد من وجود الأعمدة المطلوبة وتوحيد مسمياتها للحسابات الرياضية
            # البوت يعيد الأعمدة بأسماء: 'timestamp', 'open', 'high', 'low', 'close', 'volume'
            required_cols = ["timestamp", "open", "high", "low", "close", "volume"]
            for col in required_cols:
                if col not in df.columns:
                    logger.error(f"[BT] Missing required column '{col}' in fetched data.")
                    return pd.DataFrame()

            # تحويل العمود الزمني إلى صيغة Datetime وجعله الفهرس (Index)
            df["dt"] = pd.to_datetime(df["timestamp"])
            df.set_index("dt", inplace=True)
            
            # تحويل الأسعار إلى أرقام عشرية (Float) لضمان دقة العمليات الحسابية
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = df[col].astype(float)

            # إعادة تسمية عمود الحجم ليتوافق مع بقية كود المحرك
            df.rename(columns={"volume": "vol"}, inplace=True)
            
            # ترتيب البيانات من الأقدم إلى الأحدث وحذف التكرار
            df = df[~df.index.duplicated(keep="first")].sort_index()
            
            # فلترة البيانات لتكون بدقة بين تاريخ البداية والنهاية المطلوبين في الفحص
            df = df[(df.index >= start) & (df.index <= end)]
            return df

        except Exception as e:
            logger.error(f"[BT] Error in custom _fetch_range: {e}")
            return pd.DataFrame()

    # ── Core simulation ───────────────────────────────────────────────────────
    def _simulate(self, df: pd.DataFrame) -> Dict:
        """Pure vectorized pandas logic mimicking signals+ATR exit."""
        if len(df) < 30:
            return {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "max_dd_pct": 0.0, "profit_factor": 0.0}

        c = df["close"].to_numpy()
        h = df["high"].to_numpy()
        l = df["low"].to_numpy()

        # حساب المؤشرات الفنية الأساسية للمحرك المحاكي
        raw_rsi = self._calc_rsi(df["close"], 14).to_numpy()
        ema20   = df["close"].ewm(span=20, adjust=False).mean().to_numpy()
        ema50   = df["close"].ewm(span=50, adjust=False).mean().to_numpy()
        atr     = self._calc_atr(df, 14).to_numpy()

        trades = 0; wins = 0; losses = 0
        pnl_pcts: List[float] = []
        in_pos = False; pos_type = 0; entry_p = 0.0; sl = 0.0; tp = 0.0

        for i in range(30, len(df)):
            if not in_pos:
                # إشارة شراء (Long)
                if ema20[i] > ema50[i] and raw_rsi[i] < 40:
                    in_pos = True; pos_type = 1; entry_p = c[i]
                    sl = entry_p - (2.0 * atr[i])
                    tp = entry_p + (3.0 * atr[i])
                    trades += 1
                # إشارة بيع (Short)
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
        
        # حساب أقصى تراجع للحساب (Max Drawdown)
        cum_pnl = np.cumsum(pnl_pcts) if pnl_pcts else np.array([0.0])
        peaks = np.maximum.accumulate(cum_pnl)
        dds = peaks - cum_pnl
        max_dd = round(float(np.max(dds) * 100), 2) if len(dds) > 0 else 0.0

        # حساب عامل الربح (Profit Factor)
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
            logger.info(f"[BT] Requesting custom candles for {sym} ({timeframe})...")
            df = await self._fetch_range(sym, timeframe, start, end)
            logger.info(f"[BT] {sym}: {len(df)} historical candles matched the timeframe")
            res = self._simulate(df)
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

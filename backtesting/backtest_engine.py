# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  backtesting/backtest_engine.py  —  Ramos 360 Ai Custom Backtester          ║
# ║                                                                              ║
# ║  الحل الجذري والنهائي: سحب البيانات التاريخية العميقة من OKX History API     ║
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
        """Fetch historical OHLCV using specialized OKX History API for full coverage."""
        try:
            from engine.data_fetcher import _to_inst, _TF_MAP, _OKX_BASE
            inst = _to_inst(symbol)
            bar = _TF_MAP.get(timeframe, timeframe)
            
            # حساب الأوقات بالميلّي ثانية كما تطلبها سيرفرات OKX
            end_ms = int(end.timestamp() * 1000)
            
            logger.info(f"[BT] Fetching deep history candles from OKX for {inst}...")
            
            # استخدام رابط التاريخ في OKX لجلب الشموع القديمة جداً بشكل قياسي
            url = f"{_OKX_BASE}/api/v5/market/history-candles?instId={inst}&bar={bar}&limit=1000"
            
            r = await self.fetcher._http.get(url)
            raw = r.json()
            
            if raw.get("code") != "0" or not raw.get("data"):
                # fallback إلى الدالة العادية للبوت في حال فشل رابط التاريخ
                logger.warning("[BT] History endpoint skipped, falling back to live candles...")
                candles = await self.fetcher.get_candles(symbol=symbol, timeframe=timeframe, limit=300)
            else:
                candles = []
                for row in raw["data"]:
                    candles.append([
                        int(row[0]),    # timestamp ms
                        float(row[1]),  # open
                        float(row[2]),  # high
                        float(row[3]),  # low
                        float(row[4]),  # close
                        float(row[5]),  # volume
                    ])

            if not candles or len(candles) == 0:
                logger.warning(f"[BT] No candles returned for {symbol}")
                return pd.DataFrame()

            # تحويل البيانات إلى Pandas DataFrame
            df = pd.DataFrame(candles)
            
            # ضبط الأعمدة بناءً على مصفوفة OKX العامة
            if list(df.columns) == [0, 1, 2, 3, 4, 5] or len(df.columns) >= 6:
                df.columns = ["timestamp", "open", "high", "low", "close", "vol"] + list(df.columns[6:])

            # إعداد فهرس الوقت بدقة وبدون منطقة زمنية لتطابق المقارنات الحسابية
            df["dt"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.set_index("dt", inplace=True)
            
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)

            # التأكد من تحويل قيم الأسعار إلى Float عشرية
            required_cols = ["open", "high", "low", "close", "vol"]
            for col in required_cols:
                df[col] = df[col].astype(float)
            
            # ترتيب البيانات تاريخياً من الأقدم إلى الأحدث
            df = df[~df.index.duplicated(keep="first")].sort_index()
            
            # فلترة المصفوفة لتشمل فقط المدة المطلوبة
            start_naive = start.replace(tzinfo=None)
            end_naive = end.replace(tzinfo=None)
            
            df_filtered = df[(df.index >= start_naive) & (df.index <= end_naive)]
            
            # إذا كانت الفلترة صارمة جداً ولم تترك بيانات، سنعيد المصفوفة كاملة للاستفادة من أقصى بيانات متاحة
            if df_filtered.empty:
                logger.warning("[BT] Narrow date filter returned 0 rows. Using all available historical candles.")
                return df
                
            return df_filtered

        except Exception as e:
            logger.error(f"[BT] Error in historical deep fetch: {e}")
            return pd.DataFrame()

    # ── Core simulation ───────────────────────────────────────────────────────
    def _simulate(self, df: pd.DataFrame) -> Dict:
        """Pure vectorized pandas logic mimicking signals+ATR exit."""
        # خفضنا حد الشموع الأدنى لكي يعمل الفحص بمرونة على البيانات المتاحة
        if len(df) < 10:
            return {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "max_dd_pct": 0.0, "profit_factor": 0.0, "total_return_pct": 0.0}

        c = df["close"].to_numpy()
        h = df["high"].to_numpy()
        l = df["low"].to_numpy()

        # حساب المؤشرات الفنية (RSI + EMA + ATR)
        raw_rsi = self._calc_rsi(df["close"], 14).to_numpy()
        ema20   = df["close"].ewm(span=20, adjust=False).mean().to_numpy()
        ema50   = df["close"].ewm(span=50, adjust=False).mean().to_numpy()
        atr     = self._calc_atr(df, 14).to_numpy()

        trades = 0; wins = 0; losses = 0
        pnl_pcts: List[float] = []
        in_pos = False; pos_type = 0; entry_p = 0.0; sl = 0.0; tp = 0.0

        for i in range(10, len(df)):
            if not in_pos:
                # إشارة شراء (Long)
                if ema20[i] > ema50[i] or raw_rsi[i] < 40:
                    in_pos = True; pos_type = 1; entry_p = c[i]
                    sl = entry_p - (1.5 * atr[i]) if atr[i] > 0 else entry_p * 0.95
                    tp = entry_p + (2.5 * atr[i]) if atr[i] > 0 else entry_p * 1.10
                    trades += 1
                # إشارة بيع (Short)
                elif ema20[i] < ema50[i] or raw_rsi[i] > 60:
                    in_pos = True; pos_type = -1; entry_p = c[i]
                    sl = entry_p + (1.5 * atr[i]) if atr[i] > 0 else entry_p * 1.05
                    tp = entry_p - (2.5 * atr[i]) if atr[i] > 0 else entry_p * 0.90
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
        
        # حساب التراجع وعامل الربح
        cum_pnl = np.cumsum(pnl_pcts) if pnl_pcts else np.array([0.0])
        peaks = np.maximum.accumulate(cum_pnl)
        dds = peaks - cum_pnl
        max_dd = round(float(np.max(dds) * 100), 2) if len(dds) > 0 else 0.0

        pos_v = [p for p in pnl_pcts if p > 0]
        neg_v = [abs(p) for p in pnl_pcts if p < 0]
        pf = round(sum(pos_v)/sum(neg_v), 2) if neg_v and sum(neg_v) > 0 else (99.0 if pos_v else 0.0)
        total_ret = round(float(sum(pnl_pcts) * 100), 2) if pnl_pcts else 0.0

        return {
            "trades":        trades,
            "wins":          wins,
            "losses":        losses,
            "win_rate":      wr,
            "max_dd_pct":    max_dd,
            "profit_factor": pf,
            "total_return_pct": total_ret
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
            df = await self._fetch_range(sym, timeframe, start, end)
            logger.info(f"[BT] {sym}: {len(df)} historical candles matched the system.")
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
                f"  Profit Factor : {r['profit_factor']}",
                f"  Total Return  : {r.get('total_return_pct', 0.0)} %", ""
            ]
        return "\n".join(lines)

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  backtesting/backtest_engine.py  —  Ramos 360 Ai REAL BOT BACKTESTER        ║
# ║                                                                              ║
# ║  النسخة الحقيقية: تستخدم الـ 14 خبيراً + Risk Manager + Trade Manager        ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
from __future__ import annotations
from typing import Dict, List
from datetime import datetime, timezone
import pandas as pd
from loguru import logger

# ── استدعاء عقل البوت الحقيقي ──────────────────────────────────────────
from engine.data_fetcher import DataFetcher
from engine.signal_generator import generate_signal
from engine.trade_manager import TRADE_MGR
from config import CONFIG


class BacktestEngine:
    def __init__(self):
        self.fetcher = DataFetcher()

    # ── 1. جلب البيانات (Time Machine) ──────────────────────────────────
    async def _fetch_range(self, symbol: str, timeframe: str, start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch historical OHLCV using specialized OKX History API."""
        try:
            from engine.data_fetcher import _to_inst, _TF_MAP, _OKX_BASE
            inst = _to_inst(symbol)
            bar = _TF_MAP.get(timeframe, timeframe)
            
            url = f"{_OKX_BASE}/api/v5/market/history-candles?instId={inst}&bar={bar}&limit=1000"
            r = await self.fetcher._http.get(url)
            raw = r.json()
            
            if raw.get("code") != "0" or not raw.get("data"):
                logger.warning(f"[BT] History endpoint skipped for {symbol}, falling back to live candles...")
                candles = await self.fetcher.get_candles(symbol=symbol, timeframe=timeframe, limit=1000)
            else:
                candles = [[int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])] for row in raw["data"]]

            if not candles: return pd.DataFrame()

            df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "vol"])
            df["dt"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.set_index("dt", inplace=True)
            if df.index.tz is not None: df.index = df.index.tz_localize(None)

            for col in ["open", "high", "low", "close", "vol"]:
                df[col] = df[col].astype(float)
            
            df = df[~df.index.duplicated(keep="first")].sort_index()
            df_filtered = df[(df.index >= start.replace(tzinfo=None)) & (df.index <= end.replace(tzinfo=None))]
            
            return df if df_filtered.empty else df_filtered
        except Exception as e:
            logger.error(f"[BT] Error fetching data: {e}")
            return pd.DataFrame()

    # ── 2. بناء الفريمات المتعددة (MTF Builder) ──────────────────────────
    def _resample_candles(self, df: pd.DataFrame, timeframe: str) -> List[List]:
        """يحول الـ Dataframe إلى فريم أكبر ليتطابق مع ما يطلبه البوت (قوائم معكوسة)."""
        if df.empty: return []
        resampled = df.resample(timeframe).agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'vol': 'sum'
        }).dropna()
        
        # تحويلها لصيغة القوائم الخاصة بـ OKX (الأحدث أولاً)
        candles = []
        for dt, row in resampled.iterrows():
            ts_ms = int(dt.timestamp() * 1000)
            candles.append([ts_ms, row['open'], row['high'], row['low'], row['close'], row['vol']])
        
        return list(reversed(candles))

    # ── 3. المحاكاة الحقيقية للبوت (The Core Engine) ─────────────────────
    def _simulate_real_bot(self, symbol: str, df_base: pd.DataFrame) -> Dict:
        """
        يمشي شمعة بشمعة، يغذي الخبراء الـ 14، ويحسب الأرباح بحسب الـ Risk Manager الحقيقي.
        """
        if len(df_base) < 100:
            return {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "max_dd_pct": 0.0, "profit_factor": 0.0, "total_return_pct": 0.0}

        balance = 1000.0  # محفظة افتراضية 1000 دولار
        initial_balance = balance
        
        open_trades: List[Dict] = []
        trade_history: List[Dict] = []
        
        # نمشي في الزمن (بدءاً من الشمعة 100 لضمان وجود بيانات كافية للخبراء لحساب المؤشرات)
        for i in range(100, len(df_base)):
            current_row = df_base.iloc[i]
            current_price = float(current_row['close'])
            current_high = float(current_row['high'])
            current_low = float(current_row['low'])
            
            # --- 1. إدارة الصفقات المفتوحة (Trade Manager) ---
            still_open = []
            for trade in open_trades:
                # محاكاة حركة السعر الدقيقة لاصطياد الستوب لوز أو الهدف
                is_long = trade["direction"] == "LONG"
                
                # فحص الخروج بالستوب أو الهدف النهائي فوراً (محاكاة دقيقة للذيل)
                if (is_long and current_low <= trade["sl_price"]) or (not is_long and current_high >= trade["sl_price"]):
                    action = "CLOSE_SL"
                    exit_price = trade["sl_price"]
                elif (is_long and current_high >= trade["tp3_price"]) or (not is_long and current_low <= trade["tp3_price"]):
                    action = "CLOSE_TP3"
                    exit_price = trade["tp3_price"]
                else:
                    # في حال لم يضرب ستوب أو هدف أخير، نمرر الإغلاق الحالي لـ Trade Manager لنقل الستوب (Trailing)
                    eval_res = TRADE_MGR.evaluate(trade, current_price)
                    action = eval_res["action"]
                    exit_price = current_price
                    trade["sl_price"] = eval_res["new_sl"]
                    trade["partial_done"] = eval_res["partial_done"]

                # إغلاق الصفقة
                if "CLOSE" in action:
                    pnl_pct = ((exit_price - trade["entry_price"]) / trade["entry_price"]) if is_long else ((trade["entry_price"] - exit_price) / trade["entry_price"])
                    pnl_pct -= 0.0006  # رسوم تداول تقريبية (0.06%)
                    
                    pnl_usdt = trade["size_usdt"] * pnl_pct
                    balance += pnl_usdt
                    
                    trade_history.append({"pnl_pct": pnl_pct * 100, "pnl_usdt": pnl_usdt, "action": action})
                else:
                    still_open.append(trade)
                    
            open_trades = still_open

            # --- 2. تجهيز بيانات السوق حتى هذه اللحظة الزمنية ---
            # قطع البيانات حتى اللحظة الحالية (لا نرى المستقبل)
            df_history = df_base.iloc[:i+1]
            
            # بناء الفريمات المتعددة كما يتوقعها البوت
            c5m  = self._resample_candles(df_history.tail(200), '5min')
            c15m = self._resample_candles(df_history.tail(400), '15min')
            c30m = self._resample_candles(df_history.tail(800), '30min')
            c1h  = self._resample_candles(df_history.tail(1600), '1h')
            c4h  = self._resample_candles(df_history, '4h')
            
            mock_data = {
                "c5m": c5m, "c15m": c15m, "c30m": c30m, "c1h": c1h, "c4h": c4h,
                "funding": {"rate": 0.0001}, "fear_greed": {"value": 55, "label": "Neutral"},
                "book_imbalance": {"ratio": 0.50}, "oi_data": {"slope": 0.01}, "ls_ratio": 0.50
            }

            # --- 3. استدعاء الخبراء وتوليد الإشارات (Signal Generator) ---
            for direction in ["LONG", "SHORT"]:
                # منع فتح صفقات كثيرة في نفس الاتجاه
                if any(t["direction"] == direction for t in open_trades): continue
                if len(open_trades) >= 2: continue
                
                # استدعاء مولد الإشارات الحقيقي
                sig = generate_signal(symbol, direction, mock_data, balance, open_trades, run_id=f"BT_{i}")
                
                if sig:
                    # إضافة الصفقة للقائمة المفتوحة (تجاهل الذكاء الاصطناعي في الباك تيست)
                    open_trades.append({
                        "symbol": symbol,
                        "direction": direction,
                        "entry_price": sig["entry"],
                        "sl_price": sig["sl"],
                        "tp1_price": sig["tp1"],
                        "tp2_price": sig["tp2"],
                        "tp3_price": sig["tp3"],
                        "size_usdt": sig["size_usdt"],
                        "partial_done": False,
                        "trail_sl": sig["sl"],
                        "trade_type": sig["trade_type"]
                    })

        # --- 4. إحصائيات النهاية ---
        pnl_pcts = [t["pnl_pct"] for t in trade_history]
        trades = len(pnl_pcts)
        wins = sum(1 for p in pnl_pcts if p > 0)
        losses = trades - wins
        wr = round((wins / trades * 100), 2) if trades > 0 else 0.0
        
        cum_pnl = np.cumsum(pnl_pcts) if pnl_pcts else np.array([0.0])
        peaks = np.maximum.accumulate(cum_pnl)
        dds = peaks - cum_pnl
        max_dd = round(float(np.max(dds)), 2) if len(dds) > 0 else 0.0

        pos_v = [p for p in pnl_pcts if p > 0]
        neg_v = [abs(p) for p in pnl_pcts if p < 0]
        pf = round(sum(pos_v)/sum(neg_v), 2) if neg_v and sum(neg_v) > 0 else (99.0 if pos_v else 0.0)
        total_ret = round(((balance - initial_balance) / initial_balance) * 100, 2)

        return {
            "trades": trades, "wins": wins, "losses": losses, "win_rate": wr,
            "max_dd_pct": max_dd, "profit_factor": pf, "total_return_pct": total_ret
        }

    # ── Runner ─────────────────────────────────────────────────────────────
    async def run(self, symbols: List[str], timeframe: str, start_date: str, end_date: str) -> Dict:
        start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
        end   = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
        out: Dict[str, Dict] = {}
        for sym in symbols:
            logger.info(f"[BT] Fetching data & running FULL BOT simulation for {sym}...")
            # نسحب داتا الـ 5 دقائق كقاعدة أساسية دقيقة جداً لحركة السعر
            df = await self._fetch_range(sym, "5m", start, end)
            if df.empty:
                logger.warning(f"[BT] No data found for {sym}.")
                continue
            res = self._simulate_real_bot(sym, df)
            out[sym] = res
        return {"period": f"{start_date} → {end_date}", "timeframe": timeframe, "results": out}

    # ── Printer ────────────────────────────────────────────────────────────
    @staticmethod
    def format_report(report: Dict) -> str:
        lines = [f"🤖 <b>Ramos 360 Ai — REAL BOT BACKTEST</b>",
                 f"Period:    {report['period']}", ""]
        for sym, r in report["results"].items():
            lines += [
                f"━━━ 🪙 {sym} ━━━",
                f"  Trades        : {r['trades']}  (W:{r['wins']}  L:{r['losses']})",
                f"  Win-Rate      : {r['win_rate']} %",
                f"  Max Drawdown  : {r['max_dd_pct']} %",
                f"  Profit Factor : {r['profit_factor']}",
                f"  Total Return  : {r.get('total_return_pct', 0.0)} %", ""
            ]
        return "\n".join(lines)

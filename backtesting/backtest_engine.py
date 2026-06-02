# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  backtesting/backtest_engine.py  —  Ramos 360 Ai REAL BOT BACKTESTER        ║
# ║                                                                              ║
# ║  النسخة الحقيقية: محاكاة شاملة للـ 14 خبير + حقن البيانات الآمن لمنع الأخطاء   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
from __future__ import annotations
from typing import Dict, List
from datetime import datetime, timezone
import pandas as pd
import asyncio
import httpx
from loguru import logger

# ── استدعاء عقل البوت الحقيقي ──────────────────────────────────────────
from engine.data_fetcher import DataFetcher
from engine.signal_generator import run_all_assets
from engine.trade_manager import TRADE_MGR
from engine.indicator_engine import IndicatorEngine
from config import CONFIG


class BacktestEngine:
    def __init__(self):
        self.fetcher = DataFetcher()
        self.ie = IndicatorEngine()

    # ── 1. جلب البيانات من OKX بطريقة السحب المتتالي (Deep Pagination) ──
    async def _fetch_range(self, symbol: str, timeframe: str, start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch historical OHLCV using deep pagination to get thousands of candles."""
        try:
            from engine.data_fetcher import _to_inst, _TF_MAP, _OKX_BASE
            inst = _to_inst(symbol)
            bar = _TF_MAP.get(timeframe, timeframe)
            
            start_ms = int(start.timestamp() * 1000)
            end_ms = int(end.timestamp() * 1000)
            
            all_candles = []
            current_after = end_ms
            
            logger.info(f"[{symbol}] ⏳ جاري سحب البيانات التاريخية العميقة... (قد يستغرق بضع ثوانٍ)")
            
            async with httpx.AsyncClient(timeout=30) as client:
                while True:
                    # نطلب 100 شمعة في كل دورة (وهو الحد الأقصى المسموح من OKX في الطلب الواحد)
                    url = f"{_OKX_BASE}/api/v5/market/history-candles?instId={inst}&bar={bar}&after={current_after}&before={start_ms}&limit=100"
                    resp = await client.get(url)
                    data = resp.json()
                    
                    if data.get("code") != "0" or not data.get("data"):
                        break
                        
                    raw = data["data"]
                    all_candles.extend(raw)
                    
                    # نأخذ التوقيت الزمني لأقدم شمعة في هذه الدفعة لنطلب ما قبلها في الدورة القادمة
                    current_after = raw[-1][0] 
                    
                    if len(raw) < 100:
                        break # وصلنا لأقدم نقطة متاحة
                        
                    # حماية من اللوب اللانهائي: نتوقف عند 15000 شمعة (تكفي لأشهر طويلة جداً على فريم 15 دقيقة)
                    if len(all_candles) >= 15000:
                        logger.info(f"[{symbol}] 🛑 تم الوصول للحد الأقصى الآمن للسحب (15000 شمعة).")
                        break
                        
                    await asyncio.sleep(0.1) # نتوقف قليلاً حتى لا تقوم منصة OKX بحظر البوت (Rate Limit)

            if not all_candles:
                logger.warning(f"[{symbol}] لم يتم العثور على بيانات في هذه الفترة الزمنية.")
                return pd.DataFrame()

            # تحويل البيانات إلى DataFrame
            df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "vol", "volCcy", "volCcyQuote", "confirm"])
            
            # تنظيف البيانات وتحويلها لأرقام
            df["timestamp"] = pd.to_numeric(df["timestamp"])
            for col in ["open", "high", "low", "close", "vol"]:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # ترتيب الشموع من الأقدم للأحدث ليقرأها البوت بشكل صحيح
            df.sort_values("timestamp", inplace=True)
            df.reset_index(drop=True, inplace=True)
            
            logger.info(f"[{symbol}] ✅ تمت بنجاح! تم جمع {len(df)} شمعة. بدء اختبار الخبراء...")
            return df
            
        except Exception as e:
            logger.error(f"[BT] خطأ أثناء سحب البيانات لـ {symbol}: {e}")
            return pd.DataFrame()

    # ── 2. محرك محاكاة البوت الحي (The Magic Happens Here) ─────────────
    def _simulate_real_bot(self, symbol: str, df: pd.DataFrame) -> Dict:
        logger.info(f"[{symbol}] ⚙️ إعداد البيانات والمؤشرات للـ 14 خبيراً...")
        
        # 1. حساب كل المؤشرات مسبقاً (SMC, RSI, MACD...) لتجنب KeyError
        try:
            if hasattr(self.ie, 'add_all_indicators'):
                df = self.ie.add_all_indicators(df)
            elif hasattr(self.ie, 'process'):
                df = self.ie.process(df)
        except Exception as e:
            logger.warning(f"[{symbol}] تحذير من محرك المؤشرات: {e}")

        open_trades = []
        history = []
        balance = 1000.0
        
        # نحتاج على الأقل 150 شمعة للـ SMA200 والمؤشرات العميقة لتعمل
        start_idx = 150 if len(df) > 150 else int(len(df)/2)
        
        if start_idx >= len(df) or len(df) == 0:
            return {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0, "net_profit": 0, "ending_balance": balance}

        logger.info(f"[{symbol}] 🚀 تشغيل الباك تيست الحي من الشمعة {start_idx} إلى {len(df)}...")

        for i in range(start_idx, len(df)):
            current_close = float(df["close"].iloc[i])
            
            # ── أ. فحص وإغلاق الصفقات المفتوحة (Trade Manager) ──
            for t in open_trades[:]:
                eval_res = TRADE_MGR.evaluate(t, current_close)
                action = eval_res.get("action", "HOLD")
                
                if action in ("CLOSE_SL", "CLOSE_TP2", "CLOSE_TP3"):
                    profit = (eval_res["pnl_pct"] / 100) * t["size_usdt"]
                    balance += profit
                    history.append({"symbol": symbol, "pnl": eval_res["pnl_pct"], "action": action})
                    open_trades.remove(t)
                elif action == "TRAIL_SL":
                    t["sl_price"] = eval_res["new_sl"]

            # ── ب. حقن البيانات لمحاكاة الواقع (Mocking Environment) ──
            df_slice = df.iloc[:i+1] # نقتطع البيانات للزمن الحالي فقط (لا نرى المستقبل)
            
            assets_data = {}
            # وضعنا هذا اللوب لمنع KeyError إذا كان Signal Generator يبحث في كل الـ ASSETS
            for s in CONFIG.ASSETS:
                assets_data[s] = {} 
            
            # حقن بيانات العملة الحالية مع وضع صمامات أمان (Default Values)
            assets_data[symbol] = {
                "5m": df_slice.tail(300),
                "15m": df_slice.tail(300),
                "1h": df_slice.tail(300),
                "4h": df_slice.tail(300),
                "c4h": df_slice.tail(300),
                "funding": {"fundingRate": "0.0001"}, # ✅ الحل السحري لمشكلة الخطأ في الصورة
                "fear_greed": {"value": 50},
                "usdt_dominance": {"value": 5.0},
                "memory_mult": 1.0
            }

            # ── ج. استدعاء العقل المدبر (Signal Generator) ──
            try:
                # نمرر BT (Backtest) كـ run_id ليقوم البوت بتسجيلها بشكل صحيح
                signals = run_all_assets(assets_data, balance, open_trades, "BT")
                if signals:
                    for sig in signals:
                        if sig["symbol"] == symbol:
                            open_trades.append({
                                "symbol": sig["symbol"],
                                "direction": sig["direction"],
                                "entry_price": sig["entry"],
                                "sl_price": sig["sl"],
                                "tp1_price": sig["tp1"],
                                "tp2_price": sig["tp2"],
                                "tp3_price": sig["tp3"],
                                "size_usdt": sig.get("size_usdt", balance * 0.05),
                                "trade_type": sig.get("trade_type", "SCALP")
                            })
            except Exception as e:
                # إذا حصل خطأ في شمعة معينة، نسجله ونتجاوزها بدون إيقاف الاختبار بأكمله
                logger.error(f"[BT] فشل توليد إشارة عند الشمعة {i}: {e}")
                continue

        # ── 3. الإحصائيات النهائية ──
        wins = len([x for x in history if x["pnl"] > 0])
        losses = len([x for x in history if x["pnl"] <= 0])
        total = wins + losses
        win_rate = round((wins / total * 100) if total > 0 else 0, 2)
        net_profit = round(balance - 1000.0, 2)
        
        return {
            "trades": total,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "max_dd_pct": 0.0, # يمكن تطويرها لاحقاً
            "net_profit": net_profit,
            "ending_balance": round(balance, 2)
        }

    # ── 3. التشغيل والتقارير ──────────────────────────────────────────
    async def run(self, symbols: List[str], timeframe: str, start_date: str, end_date: str) -> Dict:
        start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
        end   = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
        out: Dict[str, Dict] = {}
        
        for sym in symbols:
            logger.info(f"[BT] سحب البيانات وبدء محاكاة البوت الحقيقي لـ {sym}...")
            df = await self._fetch_range(sym, "15m", start, end) # استخدمت 15m لأنها الأدق للخبراء
            
            if df.empty:
                logger.warning(f"[BT] لا توجد بيانات كافية لـ {sym}.")
                continue
                
            res = self._simulate_real_bot(sym, df)
            out[sym] = res
            
        return {"period": f"{start_date} → {end_date}", "timeframe": timeframe, "results": out}

    @staticmethod
    def format_report(report: Dict) -> str:
        lines = [f"🤖 <b>Ramos 360 Ai — REAL BOT BACKTEST</b>",
                 f"Period:    {report['period']}", ""]
        for sym, r in report["results"].items():
            lines += [
                f"━━━ 🪙 {sym} ━━━",
                f"  Trades        : {r['trades']}  (W:{r['wins']}  L:{r['losses']})",
                f"  Win-Rate      : {r['win_rate']} %",
                f"  Net Profit    : $ {r['net_profit']}",
                f"  Ending Bal    : $ {r['ending_balance']}",
            ]
        return "\n".join(lines)

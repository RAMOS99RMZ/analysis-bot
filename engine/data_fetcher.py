# engine/data_fetcher.py
# ══════════════════════════════════════════════════════════════════
# الإصلاح الجوهري: فصل Exchange عام (بيانات السوق) عن الخاص (الحساب)
# البيانات العامة (شموع، أسعار) لا تحتاج API Key
# البيانات الخاصة (رصيد، مراكز) تحتاج API Key حقيقي
# ══════════════════════════════════════════════════════════════════

from __future__ import annotations
import asyncio, time
from typing import Dict, List, Optional
import ccxt.async_support as ccxt
import httpx
from loguru import logger
from config import Secrets, CONFIG
from utils.helpers import clear_api_fails, record_api_fail, is_circuit_open

# ── Cache ─────────────────────────────────────────────────────────
_CANDLES_CACHE: Dict[str, Dict] = {}
_TTL = {"5m": 120, "15m": 90, "30m": 180, "1h": 300, "4h": 600, "1d": 900, "1w": 43200}
_TICKERS_CACHE: Dict = {"data": None, "ts": 0}
_FUNDING_CACHE: Dict = {}
_SLOW_CACHE:    Dict = {}


class DataFetcher:
    """
    OKX data fetcher مع فصل العام عن الخاص.

    _public  → بدون مفاتيح → للشموع، الأسعار، الأوردر بوك
    _private → بمفاتيح حقيقية → للرصيد، المراكز، الصفقات
    """

    def __init__(self) -> None:
        # ── Exchange عام: بيانات السوق بدون مصادقة ──────────────
        self._public = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        })

        # ── Exchange خاص: عمليات الحساب بمفاتيح حقيقية ──────────
        self._private = ccxt.okx({
            "apiKey":          Secrets.OKX_KEY,
            "secret":          Secrets.OKX_SECRET,
            "password":        Secrets.OKX_PASS,
            "enableRateLimit": True,
            "options":         {"defaultType": "swap"},
        })

        # هل المفاتيح حقيقية؟
        self._has_keys = all([
            Secrets.OKX_KEY    and len(Secrets.OKX_KEY)    > 10,
            Secrets.OKX_SECRET and len(Secrets.OKX_SECRET) > 10,
            Secrets.OKX_PASS   and len(Secrets.OKX_PASS)   > 3,
        ])
        if not self._has_keys:
            logger.warning(
                "⚠️  [DataFetcher] مفاتيح OKX غير موجودة أو وهمية.\n"
                "   ✅ البيانات العامة (شموع، أسعار) ستعمل بشكل طبيعي.\n"
                "   ❌ الرصيد والمراكز لن تعمل حتى تضيف مفاتيح حقيقية."
            )

    async def close(self) -> None:
        await self._public.close()
        if self._has_keys:
            await self._private.close()

    # ══════════════════════════════════════════════════════════════
    # البيانات العامة — تعمل حتى بدون مفاتيح OKX
    # ══════════════════════════════════════════════════════════════

    async def get_candles(self, symbol: str, timeframe: str = "5m",
                          limit: int = 100) -> Optional[List]:
        """جلب الشموع — لا يحتاج API Key."""
        key = f"{symbol}|{timeframe}|{limit}"
        ttl = _TTL.get(timeframe, 60)
        hit = _CANDLES_CACHE.get(key)
        if hit and (time.time() - hit["ts"]) < ttl:
            return hit["data"]
        try:
            ohlcv = await self._public.fetch_ohlcv(symbol, timeframe, limit=limit)
            ohlcv.reverse()   # OKX: أحدث أولاً
            data = [[c[0], c[1], c[2], c[3], c[4], c[5]] for c in ohlcv]
            _CANDLES_CACHE[key] = {"data": data, "ts": time.time()}
            clear_api_fails()
            return data
        except Exception as e:
            logger.warning(f"[DataFetcher] get_candles {symbol} {timeframe}: {e}")
            record_api_fail()
            return None

    async def get_all_data(self, symbol: str) -> Optional[Dict]:
        """جلب كل الإطارات الزمنية — لا يحتاج API Key."""
        if is_circuit_open():
            logger.warning(f"[DataFetcher] Circuit breaker OPEN — skipping {symbol}")
            return None
        tasks = [
            self.get_candles(symbol, "5m",  100),
            self.get_candles(symbol, "15m", 100),
            self.get_candles(symbol, "30m", 100),
            self.get_candles(symbol, "1h",  200),
            self.get_candles(symbol, "4h",  200),
            self._get_order_book(symbol),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        c5m, c15m, c30m, c1h, c4h, book = [
            r if not isinstance(r, Exception) else None for r in results
        ]
        if not c5m or len(c5m) < 20:
            logger.warning(f"[DataFetcher] بيانات غير كافية لـ {symbol}")
            return None

        book_imbalance = None
        if book and "bids" in book and "asks" in book:
            bv = sum(float(b[1]) for b in book["bids"][:10])
            av = sum(float(a[1]) for a in book["asks"][:10])
            total = bv + av or 1
            book_imbalance = {"bid_vol": bv, "ask_vol": av, "ratio": bv / total}

        slow = await self.get_slow_data(symbol)
        return {
            "c5m": c5m, "c15m": c15m or [], "c30m": c30m or [],
            "c1h": c1h or [], "c4h": c4h or [],
            "book": book, "book_imbalance": book_imbalance,
            "c1d": slow.get("c1d"), "c1w": slow.get("c1w"),
        }

    async def fetch_all_assets(self) -> Dict[str, Optional[Dict]]:
        """جلب بيانات كل العملات بالتوازي."""
        tasks = {s: self.get_all_data(s) for s in CONFIG.ASSETS}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        out = {}
        for symbol, result in zip(tasks.keys(), results):
            out[symbol] = result if not isinstance(result, Exception) else None
        good = sum(1 for v in out.values() if v)
        logger.info(f"[DataFetcher] تم جلب {good}/{len(CONFIG.ASSETS)} عملة بنجاح ✅")
        return out

    async def get_slow_data(self, symbol: str) -> Dict:
        """شموع 1D/1W — لا تحتاج API Key."""
        hit = _SLOW_CACHE.get(symbol)
        if hit and (time.time() - hit["ts"]) < 43200:
            return hit["data"]
        tasks = [
            self.get_candles(symbol, "1d", 30),
            self.get_candles(symbol, "1w", 12),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        data = {
            "c1d": results[0] if not isinstance(results[0], Exception) else None,
            "c1w": results[1] if not isinstance(results[1], Exception) else None,
        }
        _SLOW_CACHE[symbol] = {"data": data, "ts": time.time()}
        return data

    async def get_live_price(self, symbol: str) -> float:
        """السعر الحي — لا يحتاج API Key."""
        if _TICKERS_CACHE["data"] and (time.time() - _TICKERS_CACHE["ts"]) < 90:
            p = _TICKERS_CACHE["data"].get(symbol, 0)
            if p: return p
        await self._refresh_tickers()
        return _TICKERS_CACHE["data"].get(symbol, 0) if _TICKERS_CACHE["data"] else 0

    async def _refresh_tickers(self) -> None:
        try:
            tickers = await self._public.fetch_tickers()
            _TICKERS_CACHE["data"] = {k: float(v.get("last", 0)) for k, v in tickers.items()}
            _TICKERS_CACHE["ts"]   = time.time()
        except Exception as e:
            logger.warning(f"[DataFetcher] refresh_tickers: {e}")

    async def _get_order_book(self, symbol: str, depth: int = 20) -> Optional[Dict]:
        """أوردر بوك — لا يحتاج API Key."""
        try:
            return await self._public.fetch_order_book(symbol, depth)
        except Exception as e:
            logger.warning(f"[DataFetcher] order_book {symbol}: {e}")
            return None

    async def get_fear_greed(self) -> Dict:
        """Fear & Greed من alternative.me — لا يحتاج أي مفاتيح."""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get("https://api.alternative.me/fng/?limit=1")
                d = r.json()
                val   = int(d["data"][0]["value"])
                label = d["data"][0]["value_classification"]
                return {
                    "value": val, "label": label,
                    "extreme_fear":  val < 25,
                    "fear":          val < 45,
                    "greed":         val > 55,
                    "extreme_greed": val > 75,
                }
        except Exception as e:
            logger.warning(f"[DataFetcher] fear_greed: {e}")
            return {"value": 50, "label": "Neutral",
                    "extreme_fear": False, "fear": False,
                    "greed": False, "extreme_greed": False}

    # ══════════════════════════════════════════════════════════════
    # البيانات الخاصة — تحتاج مفاتيح OKX حقيقية
    # ══════════════════════════════════════════════════════════════

    async def get_balance(self) -> float:
        """الرصيد الحي — يحتاج مفاتيح حقيقية."""
        if not self._has_keys:
            logger.warning("[DataFetcher] get_balance: لا توجد مفاتيح OKX حقيقية → رصيد وهمي 1000$")
            return 1000.0   # رصيد افتراضي للاختبار
        try:
            bal = await self._private.fetch_balance()
            return float(bal.get("USDT", {}).get("total", 0) or 0)
        except Exception as e:
            logger.warning(f"[DataFetcher] get_balance: {e}")
            return 0.0

    async def get_open_positions(self) -> List[Dict]:
        """المراكز المفتوحة — يحتاج مفاتيح حقيقية."""
        if not self._has_keys:
            return []
        try:
            positions = await self._private.fetch_positions()
            return [p for p in positions if float(p.get("contracts", 0)) != 0]
        except Exception as e:
            logger.warning(f"[DataFetcher] get_open_positions: {e}")
            return []

    async def get_funding_rate(self, symbol: str) -> Dict:
        """Funding Rate — لا يحتاج API Key (بيانات عامة)."""
        hit = _FUNDING_CACHE.get(symbol)
        if hit and (time.time() - hit["ts"]) < 600:
            return hit["data"]
        try:
            info = await self._public.fetch_funding_rate(symbol)
            fr   = float(info.get("fundingRate", 0) or 0)
            data = {
                "funding_rate": fr,
                "bullish":      fr < -0.0001,
                "bearish":      fr > 0.0005,
                "strong_bull":  fr < -0.0003,
                "strong_bear":  fr > 0.0010,
            }
            _FUNDING_CACHE[symbol] = {"data": data, "ts": time.time()}
            return data
        except Exception as e:
            logger.warning(f"[DataFetcher] funding_rate {symbol}: {e}")
            return {"funding_rate": 0, "bullish": False, "bearish": False,
                    "strong_bull": False, "strong_bear": False}

    async def get_btc_global_bias(self) -> Dict:
        """BTC Global Bias — لا يحتاج API Key."""
        from engine.indicator_engine import IndicatorEngine
        btc = "BTC/USDT:USDT"
        c4h, c1h = await asyncio.gather(
            self.get_candles(btc, "4h", 50),
            self.get_candles(btc, "1h", 30),
        )
        if not c4h or not c1h:
            return {"bias": "NEUTRAL", "reason": "No data"}
        ie = IndicatorEngine()
        t4 = ie.get_trend(c4h)
        t1 = ie.get_trend_1h(c1h)
        if t4 == "BULL" and t1 == "BULL":
            return {"bias": "BULL_STRONG", "reason": "BTC 4H+1H صاعد"}
        if t4 == "BEAR" and t1 == "BEAR":
            return {"bias": "BEAR_STRONG", "reason": "BTC 4H+1H هابط"}
        return {"bias": "NEUTRAL", "reason": "مختلط"}

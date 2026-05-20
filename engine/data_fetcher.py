
# engine/data_fetcher.py — FIXED: httpx مباشرة لـ OKX (بدل ccxt للبيانات العامة)
# ══════════════════════════════════════════════════════════════════
# السبب الجذري للخطأ:
#   ccxt.okx.fetch_ohlcv → يحاول None + "str" في بناء URL
#   → الحل: httpx مباشر لـ OKX REST API (لا يحتاج مفاتيح)
# ══════════════════════════════════════════════════════════════════
from __future__ import annotations
import asyncio, time
from typing import Dict, List, Optional
import httpx
import ccxt.async_support as ccxt
from loguru import logger
from config import Secrets, CONFIG
from utils.helpers import clear_api_fails, record_api_fail, is_circuit_open

# ── OKX Public REST ───────────────────────────────────────────────
_OKX_BASE = "https://www.okx.com"

# تحويل symbol لـ OKX instId
def _to_inst(symbol: str) -> str:
    """BTC/USDT:USDT → BTC-USDT-SWAP"""
    return symbol.replace("/", "-").replace(":", "-").replace("USDT-USDT", "USDT-SWAP")

# تحويل timeframe لـ OKX bar
_TF_MAP = {
    "1m": "1m",  "3m": "3m",  "5m": "5m",  "15m": "15m",
    "30m": "30m", "1h": "1H", "2h": "2H",  "4h": "4H",
    "6h": "6H",  "12h": "12H", "1d": "1D", "1w": "1W",
}

# ── Cache ─────────────────────────────────────────────────────────
_CANDLES_CACHE: Dict[str, Dict] = {}
_TTL = {"5m":60,"15m":90,"30m":180,"1h":300,"4h":600,"1d":900,"1w":43200}
_TICKERS_CACHE: Dict = {"data": None, "ts": 0}
_FUNDING_CACHE: Dict = {}
_SLOW_CACHE:    Dict = {}


class DataFetcher:

    def __init__(self) -> None:
        self._has_keys = all([
            Secrets.OKX_KEY    and len(Secrets.OKX_KEY)    > 10,
            Secrets.OKX_SECRET and len(Secrets.OKX_SECRET) > 10,
            Secrets.OKX_PASS   and len(Secrets.OKX_PASS)   > 3,
        ])
        # ccxt فقط للعمليات الخاصة (رصيد، مراكز)
        if self._has_keys:
            self._private = ccxt.okx({
                "apiKey": Secrets.OKX_KEY, "secret": Secrets.OKX_SECRET,
                "password": Secrets.OKX_PASS, "enableRateLimit": True,
                "options": {"defaultType": "swap"},
            })
        else:
            self._private = None
            logger.warning(
                "⚠️  [DataFetcher] مفاتيح OKX غير موجودة أو وهمية.\n"
                "   ✅ البيانات العامة (شموع، أسعار) ستعمل بشكل طبيعي.\n"
                "   ❌ الرصيد والمراكز لن تعمل حتى تضيف مفاتيح حقيقية."
            )
        self._http = httpx.AsyncClient(
            timeout=15,
            headers={"User-Agent": "RamosAi360/1.0"},
            follow_redirects=True,
        )

    async def close(self) -> None:
        await self._http.aclose()
        if self._private:
            await self._private.close()

    # ══════════════════════════════════════════════════════════════
    # الشموع — httpx مباشر لـ OKX REST (الحل الجذري)
    # ══════════════════════════════════════════════════════════════
    async def get_candles(self, symbol: str, timeframe: str = "5m",
                          limit: int = 100) -> Optional[List]:
        key = f"{symbol}|{timeframe}|{limit}"
        ttl = _TTL.get(timeframe, 60)
        hit = _CANDLES_CACHE.get(key)
        if hit and (time.time() - hit["ts"]) < ttl:
            return hit["data"]
        try:
            inst = _to_inst(symbol)
            bar  = _TF_MAP.get(timeframe, timeframe)
            # OKX تُرجع أحدث أولاً — نحتاج limit*2 ثم نعكس
            url  = (f"{_OKX_BASE}/api/v5/market/candles"
                    f"?instId={inst}&bar={bar}&limit={min(limit, 300)}")
            r    = await self._http.get(url)
            raw  = r.json()
            if raw.get("code") != "0" or not raw.get("data"):
                raise ValueError(f"OKX error: {raw.get('msg','')}")
            # فرمات OKX: [ts, open, high, low, close, vol, ...]
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
            # OKX: أحدث أولاً (نفس ما يتوقعه الكود)
            _CANDLES_CACHE[key] = {"data": candles, "ts": time.time()}
            clear_api_fails()
            return candles
        except Exception as e:
            logger.warning(f"[DataFetcher] get_candles {symbol} {timeframe}: {e}")
            record_api_fail()
            return None

    async def get_live_price(self, symbol: str) -> float:
        try:
            inst = _to_inst(symbol)
            url  = f"{_OKX_BASE}/api/v5/market/ticker?instId={inst}"
            r    = await self._http.get(url)
            raw  = r.json()
            if raw.get("code") == "0" and raw.get("data"):
                return float(raw["data"][0].get("last", 0))
        except Exception as e:
            logger.warning(f"[DataFetcher] live_price {symbol}: {e}")
        return 0.0

    async def get_all_data(self, symbol: str) -> Optional[Dict]:
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
            book_imbalance = {"bid_vol": bv, "ask_vol": av, "ratio": bv/total}

        slow = await self.get_slow_data(symbol)
        return {
            "c5m": c5m, "c15m": c15m or [], "c30m": c30m or [],
            "c1h": c1h or [], "c4h": c4h or [],
            "book": book, "book_imbalance": book_imbalance,
            "c1d": slow.get("c1d"), "c1w": slow.get("c1w"),
        }

    async def fetch_all_assets(self) -> Dict[str, Optional[Dict]]:
        tasks = {s: self.get_all_data(s) for s in CONFIG.ASSETS}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        out = {}
        for symbol, result in zip(tasks.keys(), results):
            out[symbol] = result if not isinstance(result, Exception) else None
        good = sum(1 for v in out.values() if v)
        logger.info(f"[DataFetcher] تم جلب {good}/{len(CONFIG.ASSETS)} عملة بنجاح ✅")
        return out

    async def get_slow_data(self, symbol: str) -> Dict:
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

    async def _get_order_book(self, symbol: str, depth: int = 20) -> Optional[Dict]:
        try:
            inst = _to_inst(symbol)
            url  = f"{_OKX_BASE}/api/v5/market/books?instId={inst}&sz={depth}"
            r    = await self._http.get(url)
            raw  = r.json()
            if raw.get("code") == "0" and raw.get("data"):
                d = raw["data"][0]
                return {"bids": d.get("bids",[]), "asks": d.get("asks",[])}
        except Exception as e:
            logger.warning(f"[DataFetcher] order_book {symbol}: {e}")
        return None

    async def get_funding_rate(self, symbol: str) -> Dict:
        hit = _FUNDING_CACHE.get(symbol)
        if hit and (time.time() - hit["ts"]) < 600:
            return hit["data"]
        try:
            inst = _to_inst(symbol)
            url  = f"{_OKX_BASE}/api/v5/public/funding-rate?instId={inst}"
            r    = await self._http.get(url)
            raw  = r.json()
            fr   = 0.0
            if raw.get("code") == "0" and raw.get("data"):
                fr = float(raw["data"][0].get("fundingRate", 0) or 0)
            data = {
                "funding_rate": fr,
                "bullish":     fr < -0.0001, "bearish":     fr > 0.0005,
                "strong_bull": fr < -0.0003, "strong_bear": fr > 0.0010,
            }
            _FUNDING_CACHE[symbol] = {"data": data, "ts": time.time()}
            return data
        except Exception as e:
            logger.warning(f"[DataFetcher] funding_rate {symbol}: {e}")
            return {"funding_rate":0,"bullish":False,"bearish":False,
                    "strong_bull":False,"strong_bear":False}

    async def get_fear_greed(self) -> Dict:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get("https://api.alternative.me/fng/?limit=1")
                d = r.json()
                val   = int(d["data"][0]["value"])
                label = d["data"][0]["value_classification"]
                return {
                    "value": val, "label": label,
                    "extreme_fear": val < 25, "fear": val < 45,
                    "greed": val > 55, "extreme_greed": val > 75,
                }
        except Exception as e:
            logger.warning(f"[DataFetcher] fear_greed: {e}")
            return {"value":50,"label":"Neutral","extreme_fear":False,
                    "fear":False,"greed":False,"extreme_greed":False}

    # ══════════════════════════════════════════════════════════════
    # البيانات الخاصة — ccxt مع مفاتيح حقيقية
    # ══════════════════════════════════════════════════════════════
    async def get_balance(self) -> float:
        if not self._has_keys:
            logger.warning("[DataFetcher] get_balance: لا توجد مفاتيح OKX حقيقية → رصيد وهمي 1000$")
            return 1000.0
        try:
            bal = await self._private.fetch_balance()
            return float(bal.get("USDT", {}).get("total", 0) or 0)
        except Exception as e:
            logger.warning(f"[DataFetcher] get_balance: {e}")
            return 1000.0

    async def get_open_positions(self) -> List[Dict]:
        if not self._has_keys: return []
        try:
            positions = await self._private.fetch_positions()
            return [p for p in positions if float(p.get("contracts", 0)) != 0]
        except Exception as e:
            logger.warning(f"[DataFetcher] get_open_positions: {e}")
            return []

    async def get_btc_global_bias(self) -> Dict:
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

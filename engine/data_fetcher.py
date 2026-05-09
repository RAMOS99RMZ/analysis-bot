# engine/data_fetcher.py
# Mirrors: getAllData, getCandles, getLivePrice, getFundingRate, getOnChainData
# Uses CCXT for exchange abstraction + async for performance

from __future__ import annotations
import asyncio, time
from typing import Dict, List, Optional, Tuple
import ccxt.async_support as ccxt
import httpx
from loguru import logger
from config import Secrets, CONFIG
from utils.helpers import clear_api_fails, record_api_fail, is_circuit_open

# ── Candle TTL cache (mirrors _CANDLES_CACHE in GAS) ─────────────────────────
_CANDLES_CACHE: Dict[str, Dict] = {}
_TTL = {"5m": 120, "15m": 90, "30m": 180, "1h": 300, "4h": 600, "1d": 900, "1w": 43200}
_TICKERS_CACHE: Dict = {"data": None, "ts": 0}
_FUNDING_CACHE: Dict = {}
_SLOW_CACHE: Dict = {}

# OKX CCXT timeframe map
_TF_MAP = {"5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h", "4h": "4h", "1d": "1d", "1w": "1w"}


class DataFetcher:
    """
    Async CCXT-based data fetcher for OKX.
    Replaces all UrlFetchApp calls in GAS with proper async HTTP.
    """

    def __init__(self) -> None:
        self._exchange = ccxt.okx({
            "apiKey":    Secrets.OKX_KEY,
            "secret":    Secrets.OKX_SECRET,
            "password":  Secrets.OKX_PASS,
            "enableRateLimit": True,
            "options":   {"defaultType": "swap"},
        })

    async def close(self) -> None:
        await self._exchange.close()

    # ── Candles ───────────────────────────────────────────────────────────────

    async def get_candles(self, symbol: str, timeframe: str = "5m",
                          limit: int = 100) -> Optional[List]:
        """
        Fetch OHLCV candles. Returns list[list] in OKX format:
        [timestamp_ms, open, high, low, close, volume]
        Newest candle is index 0 (OKX convention, matches GAS).
        """
        key = f"{symbol}|{timeframe}|{limit}"
        ttl = _TTL.get(timeframe, 60)
        hit = _CANDLES_CACHE.get(key)
        if hit and (time.time() - hit["ts"]) < ttl:
            return hit["data"]
        try:
            ohlcv = await self._exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            # CCXT returns oldest-first → reverse to newest-first (OKX/GAS convention)
            ohlcv.reverse()
            # Convert to list[list] matching OKX API format
            data = [[c[0], c[1], c[2], c[3], c[4], c[5]] for c in ohlcv]
            _CANDLES_CACHE[key] = {"data": data, "ts": time.time()}
            clear_api_fails()
            return data
        except Exception as e:
            logger.warning(f"[DataFetcher] get_candles {symbol} {timeframe}: {e}")
            record_api_fail()
            return None

    async def get_all_data(self, symbol: str) -> Optional[Dict]:
        """
        Fetch 5m/15m/30m/1h/4h candles + orderbook in parallel.
        Mirrors getAllData() in GAS (uses fetchAll equivalent).
        """
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
            logger.warning(f"[DataFetcher] Insufficient 5m data for {symbol}")
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
        """
        Parallel fetch for all configured assets.
        Mirrors fetchAllAssetsData() in GAS (with batching).
        """
        tasks = {s: self.get_all_data(s) for s in CONFIG.ASSETS}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        out = {}
        for symbol, result in zip(tasks.keys(), results):
            out[symbol] = result if not isinstance(result, Exception) else None
        good = sum(1 for v in out.values() if v)
        logger.info(f"[DataFetcher] Fetched {good}/{len(CONFIG.ASSETS)} assets")
        return out

    async def get_slow_data(self, symbol: str) -> Dict:
        """1D/1W candles cached 12 hours. Mirrors getSlowData()."""
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

    # ── Price ─────────────────────────────────────────────────────────────────

    async def get_live_price(self, symbol: str) -> float:
        """Live price with batch cache. Mirrors getLivePrice()."""
        if _TICKERS_CACHE["data"] and (time.time() - _TICKERS_CACHE["ts"]) < 90:
            p = _TICKERS_CACHE["data"].get(symbol, 0)
            if p: return p
        await self._refresh_tickers()
        return _TICKERS_CACHE["data"].get(symbol, 0) if _TICKERS_CACHE["data"] else 0

    async def _refresh_tickers(self) -> None:
        try:
            tickers = await self._exchange.fetch_tickers()
            _TICKERS_CACHE["data"] = {k: float(v.get("last", 0)) for k, v in tickers.items()}
            _TICKERS_CACHE["ts"] = time.time()
        except Exception as e:
            logger.warning(f"[DataFetcher] refresh_tickers: {e}")

    # ── Order Book ────────────────────────────────────────────────────────────

    async def _get_order_book(self, symbol: str, depth: int = 20) -> Optional[Dict]:
        try:
            ob = await self._exchange.fetch_order_book(symbol, depth)
            return ob
        except Exception as e:
            logger.warning(f"[DataFetcher] order_book {symbol}: {e}")
            return None

    # ── Funding Rate ──────────────────────────────────────────────────────────

    async def get_funding_rate(self, symbol: str) -> Dict:
        """Mirrors getFundingRate() + getFundingHistory() in GAS."""
        hit = _FUNDING_CACHE.get(symbol)
        if hit and (time.time() - hit["ts"]) < 600:
            return hit["data"]
        try:
            info = await self._exchange.fetch_funding_rate(symbol)
            fr = float(info.get("fundingRate", 0) or 0)
            data = {
                "funding_rate": fr,
                "bullish": fr < -0.0001,
                "bearish": fr > 0.0005,
                "strong_bull": fr < -0.0003,
                "strong_bear": fr > 0.0010,
            }
            _FUNDING_CACHE[symbol] = {"data": data, "ts": time.time()}
            return data
        except Exception as e:
            logger.warning(f"[DataFetcher] funding_rate {symbol}: {e}")
            return {"funding_rate": 0, "bullish": False, "bearish": False}

    # ── Account Balance ───────────────────────────────────────────────────────

    async def get_balance(self) -> float:
        """Live USDT balance. Mirrors getRealBalance()."""
        try:
            bal = await self._exchange.fetch_balance()
            total = bal.get("USDT", {}).get("total", 0) or 0
            return float(total)
        except Exception as e:
            logger.warning(f"[DataFetcher] get_balance: {e}")
            return 0.0

    # ── Open Positions ────────────────────────────────────────────────────────

    async def get_open_positions(self) -> List[Dict]:
        """Fetch all currently open perpetual positions."""
        try:
            positions = await self._exchange.fetch_positions()
            return [p for p in positions if float(p.get("contracts", 0)) != 0]
        except Exception as e:
            logger.warning(f"[DataFetcher] get_open_positions: {e}")
            return []

    # ── Fear & Greed Index (external API) ─────────────────────────────────────

    async def get_fear_greed(self) -> Dict:
        """Fetch BTC Fear & Greed from alternative.me."""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get("https://api.alternative.me/fng/?limit=1")
                d = r.json()
                val = int(d["data"][0]["value"])
                label = d["data"][0]["value_classification"]
                return {
                    "value": val, "label": label,
                    "extreme_fear": val < 25, "fear": val < 45,
                    "greed": val > 55, "extreme_greed": val > 75,
                }
        except Exception as e:
            logger.warning(f"[DataFetcher] fear_greed: {e}")
            return {"value": 50, "label": "Neutral", "extreme_fear": False,
                    "fear": False, "greed": False, "extreme_greed": False}

    # ── BTC Global Bias ───────────────────────────────────────────────────────

    async def get_btc_global_bias(self) -> Dict:
        """Get BTC 4H + 1H trend alignment. Mirrors getBTCGlobalBias()."""
        from engine.indicator_engine import IndicatorEngine
        btc = "BTC/USDT:USDT"
        tasks = [self.get_candles(btc, "4h", 50), self.get_candles(btc, "1h", 30)]
        results = await asyncio.gather(*tasks)
        c4h, c1h = results
        if not c4h or not c1h:
            return {"bias": "NEUTRAL", "reason": ""}
        ie = IndicatorEngine()
        t4 = ie.get_trend(c4h)
        t1 = ie.get_trend_1h(c1h)
        if t4 == "BEAR" and t1 == "BEAR":
            return {"bias": "BEAR_STRONG", "reason": "BTC 4H+1H bearish"}
        if t4 == "BULL" and t1 == "BULL":
            return {"bias": "BULL_STRONG", "reason": "BTC 4H+1H bullish"}
        return {"bias": "NEUTRAL", "reason": ""}

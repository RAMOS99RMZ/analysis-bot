"""
engine/data_fetcher.py — Ramos 360 Ai 🎖️
Public market data (candles, prices) works WITHOUT OKX keys.
Private data (balance, positions) requires OKX keys.
"""
from __future__ import annotations
import asyncio, time
from datetime import datetime, timezone
from typing import Dict, List, Optional
import ccxt.async_support as ccxt
import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import Secrets, CONFIG

# ── Cache storage ─────────────────────────────────────────────────────────────
_CACHE: Dict[str, Dict] = {}
_TTL   = {"5m":90,"15m":180,"30m":300,"1h":360,"4h":720,"1d":3600,"1w":14400}
_TICKERS: Dict = {"data": None, "ts": 0}
_FUNDING: Dict = {}


def _hit(key: str, ttl: int) -> Optional[List]:
    h = _CACHE.get(key)
    return h["d"] if h and (time.time() - h["ts"]) < ttl else None


def _set(key: str, data: List) -> None:
    _CACHE[key] = {"d": data, "ts": time.time()}


# ── Circuit Breaker ────────────────────────────────────────────────────────────
_fail_count = 0
def _fail() -> None: global _fail_count; _fail_count += 1
def _ok()   -> None: global _fail_count; _fail_count = 0
def circuit_open() -> bool: return _fail_count >= 8


class DataFetcher:
    """
    OKX data fetcher with public/private separation.
    Public endpoints (candles, prices) → no API keys needed.
    Private endpoints (balance, positions) → require OKX keys.
    """

    def __init__(self) -> None:
        base = {"enableRateLimit": True, "options": {"defaultType": "swap"}}

        # Public — no auth, works for everyone
        self._pub = ccxt.okx({**base})

        # Private — only initialized if keys exist
        self._prv: Optional[ccxt.okx] = None
        if Secrets.has_okx():
            self._prv = ccxt.okx({
                **base,
                "apiKey":   Secrets.OKX_KEY,
                "secret":   Secrets.OKX_SECRET,
                "password": Secrets.OKX_PASS,
            })
            logger.info("✅ OKX private client initialized")
        else:
            logger.info("ℹ️  OKX public-only mode (no trading — analysis only)")

    async def close(self) -> None:
        await self._pub.close()
        if self._prv:
            await self._prv.close()

    # ── PUBLIC: Candles ────────────────────────────────────────────────────────

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=6),
           retry=retry_if_exception_type(Exception), reraise=False)
    async def get_candles(self, symbol: str, tf: str = "5m",
                          limit: int = 100) -> Optional[List]:
        key = f"{symbol}|{tf}|{limit}"
        cached = _hit(key, _TTL.get(tf, 60))
        if cached: return cached
        try:
            ohlcv = await self._pub.fetch_ohlcv(symbol, tf, limit=limit)
            ohlcv.reverse()   # newest first
            data = [[c[0],c[1],c[2],c[3],c[4],c[5]] for c in ohlcv]
            _set(key, data); _ok()
            return data
        except Exception as e:
            logger.warning(f"[DataFetcher] candles {symbol} {tf}: {e}")
            _fail(); return None

    async def get_all_data(self, symbol: str) -> Optional[Dict]:
        if circuit_open():
            logger.warning(f"[DataFetcher] Circuit OPEN — skip {symbol}")
            return None
        tasks = [
            self.get_candles(symbol, "5m",  100),
            self.get_candles(symbol, "15m", 100),
            self.get_candles(symbol, "30m", 100),
            self.get_candles(symbol, "1h",  200),
            self.get_candles(symbol, "4h",  200),
            self._order_book(symbol),
        ]
        r = await asyncio.gather(*tasks, return_exceptions=True)
        c5, c15, c30, c1h, c4h, book = [
            x if not isinstance(x, Exception) else None for x in r
        ]
        if not c5 or len(c5) < 20: return None

        obi = None
        if book and "bids" in book and "asks" in book:
            bv = sum(float(b[1]) for b in book["bids"][:10])
            av = sum(float(a[1]) for a in book["asks"][:10])
            t  = bv + av or 1
            obi = {"bid_vol": bv, "ask_vol": av, "ratio": bv / t}

        slow = await self.get_slow_data(symbol)
        return {
            "c5m": c5, "c15m": c15 or [], "c30m": c30 or [],
            "c1h": c1h or [], "c4h": c4h or [],
            "book": book, "book_imbalance": obi,
            "c1d": slow.get("c1d"), "c1w": slow.get("c1w"),
            "symbol": symbol,
        }

    async def fetch_all_assets(self) -> Dict[str, Optional[Dict]]:
        tasks = {s: self.get_all_data(s) for s in CONFIG.ASSETS}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        out = {}
        for s, res in zip(tasks.keys(), results):
            out[s] = res if not isinstance(res, Exception) else None
        good = sum(1 for v in out.values() if v)
        logger.info(f"[DataFetcher] {good}/{len(CONFIG.ASSETS)} assets fetched")
        return out

    async def get_slow_data(self, symbol: str) -> Dict:
        key = f"slow|{symbol}"
        cached = _hit(key, 43200)
        if cached: return {"c1d": cached[:30], "c1w": cached[30:42]}
        c1d, c1w = await asyncio.gather(
            self.get_candles(symbol, "1d", 30),
            self.get_candles(symbol, "1w", 12),
        )
        result = {"c1d": c1d, "c1w": c1w}
        if c1d:
            merged = (c1d or []) + (c1w or [])
            _set(key, merged)
        return result

    async def _order_book(self, symbol: str) -> Optional[Dict]:
        try: return await self._pub.fetch_order_book(symbol, 20)
        except Exception as e: logger.debug(f"[OB] {symbol}: {e}"); return None

    # ── PUBLIC: Price & Funding ────────────────────────────────────────────────

    async def get_live_price(self, symbol: str) -> float:
        if _TICKERS["data"] and (time.time() - _TICKERS["ts"]) < 90:
            p = _TICKERS["data"].get(symbol, 0)
            if p: return p
        try:
            tickers = await self._pub.fetch_tickers()
            _TICKERS["data"] = {k: float(v.get("last", 0) or 0) for k, v in tickers.items()}
            _TICKERS["ts"]   = time.time()
            return _TICKERS["data"].get(symbol, 0)
        except Exception as e:
            logger.warning(f"[Price] {e}"); return 0.0

    async def get_funding_rate(self, symbol: str) -> Dict:
        hit = _FUNDING.get(symbol)
        if hit and (time.time() - hit["ts"]) < 600: return hit["d"]
        try:
            info = await self._pub.fetch_funding_rate(symbol)
            fr   = float(info.get("fundingRate", 0) or 0)
            d    = {"rate": fr, "bull": fr < -0.0001, "bear": fr > 0.0005,
                    "extreme_bull": fr < -0.0006, "extreme_bear": fr > 0.0010}
            _FUNDING[symbol] = {"d": d, "ts": time.time()}
            return d
        except Exception as e:
            logger.warning(f"[Funding] {symbol}: {e}")
            return {"rate": 0, "bull": False, "bear": False,
                    "extreme_bull": False, "extreme_bear": False}

    async def get_oi_data(self, symbol: str) -> Optional[Dict]:
        """Open Interest — public endpoint."""
        try:
            ccy = symbol.replace("/USDT:USDT", "")
            async with httpx.AsyncClient(timeout=10) as cl:
                r = await cl.get(
                    f"https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-volume",
                    params={"ccy": ccy, "period": "1H", "limit": "8"}
                )
                if r.status_code == 200:
                    data = r.json().get("data", [])
                    if len(data) >= 4:
                        oi_vals = [float(x[1]) for x in data]
                        slope = (oi_vals[0] - oi_vals[3]) / (oi_vals[3] or 1)
                        return {"now": oi_vals[0], "slope": slope, "arr": oi_vals[:4]}
        except Exception as e:
            logger.debug(f"[OI] {symbol}: {e}")
        return None

    async def get_ls_ratio(self, symbol: str) -> Optional[float]:
        """Long/Short ratio — public endpoint."""
        try:
            ccy = symbol.replace("/USDT:USDT", "")
            async with httpx.AsyncClient(timeout=10) as cl:
                r = await cl.get(
                    "https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio",
                    params={"ccy": ccy, "period": "1H", "limit": "1"}
                )
                if r.status_code == 200:
                    data = r.json().get("data", [])
                    if data:
                        return float(data[0][1])  # LS ratio newest
        except Exception as e:
            logger.debug(f"[LS] {symbol}: {e}")
        return None

    # ── PRIVATE: Account (requires OKX keys) ──────────────────────────────────

    async def get_balance(self) -> float:
        if not self._prv:
            logger.debug("[Balance] No OKX keys — returning 0")
            return 0.0
        try:
            bal = await self._prv.fetch_balance()
            return float(bal.get("USDT", {}).get("total", 0) or 0)
        except Exception as e:
            logger.warning(f"[Balance] {e}"); return 0.0

    async def get_open_positions(self) -> List[Dict]:
        if not self._prv: return []
        try:
            pos = await self._prv.fetch_positions()
            return [p for p in pos if float(p.get("contracts", 0) or 0) != 0]
        except Exception as e:
            logger.warning(f"[Positions] {e}"); return []

    # ── External: Fear & Greed ─────────────────────────────────────────────────

    async def get_fear_greed(self) -> Dict:
        try:
            async with httpx.AsyncClient(timeout=10) as cl:
                r = await cl.get("https://api.alternative.me/fng/?limit=1")
                if r.status_code == 200:
                    d = r.json()["data"][0]
                    val = int(d["value"])
                    return {"value": val, "label": d["value_classification"],
                            "extreme_fear": val < 25, "fear": val < 45,
                            "greed": val > 55, "extreme_greed": val > 75}
        except Exception as e:
            logger.warning(f"[F&G] {e}")
        return {"value": 50, "label": "Neutral", "extreme_fear": False,
                "fear": False, "greed": False, "extreme_greed": False}

    async def get_macro_data(self) -> Dict:
        """USDT.D + BTC.D + Total market cap from CoinGecko."""
        try:
            async with httpx.AsyncClient(timeout=12) as cl:
                r = await cl.get("https://api.coingecko.com/api/v3/global",
                                 headers={"Accept": "application/json"})
                if r.status_code == 200:
                    data = r.json().get("data", {})
                    dom  = data.get("market_cap_percentage", {})
                    return {
                        "usdt_d":  round(dom.get("usdt", 7.0), 3),
                        "btc_d":   round(dom.get("btc",  50.0), 2),
                        "total_mc": data.get("total_market_cap", {}).get("usd", 0),
                        "ok": True,
                    }
        except Exception as e:
            logger.warning(f"[Macro] {e}")
        return {"usdt_d": 7.0, "btc_d": 50.0, "total_mc": 0, "ok": False}

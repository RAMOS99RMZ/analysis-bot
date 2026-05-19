# engine/smart_money.py — مؤشر السيولة المؤسسية (Smart Money Flow)
from __future__ import annotations
import asyncio, time
from typing import Dict, List, Optional
import httpx
from loguru import logger

_CACHE: Dict = {"data": None, "ts": 0}
_TTL = 300   # 5 دقائق


class SmartMoneyFlow:
    """
    يتتبع مؤشرات الأموال الكبيرة:
    - Funding Rate على OKX
    - Open Interest (تغير عبر الزمن)
    - Global Market Cap + USDT.D من CoinGecko
    يُنتج score نهائي (-1 إلى +1) يُضاف لمحرك الإشارات.
    """

    def __init__(self, fetcher=None):
        self._fetcher = fetcher

    async def get_signal(self, symbol: str, direction: str,
                         funding: Optional[Dict] = None) -> Dict:
        """
        يُرجع:
        - score: -1 إلى +1 (إيجابي = صاعد، سالب = هابط)
        - boost: 0 إلى 0.20 مضاف للإشارة
        - details: نص شرح
        """
        score   = 0.0
        details = []

        # ── 1. Funding Rate ──────────────────────────────────────────
        if funding:
            fr = float(funding.get("funding_rate", 0))
            if direction == "LONG":
                if fr < -0.0003:
                    score += 0.3; details.append(f"🏦 Funding سالب قوي ({fr:.4f}) ← دعم LONG")
                elif fr < -0.0001:
                    score += 0.15; details.append(f"🏦 Funding سالب ({fr:.4f})")
                elif fr > 0.0010:
                    score -= 0.2; details.append(f"⚠️ Funding مرتفع ({fr:.4f}) ← ضغط على LONG")
            else:
                if fr > 0.0010:
                    score += 0.3; details.append(f"🏦 Funding مرتفع جداً ({fr:.4f}) ← دعم SHORT")
                elif fr > 0.0005:
                    score += 0.15; details.append(f"🏦 Funding مرتفع ({fr:.4f})")
                elif fr < -0.0003:
                    score -= 0.2; details.append(f"⚠️ Funding سالب قوي ← ضغط على SHORT")

        # ── 2. USDT.D من CoinGecko (كاش 5 دقائق) ────────────────────
        global_data = await self._get_global()
        if global_data:
            usdt_d = global_data.get("usdt_d", 6.0)
            if direction == "LONG":
                if usdt_d < 5.0:
                    score += 0.2; details.append(f"📊 USDT.D منخفض ({usdt_d:.2f}%) ← مال يدخل كريبتو")
                elif usdt_d > 8.0:
                    score -= 0.2; details.append(f"📊 USDT.D مرتفع ({usdt_d:.2f}%) ← مال يخرج")
            else:
                if usdt_d > 8.0:
                    score += 0.2; details.append(f"📊 USDT.D مرتفع ({usdt_d:.2f}%) ← دعم SHORT")
                elif usdt_d < 5.0:
                    score -= 0.2; details.append(f"📊 USDT.D منخفض ← ضغط على SHORT")

        score = max(-1.0, min(1.0, score))

        # boost: نسبة من الـ score الإيجابي فقط
        boost = max(0.0, score) * 0.20

        return {
            "score":   round(score, 3),
            "boost":   round(boost, 3),
            "details": " | ".join(details) if details else "لا بيانات مؤسسية",
        }

    async def _get_global(self) -> Optional[Dict]:
        global _CACHE
        if _CACHE["data"] and (time.time() - _CACHE["ts"]) < _TTL:
            return _CACHE["data"]
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                r = await client.get("https://api.coingecko.com/api/v3/global")
                if r.status_code == 200:
                    d   = r.json().get("data", {})
                    dom = d.get("market_cap_percentage", {})
                    data = {
                        "usdt_d": dom.get("usdt", 6.0),
                        "btc_d":  dom.get("btc",  50.0),
                        "total_mc": d.get("total_market_cap", {}).get("usd", 0),
                    }
                    _CACHE = {"data": data, "ts": time.time()}
                    return data
        except Exception as e:
            logger.debug(f"[SmartMoney] CoinGecko: {e}")
        return _CACHE.get("data")

    def format_report(self, sym: str, result: Dict) -> str:
        score = result.get("score", 0)
        boost = result.get("boost", 0)
        emoji = "🟢" if score > 0.2 else "🔴" if score < -0.2 else "🟡"
        return (
            f"{emoji} <b>Smart Money — {sym.replace('/USDT:USDT','')}</b>\n"
            f"  Score: {score:+.2f} | Boost: +{boost:.2f}\n"
            f"  {result.get('details','')}"
        )


SMART_MONEY = SmartMoneyFlow()

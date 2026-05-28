# ai/gemini_vision.py — Pattern Recognition بـ Gemini Vision
# ══════════════════════════════════════════════════════════════════
# يرسم الشارت كـ ASCII/JSON ثم يطلب من Gemini تحليله بصرياً
# يُستدعى مرة واحدة فقط لكل عملة كل 4 ساعات (الحد المجاني: 12/ساعة)
# ══════════════════════════════════════════════════════════════════
from __future__ import annotations
import time, json
from typing import Dict, List, Optional
from loguru import logger
from config import Secrets, CONFIG

# ── تتبع الاستخدام (لا تتجاوز 12/ساعة) ──────────────────────────────────
_USAGE:   Dict[str, float] = {}   # symbol → last_call_ts
_HOUR_CALLS: List[float]  = []    # timestamps آخر ساعة
_MAX_PER_HOUR = 10                # هامش أمان تحت الحد المجاني (12)
_MIN_INTERVAL = 14400             # 4 ساعات بين كل تحليل لنفس العملة


def _can_call() -> bool:
    """هل يمكن استدعاء Gemini الآن دون تجاوز الحد؟"""
    now = time.time()
    # احذف الاستدعاءات الأقدم من ساعة
    global _HOUR_CALLS
    _HOUR_CALLS = [t for t in _HOUR_CALLS if now - t < 3600]
    return len(_HOUR_CALLS) < _MAX_PER_HOUR


def _symbol_fresh(symbol: str) -> bool:
    """هل مرّت 4 ساعات منذ آخر تحليل لهذه العملة؟"""
    last = _USAGE.get(symbol, 0)
    return (time.time() - last) > _MIN_INTERVAL


def _candles_to_summary(candles: List, n: int = 20) -> str:
    """
    يحوّل الشموع لوصف نصي مختصر لـ Gemini.
    (لا نرسل صورة — نرسل بيانات OHLCV + أنماط مكتشفة)
    """
    try:
        c = candles[:n]
        prices  = [round(float(x[4]), 2) for x in c]
        highs   = [round(float(x[2]), 2) for x in c]
        lows    = [round(float(x[3]), 2) for x in c]
        volumes = [round(float(x[5]), 0) for x in c]

        # معلومات أساسية
        current    = prices[0]
        prev_close = prices[1]  if len(prices) > 1 else current
        high_20    = max(highs)
        low_20     = min(lows)
        avg_vol    = sum(volumes) / len(volumes) if volumes else 0
        curr_vol   = volumes[0] if volumes else 0

        # أنماط بسيطة
        bullish_candles = sum(1 for i in range(min(5, len(c)))
                              if float(c[i][4]) > float(c[i][1]))
        bearish_candles = 5 - bullish_candles

        # الاتجاه من آخر 5 شمعات
        trend = "صاعد" if prices[0] > prices[4] else "هابط"

        return json.dumps({
            "current_price": current,
            "change_pct": round((current - prev_close) / prev_close * 100, 2),
            "high_20bar": high_20,
            "low_20bar": low_20,
            "position_in_range": round((current - low_20) / max(high_20 - low_20, 1) * 100, 1),
            "volume_ratio": round(curr_vol / avg_vol, 2) if avg_vol else 1.0,
            "last_5_candles_trend": trend,
            "bullish_candles_of_5": bullish_candles,
            "last_5_closes": prices[:5],
        }, ensure_ascii=False)
    except Exception:
        return "{}"


async def analyze_pattern(symbol: str, candles_4h: List,
                           current_signal: Dict) -> Optional[str]:
    """
    يطلب من Gemini تحليل النمط البصري.
    يُرجع: "CONFIRM" / "REJECT" / "NEUTRAL" + سبب قصير
    """
    if not Secrets.has_gemini():
        return None
    if not _can_call():
        logger.debug("[GeminiVision] تجاوز الحد، تخطي")
        return None
    if not _symbol_fresh(symbol):
        logger.debug(f"[GeminiVision] {symbol} محلَّل مؤخراً، تخطي")
        return None

    sym_c     = symbol.replace("/USDT:USDT", "")
    direction = current_signal.get("direction", "?")
    score     = current_signal.get("score", 0)
    summary   = _candles_to_summary(candles_4h, 20)

    prompt = f"""أنت خبير تداول كريبتو متخصص في التحليل الفني.

العملة: {sym_c}/USDT | الاتجاه المقترح: {direction} | Score: {score:.3f}

بيانات الشارت (4H) — آخر 20 شمعة:
{summary}

بناءً على هذه البيانات:
1. هل النمط الحالي يدعم إشارة {direction}؟
2. هل حجم التداول يؤكد الاتجاه؟

أجب بكلمة واحدة فقط: CONFIRM أو REJECT أو NEUTRAL
ثم سطر واحد للسبب (بالعربية، أقل من 15 كلمة)."""

    result = await _call_gemini(prompt)
    if result:
        _USAGE[symbol] = time.time()
        _HOUR_CALLS.append(time.time())
        logger.info(f"[GeminiVision] {sym_c}: {result[:60]}")
    return result


async def _call_gemini(prompt: str) -> Optional[str]:
    """استدعاء Gemini Flash 1.5 API."""
    try:
        import httpx
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-1.5-flash:generateContent?key={Secrets.GEMINI_KEY}"
        )
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": 80,
                "topP": 0.8,
            },
        }
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(url, json=body)
            if r.status_code == 429:
                logger.warning("[GeminiVision] Rate limit — تخطي")
                return None
            if r.status_code != 200:
                return None
            data = r.json()
            text = (data.get("candidates", [{}])[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", ""))
            return text.strip() if text else None
    except Exception as e:
        logger.debug(f"[GeminiVision] Error: {e}")
        return None


def parse_vision_decision(text: Optional[str]) -> str:
    """يُحوّل رد Gemini إلى CONFIRM / REJECT / NEUTRAL."""
    if not text:
        return "NEUTRAL"
    t = text.upper()
    if "CONFIRM" in t:   return "CONFIRM"
    elif "REJECT" in t:  return "REJECT"
    return "NEUTRAL"
